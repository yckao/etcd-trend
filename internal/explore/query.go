package explore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/yckao/etcd-trend/internal/analysis"
	bolt "go.etcd.io/bbolt"
)

var aggregateBucket = []byte("aggregate")
var sortedBucket = []byte("sorted")

func (s *Store) Rankings(ctx context.Context, q Query) (result RankingResult, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err = s.checkRange(q.From, q.To); err != nil {
		return result, err
	}
	if err = s.validateGrouping(q); err != nil {
		return result, err
	}
	if !validMetric(q.Metric) {
		return result, fmt.Errorf("unknown ranking metric %q", q.Metric)
	}
	if err = validatePage(q.Limit, q.Offset); err != nil {
		return result, err
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	result = RankingResult{From: q.From, To: q.To, Scope: q.Scope, Metric: q.Metric, Estimated: strings.HasPrefix(q.Metric, "db-") || strings.HasPrefix(q.Metric, "free-"), Limit: q.Limit, Offset: q.Offset, Items: []Ranking{}, Attribution: attribution}
	result.Totals, err = s.totals(ctx, q.From, q.To)
	if err != nil {
		return result, err
	}
	tmp, err := os.MkdirTemp(s.cache, "query-")
	if err != nil {
		return result, err
	}
	defer os.RemoveAll(tmp)
	agg, err := newWriter(filepath.Join(tmp, "aggregate.db"), aggregateBucket)
	if err != nil {
		return result, err
	}
	defer agg.Close()
	err = s.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(bucketObjectRows).Cursor()
		end := numberKey(q.To)
		for key, value := cursor.Seek(numberKey(q.From)); key != nil && bytes.Compare(key[:8], end) < 0; key, value = cursor.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			var row analysis.Row
			if err := json.Unmarshal(value, &row); err != nil {
				return err
			}
			win := int(binary.BigEndian.Uint64(key[:8]))
			ns, _ := namespace(row.Identity, row.Metadata)
			if q.Namespace != "" && ns != q.Namespace {
				continue
			}
			if q.Resource != "" && row.Identity.Resource != q.Resource {
				continue
			}
			if q.LabelFilter || q.LabelValue != "" {
				value, exists := row.Metadata.Labels[q.LabelKey]
				if !decoded(row.Metadata) || !exists || value != q.LabelValue {
					continue
				}
			}
			if q.GroupScope != "" {
				g, err := rowGroup(tx, row, win, q.GroupScope, q.LabelKey)
				if err != nil {
					return err
				}
				if g.Key != q.GroupValue {
					continue
				}
			}
			id := row.Identity
			var grouping *Group
			rid := identityID(id, true)
			if q.Scope != "object" {
				g, err := rowGroup(tx, row, win, q.Scope, q.LabelKey)
				if err != nil {
					return err
				}
				grouping = &g
				h := sha256.Sum256([]byte(g.Key))
				rid = hex.EncodeToString(h[:])
				if q.Scope == "resource" {
					id = analysis.Identity{Resource: id.Resource, APIGroup: id.APIGroup, Source: "storage-prefix", Confidence: "storage-prefix"}
				} else {
					id = analysis.Identity{}
				}
			}
			if q.Search != "" {
				if grouping != nil {
					if !strings.Contains(strings.ToLower(grouping.Value), strings.ToLower(q.Search)) {
						continue
					}
				} else if !matchesMetadata(id, row.Metadata, q.Search, "") {
					continue
				}
			}
			var r Ranking
			exists := agg.tx.Bucket(aggregateBucket).Get([]byte(rid)) != nil
			if err := getJSON(agg.tx.Bucket(aggregateBucket), []byte(rid), &r); err != nil {
				return err
			}
			if !exists {
				r.ID, r.Identity, r.Group = rid, id, grouping
			}
			// Endpoint stocks are meaningful only for object rows. Group growth
			// is the sum of each classified row's delta, never a sum of stocks.
			if q.Scope == "object" {
				if !exists {
					r.Start = row.Start
				}
				r.End = row.End
				r.Metadata = row.Metadata
			}
			r.RetainedDeltaBytes += row.End.RetainedBytes - row.Start.RetainedBytes
			r.HistoryDeltaBytes += row.End.HistoryBytes - row.Start.HistoryBytes
			addMetrics(&r.Metrics, row.Metrics)
			indexRange(&r.FirstIndex, &r.LastIndex, row.FirstIndex, row.LastIndex)
			if err := agg.putJSON(aggregateBucket, []byte(rid), r, false); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	if err = agg.checkpoint(); err != nil {
		return result, err
	}
	// Use a separate sorted disk index instead of retaining one record per
	// contributor (or an offset-sized heap) in the UI process.
	sorted, err := newWriter(filepath.Join(tmp, "sorted.db"), sortedBucket)
	if err != nil {
		return result, err
	}
	defer sorted.Close()
	err = agg.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(aggregateBucket).ForEach(func(key, value []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			var r Ranking
			if err := json.Unmarshal(value, &r); err != nil {
				return err
			}
			r.Bytes = metricValue(q.Metric, r)
			if r.Bytes <= 0 {
				return nil
			}
			result.Total++
			result.DenominatorBytes += r.Bytes
			k := make([]byte, 8+len(r.ID))
			binary.BigEndian.PutUint64(k, math.MaxUint64-uint64(r.Bytes))
			copy(k[8:], r.ID)
			return sorted.putJSON(sortedBucket, k, r, true)
		})
	})
	if err != nil {
		return result, err
	}
	result.DenominatorLabel = "all positive interval contributors matching filters, before pagination"
	if q.Search == "" && q.Resource == "" && q.Namespace == "" && q.GroupScope == "" && !q.LabelFilter && q.LabelValue == "" {
		switch q.Metric {
		case "db-growth":
			result.DenominatorBytes = result.Totals.DBGrowthBytes
			result.DenominatorLabel = "all observed interval DB growth, including unattributed"
		case "free-growth":
			result.DenominatorBytes = result.Totals.FreeGrowthBytes
			result.DenominatorLabel = "all observed interval free growth, including unattributed"
		}
	}
	if err = sorted.checkpoint(); err != nil {
		return result, err
	}
	err = sorted.db.View(func(tx *bolt.Tx) error {
		n := 0
		c := tx.Bucket(sortedBucket).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if n < q.Offset {
				n++
				continue
			}
			if len(result.Items) >= q.Limit {
				break
			}
			var r Ranking
			if err := json.Unmarshal(v, &r); err != nil {
				return err
			}
			if result.DenominatorBytes > 0 {
				r.SharePercent = 100 * float64(r.Bytes) / float64(result.DenominatorBytes)
			}
			result.Items = append(result.Items, r)
		}
		return nil
	})
	return result, err
}

func (s *Store) Objects(ctx context.Context, search, resource string, limit, offset int) (result ObjectList, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return result, fmt.Errorf("explorer is closed")
	}
	if err = validatePage(limit, offset); err != nil {
		return result, err
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	result = ObjectList{Items: []ObjectItem{}, Limit: limit, Offset: offset}
	err = s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketObjects).ForEach(func(k, v []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			var item catalog
			if err := json.Unmarshal(v, &item); err != nil {
				return err
			}
			if !matchesMetadata(item.Identity, item.Metadata, search, resource) {
				return nil
			}
			if result.Total >= offset && len(result.Items) < limit {
				result.Items = append(result.Items, item.ObjectItem)
			}
			result.Total++
			return nil
		})
	})
	return result, err
}

func (s *Store) totals(ctx context.Context, from, to int) (t RangeTotals, err error) {
	t.From, t.To = from, to
	err = s.db.View(func(tx *bolt.Tx) error {
		for i := from; i < to; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			var w analysis.Window
			if err := getJSON(tx.Bucket(bucketWindows), numberKey(i), &w); err != nil {
				return err
			}
			if i == from {
				t.StartNS, t.Start, t.PhysicalStart = w.StartNS, w.Start, w.PhysicalStart
			}
			t.EndNS, t.End, t.PhysicalEnd = w.EndNS, w.End, w.PhysicalEnd
			addMetrics(&t.Metrics, w.Metrics)
			t.DBGrowthBytes += w.DBGrowthBytes
			t.FreeGrowthBytes += w.FreeGrowthBytes
			t.FreeReuseBytes += w.FreeReuseBytes
			t.UnattributedDBGrowthBytes += w.UnattributedDBGrowthBytes
			t.UnattributedFreeGrowthBytes += w.UnattributedFreeGrowthBytes
			t.UnattributedFreeReuseBytes += w.UnattributedFreeReuseBytes
		}
		return nil
	})
	return t, err
}

func validMetric(m string) bool {
	switch m {
	case "writes", "history", "retained", "compacted", "db-growth", "free-growth", "free-net":
		return true
	}
	return false
}
func metricValue(metric string, r Ranking) int64 {
	switch metric {
	case "writes":
		return r.Metrics.ValueWriteBytes
	case "history":
		return r.HistoryDeltaBytes
	case "retained":
		return r.RetainedDeltaBytes
	case "compacted":
		return r.Metrics.CompactedBytes
	case "db-growth":
		return r.Metrics.EstimatedDBGrowthBytes
	case "free-growth":
		return r.Metrics.EstimatedFreeGrowthBytes
	default:
		return r.Metrics.EstimatedFreeGrowthBytes - r.Metrics.EstimatedFreeReuseBytes
	}
}
func validatePage(limit, offset int) error {
	if limit < 1 || limit > 100 || offset < 0 {
		return fmt.Errorf("limit must be 1-100 and offset nonnegative")
	}
	return nil
}
func matches(id analysis.Identity, search, resource string) bool {
	if resource != "" && id.Resource != resource {
		return false
	}
	if search == "" {
		return true
	}
	s := strings.ToLower(search)
	return strings.Contains(strings.ToLower(id.Key), s) || strings.Contains(strings.ToLower(id.Resource), s) || strings.Contains(strings.ToLower(id.Namespace), s) || strings.Contains(strings.ToLower(id.Name), s)
}
func indexRange(first, last *uint64, a, b uint64) {
	if a != 0 && (*first == 0 || a < *first) {
		*first = a
	}
	if b > *last {
		*last = b
	}
}
func addMetrics(a *analysis.Metrics, b analysis.Metrics) {
	a.Puts += b.Puts
	a.Deletes += b.Deletes
	a.ValueWriteBytes += b.ValueWriteBytes
	a.RecordWriteBytes += b.RecordWriteBytes
	a.SupersededLiveBytes += b.SupersededLiveBytes
	a.CompactedRecords += b.CompactedRecords
	a.CompactedBytes += b.CompactedBytes
	a.EstimatedDBGrowthBytes += b.EstimatedDBGrowthBytes
	a.EstimatedFreeGrowthBytes += b.EstimatedFreeGrowthBytes
	a.EstimatedDeferredFreeGrowthBytes += b.EstimatedDeferredFreeGrowthBytes
	a.EstimatedFreeReuseBytes += b.EstimatedFreeReuseBytes
}
