package explore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/yckao/etcd-trend/internal/analysis"
	bolt "go.etcd.io/bbolt"
)

// Curve returns window endpoints, not an interpolation of native commit times.
// When sampled, extrema of DB, in-use, free, ratio and history are preserved in
// each time bin along with the first and last point.
func (s *Store) Curve(ctx context.Context, from, to, maxPoints int) (result []Point, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err = s.checkRange(from, to); err != nil {
		return nil, err
	}
	maxPoints, err = pointLimit(maxPoints)
	if err != nil {
		return nil, err
	}
	collector := newCollector[Point](to-from+1, maxPoints, func(p Point) []float64 {
		return []float64{float64(p.DBSize), float64(p.DBSizeInUse), float64(p.FreeBytes), p.FragmentationPercent, float64(p.HistoryBytes)}
	})
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
				collector.Add(physicalPoint(i-1, w.StartNS, w.PhysicalStart, w.Start))
			}
			collector.Add(physicalPoint(i, w.EndNS, w.PhysicalEnd, w.End))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return collector.Result(), nil
}

func physicalPoint(win int, t int64, p analysis.Commit, s analysis.Stock) Point {
	v := Point{Window: win, TimeNS: t, DBSize: p.DBSize, DBSizeInUse: p.DBSizeInUse, FreeBytes: p.DBSize - p.DBSizeInUse, PendingPages: p.PendingPages, HistoryBytes: s.HistoryBytes, RetainedBytes: s.RetainedBytes}
	if v.DBSize > 0 {
		v.FragmentationPercent = 100 * float64(v.FreeBytes) / float64(v.DBSize)
	}
	return v
}

func (s *Store) Object(ctx context.Context, id string, from, to, maxPoints int) (result ObjectDetail, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err = s.checkRange(from, to); err != nil {
		return result, err
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	maxPoints, err = pointLimit(maxPoints)
	if err != nil {
		return result, err
	}
	if len(id) != 64 {
		return result, ErrNotFound
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		var item catalog
		if err := getJSON(tx.Bucket(bucketObjects), []byte(id), &item); err != nil {
			return err
		}
		if item.ID == "" {
			return ErrNotFound
		}
		result = ObjectDetail{ID: id, Identity: item.Identity, From: from, To: to, Baseline: item.Baseline, Final: item.Final, TotalPoints: to - from + 1, MetadataBaseline: metadataOrUnknown(item.BaselineMetadata), MetadataFinal: metadataOrUnknown(item.Metadata)}
		current := item.Baseline
		metadata := result.MetadataBaseline
		prefix := append([]byte(id), 0)
		c := tx.Bucket(bucketHistory).Cursor()
		k, v := c.Seek(historyKey(id, from))
		if k == nil {
			k, v = c.Last()
		} else {
			k, v = c.Prev()
		}
		if k != nil && bytes.HasPrefix(k, prefix) {
			var previous analysis.Row
			if err := json.Unmarshal(v, &previous); err != nil {
				return err
			}
			current = previous.End
			metadata = metadataOrUnknown(previous.Metadata)
		}
		result.Start = current
		result.MetadataStart = metadata
		collector := newCollector[ObjectPoint](result.TotalPoints, maxPoints, func(p ObjectPoint) []float64 {
			return []float64{float64(p.Stock.LiveValueBytes), float64(p.Stock.HistoryBytes), float64(p.Stock.RetainedBytes), float64(p.Metrics.ValueWriteBytes), float64(p.Metrics.CompactedBytes), float64(p.Metrics.EstimatedFreeGrowthBytes - p.Metrics.EstimatedFreeReuseBytes)}
		})
		var first analysis.Window
		if err := getJSON(tx.Bucket(bucketWindows), numberKey(from), &first); err != nil {
			return err
		}
		collector.Add(ObjectPoint{Window: from - 1, TimeNS: first.StartNS, Stock: current, Metadata: metadata})
		k, v = c.Seek(historyKey(id, from))
		for i := from; i < to; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			var w analysis.Window
			if err := getJSON(tx.Bucket(bucketWindows), numberKey(i), &w); err != nil {
				return err
			}
			point := ObjectPoint{Window: i, TimeNS: w.EndNS, Stock: current, Metadata: metadata}
			if k != nil && bytes.HasPrefix(k, prefix) && int(binary.BigEndian.Uint64(k[len(prefix):])) == i {
				var row analysis.Row
				if err := json.Unmarshal(v, &row); err != nil {
					return err
				}
				current = row.End
				metadata = metadataOrUnknown(row.Metadata)
				point.Stock = current
				point.Metadata = metadata
				point.Metrics = row.Metrics
				point.FirstIndex, point.LastIndex = row.FirstIndex, row.LastIndex
				point.Changed = true
				addMetrics(&result.Metrics, row.Metrics)
				indexRange(&result.FirstIndex, &result.LastIndex, row.FirstIndex, row.LastIndex)
				k, v = c.Next()
			}
			collector.Add(point)
		}
		result.End = current
		result.Metadata = metadata
		result.Points = collector.Result()
		result.Sampled = len(result.Points) < result.TotalPoints
		return nil
	})
	return result, err
}

func pointLimit(n int) (int, error) {
	if n == 0 {
		n = 800
	}
	if n < 16 || n > 5000 {
		return 0, fmt.Errorf("points must be 16-5000 (or zero for default)")
	}
	return n, nil
}

type sample[T any] struct {
	index    int
	value    T
	features []float64
}
type collector[T any] struct {
	total, max, index, features, bins int
	values                            []sample[T]
	first, last                       sample[T]
	min, maxima                       [][]sample[T]
	seen                              []bool
	feature                           func(T) []float64
}

func newCollector[T any](total, max int, feature func(T) []float64) *collector[T] {
	return &collector[T]{total: total, max: max, feature: feature}
}
func (c *collector[T]) Add(value T) {
	s := sample[T]{index: c.index, value: value}
	c.index++
	if c.total <= c.max {
		c.values = append(c.values, s)
		return
	}
	if s.index == 0 {
		c.first = s
		return
	}
	if s.index == c.total-1 {
		c.last = s
		return
	}
	s.features = c.feature(value)
	if c.features == 0 {
		c.features = len(s.features)
		c.bins = (c.max - 2) / (2 * c.features)
		if c.bins < 1 {
			c.bins = 1
		}
		c.min = make([][]sample[T], c.bins)
		c.maxima = make([][]sample[T], c.bins)
		c.seen = make([]bool, c.bins)
	}
	bin := (s.index - 1) * c.bins / (c.total - 2)
	if bin >= c.bins {
		bin = c.bins - 1
	}
	if !c.seen[bin] {
		c.min[bin] = make([]sample[T], c.features)
		c.maxima[bin] = make([]sample[T], c.features)
		for j := 0; j < c.features; j++ {
			c.min[bin][j] = s
			c.maxima[bin][j] = s
		}
		c.seen[bin] = true
		return
	}
	for j, n := range s.features {
		if n < c.min[bin][j].features[j] {
			c.min[bin][j] = s
		}
		if n > c.maxima[bin][j].features[j] {
			c.maxima[bin][j] = s
		}
	}
}
func (c *collector[T]) Result() []T {
	if c.total > c.max {
		c.values = append(c.values, c.first, c.last)
		for i := range c.seen {
			if c.seen[i] {
				c.values = append(c.values, c.min[i]...)
				c.values = append(c.values, c.maxima[i]...)
			}
		}
		sort.Slice(c.values, func(i, j int) bool { return c.values[i].index < c.values[j].index })
	}
	result := make([]T, 0, len(c.values))
	last := -1
	for _, p := range c.values {
		if p.index != last {
			result = append(result, p.value)
			last = p.index
		}
	}
	return result
}
