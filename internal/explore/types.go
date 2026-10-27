// Package explore indexes and queries completed offline replay analysis.
package explore

import (
	"errors"

	"github.com/yckao/etcd-trend/internal/analysis"
)

var ErrNotFound = errors.New("object not found")

type Info struct {
	SourceVersion   string         `json:"source_version"`
	Engine          string         `json:"engine"`
	Model           string         `json:"model"`
	AnalysisVersion string         `json:"analysis_version"`
	WindowCount     int            `json:"window_count"`
	WindowNS        int64          `json:"window_ns"`
	DurationNS      int64          `json:"duration_ns"`
	ObjectCount     int64          `json:"object_count"`
	ResourceCount   int64          `json:"resource_count"`
	FinalRevision   int64          `json:"final_revision"`
	LogicalSHA256   string         `json:"logical_sha256"`
	Parameters      map[string]any `json:"parameters"`
	LabelKeys       []string       `json:"label_keys"`
}

// Point is a window boundary observation. Window=-1 is the initial boundary;
// other values identify the window whose endpoint is represented.
type Point struct {
	Window               int     `json:"window"`
	TimeNS               int64   `json:"time_ns"`
	DBSize               int64   `json:"db_size"`
	DBSizeInUse          int64   `json:"db_size_in_use"`
	FreeBytes            int64   `json:"free_bytes"`
	FragmentationPercent float64 `json:"fragmentation_percent"`
	PendingPages         int     `json:"pending_pages"`
	HistoryBytes         int64   `json:"history_bytes"`
	RetainedBytes        int64   `json:"retained_bytes"`
}

type Query struct {
	From, To                    int // Inclusive/exclusive window indexes.
	Scope, Metric               string
	Search, Resource, Namespace string
	LabelKey, LabelValue        string
	LabelFilter                 bool   // Enables exact matching even for an empty label value.
	GroupScope, GroupValue      string // Exact group filter for drilling into contributors.
	Limit, Offset               int
}

// Group distinguishes missing metadata from a real label with the same display
// text. Key is an opaque filter token; Value is a presentation label.
type Group struct {
	Scope    string `json:"scope"`
	Key      string `json:"key"`
	Value    string `json:"value"`
	LabelKey string `json:"label_key,omitempty"`
	Status   string `json:"status"`
}

type RangeTotals struct {
	From                        int              `json:"from"`
	To                          int              `json:"to"`
	StartNS                     int64            `json:"start_ns"`
	EndNS                       int64            `json:"end_ns"`
	Start                       analysis.Stock   `json:"start"`
	End                         analysis.Stock   `json:"end"`
	PhysicalStart               analysis.Commit  `json:"physical_start"`
	PhysicalEnd                 analysis.Commit  `json:"physical_end"`
	Metrics                     analysis.Metrics `json:"metrics"`
	DBGrowthBytes               int64            `json:"db_growth_bytes"`
	FreeGrowthBytes             int64            `json:"free_growth_bytes"`
	FreeReuseBytes              int64            `json:"free_reuse_bytes"`
	UnattributedDBGrowthBytes   int64            `json:"unattributed_db_growth_bytes"`
	UnattributedFreeGrowthBytes int64            `json:"unattributed_free_growth_bytes"`
	UnattributedFreeReuseBytes  int64            `json:"unattributed_free_reuse_bytes"`
}

type Ranking struct {
	ID                 string            `json:"id"`
	Identity           analysis.Identity `json:"identity"`
	Bytes              int64             `json:"bytes"`
	SharePercent       float64           `json:"share_percent"`
	Start              analysis.Stock    `json:"start"`
	End                analysis.Stock    `json:"end"`
	Metrics            analysis.Metrics  `json:"metrics"`
	FirstIndex         uint64            `json:"first_index"`
	LastIndex          uint64            `json:"last_index"`
	Group              *Group            `json:"group,omitempty"`
	Metadata           analysis.Metadata `json:"metadata"`
	RetainedDeltaBytes int64             `json:"retained_delta_bytes"`
	HistoryDeltaBytes  int64             `json:"history_delta_bytes"`
}

type RankingResult struct {
	From             int         `json:"from"`
	To               int         `json:"to"`
	Scope            string      `json:"scope"`
	Metric           string      `json:"metric"`
	Estimated        bool        `json:"estimated"`
	Items            []Ranking   `json:"items"`
	Total            int         `json:"total"` // Positive contributors matching all filters, before pagination.
	DenominatorBytes int64       `json:"denominator_bytes"`
	DenominatorLabel string      `json:"denominator_label"`
	Limit            int         `json:"limit"`
	Offset           int         `json:"offset"`
	Totals           RangeTotals `json:"totals"` // Global selected-range totals, independent of filters.
	Attribution      string      `json:"attribution"`
}

type ObjectItem struct {
	ID       string            `json:"id"`
	Identity analysis.Identity `json:"identity"`
	Baseline analysis.Stock    `json:"baseline"`
	Final    analysis.Stock    `json:"final"`
	Metadata analysis.Metadata `json:"metadata"`
}

type ObjectList struct {
	Items  []ObjectItem `json:"items"`
	Total  int          `json:"total"`
	Limit  int          `json:"limit"`
	Offset int          `json:"offset"`
}

type ObjectPoint struct {
	Window     int               `json:"window"`
	TimeNS     int64             `json:"time_ns"`
	Stock      analysis.Stock    `json:"stock"`
	Metrics    analysis.Metrics  `json:"metrics"`
	FirstIndex uint64            `json:"first_index"`
	LastIndex  uint64            `json:"last_index"`
	Changed    bool              `json:"changed"`
	Metadata   analysis.Metadata `json:"metadata"`
}

type ObjectDetail struct {
	ID               string            `json:"id"`
	Identity         analysis.Identity `json:"identity"`
	From             int               `json:"from"`
	To               int               `json:"to"`
	Baseline         analysis.Stock    `json:"baseline"`
	Final            analysis.Stock    `json:"final"`
	Start            analysis.Stock    `json:"start"`
	End              analysis.Stock    `json:"end"`
	Metrics          analysis.Metrics  `json:"metrics"` // Sum across every selected window, not sampled points.
	FirstIndex       uint64            `json:"first_index"`
	LastIndex        uint64            `json:"last_index"`
	Points           []ObjectPoint     `json:"points"`
	TotalPoints      int               `json:"total_points"`
	Sampled          bool              `json:"sampled"`
	Metadata         analysis.Metadata `json:"metadata"`
	MetadataStart    analysis.Metadata `json:"metadata_start"`
	MetadataBaseline analysis.Metadata `json:"metadata_baseline"`
	MetadataFinal    analysis.Metadata `json:"metadata_final"`
}
