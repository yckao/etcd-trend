package tui

import (
	bytesbuf "bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yckao/etcd-trend/internal/analysis"
	"github.com/yckao/etcd-trend/internal/explore"
)

func newTestState(t *testing.T, windows int) *State {
	t.Helper()
	s, err := NewState(explore.Info{SourceVersion: "3.6.0", WindowCount: windows, WindowNS: int64(time.Second), DurationNS: int64(windows) * int64(time.Second)}, Config{To: -1, Width: 100, Height: 28})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestRangeSelectionNeverEscapesOrBecomesEmpty(t *testing.T) {
	for _, count := range []int{1, 4, 100} {
		s := newTestState(t, count)
		for _, key := range []string{"[", "{", "]", "}", "h", "l", "a"} {
			for i := 0; i < count*2+5; i++ {
				s.Key(key)
				if s.From < 0 || s.To > count || s.From >= s.To {
					t.Fatalf("count=%d key=%q invalidrange [%d,%d)", count, key, s.From, s.To)
				}
			}
		}
	}
	s := newTestState(t, 8)
	s.From, s.To = 2, 5
	s.Key("right")
	if s.From != 3 || s.To != 6 {
		t.Fatalf("shift changedrange width: %+v", s)
	}
	s.Key("]")
	s.Key("{")
	if s.From != 4 || s.To != 5 {
		t.Fatalf("rangehandles incorrect: %+v", s)
	}
}

func TestDrilldownSearchAndReturn(t *testing.T) {
	s := newTestState(t, 4)
	s.Data.Ranking.Items = []explore.Ranking{{ID: "resource-id", Identity: analysis.Identity{Resource: "widgets"}}}
	if query, quit := s.Key("enter"); !query || quit || s.Resource != "widgets" || s.Scope != "object" {
		t.Fatal("resource drilldown failed")
	}
	s.Data.Ranking.Items = []explore.Ranking{{ID: "hot-id", Identity: analysis.Identity{Key: "/registry/widgets/ns/hot"}}}
	s.Key("enter")
	if s.Mode != "detail" || s.ObjectID != "hot-id" {
		t.Fatal("object drilldown failed")
	}
	s.Key("esc")
	if s.Mode != "ranking" || s.Resource != "widgets" {
		t.Fatal("detail return lost filter")
	}
	s.Key("/")
	for _, k := range []string{"q", "u", "i", "e", "t"} {
		if _, quit := s.Key(k); quit {
			t.Fatal("typing q quitsearch")
		}
	}
	s.Key("enter")
	if s.Mode != "objects" || s.Search != "quiet" {
		t.Fatal("object catalog search failed")
	}
	s.Data.Objects.Items = []explore.ObjectItem{{ID: "quiet-id"}}
	s.Key("enter")
	s.Key("esc")
	if s.Mode != "objects" {
		t.Fatal("object detail didnot returnto catalog")
	}
}

func TestDecoderSplitSequencesAndUTF8(t *testing.T) {
	var d Decoder
	if out := d.Feed([]byte{27, '['}, false); len(out) != 0 {
		t.Fatal(out)
	}
	if out := d.Feed([]byte("A\x1b[6~\x03"), false); !reflect.DeepEqual(out, []string{"up", "pgdown", "ctrlc"}) {
		t.Fatal(out)
	}
	b := []byte("測")
	if out := d.Feed(b[:1], false); len(out) != 0 {
		t.Fatal(out)
	}
	if out := d.Feed(b[1:], false); !reflect.DeepEqual(out, []string{"測"}) {
		t.Fatal(out)
	}
	d.Feed([]byte{27}, false)
	if out := d.Feed(nil, true); !reflect.DeepEqual(out, []string{"esc"}) {
		t.Fatal(out)
	}
}

func TestRenderIsBoundedAndTerminalInert(t *testing.T) {
	s := newTestState(t, 1)
	s.Info.DurationNS = 0
	s.Data.Curve = []explore.Point{{DBSize: 4096, DBSizeInUse: 2048, FreeBytes: 2048, FragmentationPercent: 50}}
	s.Scope = "object"
	s.Data.Ranking = explore.RankingResult{Items: []explore.Ranking{{Identity: analysis.Identity{Key: "/bad\x1b]52;c;secret\a\nforged\trow"}, Bytes: 10, SharePercent: 25}}, Total: 4, DenominatorBytes: 40}
	out := s.Render()
	if strings.ContainsAny(out, "\x1b\a\t") || strings.Contains(out, "\nforged") {
		t.Fatalf("terminalcontrol leaked: %q", out)
	}
	if !strings.Contains(out, `\x1b`) || !strings.Contains(out, "25.00%") {
		t.Fatal("escaped identity/share missing")
	}
	if len(strings.Split(strings.TrimSuffix(out, "\n"), "\n")) > s.Height {
		t.Fatal("frame exceeds height")
	}
	for _, line := range strings.Split(out, "\n") {
		if len(line) > s.Width {
			t.Fatal("frame exceedswidth")
		}
	}
	s.Resize(50, 10)
	if !strings.Contains(s.Render(), "Terminal is too small") {
		t.Fatal("small terminal guidance missing")
	}
	s.Resize(100, 28)
	s.Loading = true
	if strings.Contains(s.Render(), "25.00%") {
		t.Fatal("stale ranking displayedwhileloading")
	}
}

func TestQuietZeroDurationObjectCanBeFoundAndInspected(t *testing.T) {
	root := t.TempDir()
	a, err := analysis.New(analysis.Config{OutputDir: filepath.Join(root, "analysis"), Window: time.Second, TopN: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Event(analysis.Event{Kind: "baseline", Key: []byte("/registry/configmaps/default/quiet"), Revision: 1, ValueBytes: 7, RecordBytes: 50}); err != nil {
		t.Fatal(err)
	}
	if err = a.Start(analysis.Commit{DBSize: 4096, DBSizeInUse: 2048}); err != nil {
		t.Fatal(err)
	}
	summary, err := a.Finish(0)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(map[string]any{"analysis_directory": "analysis", "analysis": summary, "source_version": "3.6.0"})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "report.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	store, err := explore.Open(context.Background(), root, "")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var out bytesbuf.Buffer
	if err = Snapshot(context.Background(), store, Config{To: -1, Search: "quiet", Width: 120, Height: 28}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "/registry/configmaps/default/quiet") || strings.ContainsRune(out.String(), 27) {
		t.Fatal("plain catalog snapshot missingquietobject")
	}
	s, err := NewState(store.Info(), Config{To: -1, Search: "quiet", Width: 120, Height: 32})
	if err != nil {
		t.Fatal(err)
	}
	s.Data = s.Query(context.Background(), store)
	if s.Data.Err != nil {
		t.Fatal(s.Data.Err)
	}
	s.Key("enter")
	s.Data = s.Query(context.Background(), store)
	if s.Data.Err != nil {
		t.Fatal(s.Data.Err)
	}
	if s.Data.Detail == nil || s.Data.Detail.Start.LiveValueBytes != 7 || s.Data.Detail.End.LiveValueBytes != 7 || s.Data.Detail.Metrics.Puts != 0 {
		t.Fatalf("quiet objectstock was notcarried: %+v", s.Data.Detail)
	}
	if !strings.Contains(s.Render(), "Measured: puts=0") {
		t.Fatal("objectdetails missing")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out.Reset()
	if err = Snapshot(ctx, store, Config{To: -1}, &out); !errors.Is(err, context.Canceled) || out.Len() != 0 {
		t.Fatalf("cancellation produced partialsnapshot: %v", err)
	}
}

func TestSparkKeepsPeakAndHandlesZeroDuration(t *testing.T) {
	if got := spark([]int64{0, 1, 2}, []float64{0, 100, 0}, 3, 100); got != " @ " {
		t.Fatalf("peakmissing:%q", got)
	}
	if got := spark([]int64{0}, []float64{50}, 4, 100); got != "++++" {
		t.Fatalf("zero duration failed:%q", got)
	}
}

func TestTenantGroupDrillAndFilterEditing(t *testing.T) {
	s := newTestState(t, 4)
	s.Info.LabelKeys = []string{"team", "tenant"}
	s.LabelKey = "tenant"
	s.Namespace = "ns-a"
	s.Scope = "namespace-label"
	s.Data.Ranking.Items = []explore.Ranking{{Group: &explore.Group{Scope: "namespace-label", Key: `["namespace-label","present","tenant","alpha"]`, Value: "alpha"}}}
	s.Key("enter")
	if s.Scope != "object" || s.GroupScope != "namespace-label" || s.GroupValue == "" || s.Namespace != "ns-a" || s.LabelKey != "tenant" {
		t.Fatalf("group drill lost filters: %+v", s)
	}
	f := &tenantStore{}
	s.Query(context.Background(), f)
	if f.query.GroupValue != s.GroupValue || f.query.GroupScope != "namespace-label" || f.query.Namespace != "ns-a" || f.query.LabelKey != "tenant" {
		t.Fatalf("query lost filters: %+v", f.query)
	}
	s.Key("esc")
	if s.Scope != "namespace-label" || s.GroupScope != "" {
		t.Fatal("group return failed")
	}
	s.Key("v")
	s.Key("enter")
	if !s.LabelFilter || s.LabelValue != "" {
		t.Fatal("explicit empty label value lost")
	}
	s.Query(context.Background(), f)
	if !f.query.LabelFilter {
		t.Fatal("empty value filter not forwarded")
	}
	s.Key("n")
	s.Edit = "ns-b"
	s.Key("enter")
	if s.Namespace != "ns-b" || !s.LabelFilter {
		t.Fatal("namespace editing lost label filter")
	}
	s.Key("L")
	if s.LabelKey != "team" || s.LabelFilter {
		t.Fatal("label key cycling retained old value filter")
	}
	s.Key("x")
	if s.Namespace != "" || s.GroupValue != "" || s.LabelFilter {
		t.Fatal("clear filters failed")
	}
}

func TestMetadataDetailScrollAndTerminalSafety(t *testing.T) {
	s := newTestState(t, 4)
	s.Mode = "detail"
	s.Data.Detail = &explore.ObjectDetail{Metadata: analysis.Metadata{Status: "decoded-json", Kind: "Pod", APIVersion: "v1", Namespace: "ns", UID: "new", Labels: map[string]string{"tenant": "alpha\x1b[31m", "team": "one"}, Controller: &analysis.OwnerReference{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web", UID: "rs-1"}, Managers: []string{"controller", "kubectl"}}, MetadataStart: analysis.Metadata{UID: "old", Labels: map[string]string{"tenant": "beta"}}}
	s.Key("i")
	out := strings.Join(s.metadataLines(), "\n")
	for _, want := range []string{"Pod", "new", "old", "ReplicaSet/web", "rs-1", "controller, kubectl", "tenant", "beta", "not authenticated writers"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q: %s", want, out)
		}
	}
	if strings.ContainsRune(out, 27) {
		t.Fatal("metadata emitted terminal control")
	}
	s.Key("j")
	if s.DetailOffset != 1 {
		t.Fatal("metadata does not scroll")
	}
	s.Key("i")
	if s.MetadataView || s.DetailOffset != 0 {
		t.Fatal("metadata toggle did not return")
	}
}

type tenantStore struct{ query explore.Query }

func (f *tenantStore) Info() explore.Info                                            { return explore.Info{} }
func (f *tenantStore) Curve(context.Context, int, int, int) ([]explore.Point, error) { return nil, nil }
func (f *tenantStore) Rankings(_ context.Context, q explore.Query) (explore.RankingResult, error) {
	f.query = q
	return explore.RankingResult{}, nil
}
func (f *tenantStore) Objects(context.Context, string, string, int, int) (explore.ObjectList, error) {
	return explore.ObjectList{}, nil
}
func (f *tenantStore) Object(context.Context, string, int, int, int) (explore.ObjectDetail, error) {
	return explore.ObjectDetail{}, nil
}

func TestManagerGroupDrillClearsDisplaySearchOnly(t *testing.T) {
	s := newTestState(t, 4)
	s.Scope = "manager"
	s.Search = "controller"
	s.Namespace = "ns-a"
	s.LabelKey = "tenant"
	s.LabelValue = "alpha"
	s.LabelFilter = true
	token := `["manager","present","controller"]`
	s.Data.Ranking.Items = []explore.Ranking{{Group: &explore.Group{Scope: "manager", Key: token, Value: "controller"}}}
	s.Key("enter")
	f := &tenantStore{}
	s.Query(context.Background(), f)
	if f.query.Scope != "object" || f.query.Search != "" || f.query.GroupScope != "manager" || f.query.GroupValue != token || f.query.Namespace != "ns-a" || f.query.LabelKey != "tenant" || f.query.LabelValue != "alpha" || !f.query.LabelFilter {
		t.Fatalf("group drill changed structural filters or retained display search: %+v", f.query)
	}
}

func TestCompactKeyboardWorkflow(t *testing.T) {
	s := newTestState(t, 120)
	s.Info.LabelKeys = []string{"team", "tenant"}
	s.LabelKey = "team"
	s.Data.Ranking.Total = 50
	s.Data.Ranking.Items = make([]explore.Ranking, s.PageSize())
	s.Key("d")
	if s.Offset != s.PageSize() {
		t.Fatal("d did not advance page")
	}
	s.Key("u")
	if s.Offset != 0 {
		t.Fatal("u did not return page")
	}
	s.Key("b")
	if s.Offset != 40 {
		t.Fatalf("b not last page: %d", s.Offset)
	}
	s.Key("t")
	if s.Offset != 0 {
		t.Fatal("t not first page")
	}
	s.Key("c")
	s.Key("j")
	s.Key("enter")
	if s.Menu != "group" {
		t.Fatal("command menu did not open grouping choices")
	}
	s.Key("j")
	s.Key("j")
	s.Key("enter")
	if s.Scope != "namespace" || s.Menu != "" {
		t.Fatal("direct namespace choice failed")
	}
	s.Key("r")
	s.Key("ctrlu")
	for _, r := range "10s 30s" {
		s.Key(string(r))
	}
	query, _ := s.Key("enter")
	if !query || s.From != 10 || s.To != 30 || s.Searching {
		t.Fatalf("range entry failed: %+v", s)
	}
	s.Key("h")
	if s.From != 9 || s.To != 29 {
		t.Fatal("range shift failed")
	}
	s.Data.Ranking.Items = []explore.Ranking{{Group: &explore.Group{Scope: "namespace", Key: "opaque", Value: "ns"}}}
	s.Key("enter")
	if s.Scope != "object" {
		t.Fatal("group open failed")
	}
	s.Key("e")
	if s.Scope != "namespace" || s.GroupScope != "" {
		t.Fatal("e did not return to grouping")
	}
	s.Key("c")
	s.Key("b")
	s.Key("t")
	s.Key("e")
	if s.Menu != "" {
		t.Fatal("menu navigation/back failed")
	}
}

func TestRangeEditorValidationCancellationAndWordEditing(t *testing.T) {
	s := newTestState(t, 6)
	s.Info.WindowNS = int64(5 * time.Second)
	s.Info.DurationNS = int64(28 * time.Second)
	for _, tc := range []struct {
		input    string
		from, to int
		bad      bool
	}{{"1 4", 1, 4, false}, {"6s 21s", 1, 5, false}, {"25s 28s", 5, 6, false}, {"3 3", 0, 0, true}, {"-1 3", 0, 0, true}, {"0 7", 0, 0, true}, {"0s 29s", 0, 0, true}, {"junk", 0, 0, true}} {
		a, b, err := s.parseRange(tc.input)
		if (err != nil) != tc.bad || !tc.bad && (a != tc.from || b != tc.to) {
			t.Fatalf("%q: [%d,%d) %v", tc.input, a, b, err)
		}
	}
	s.From, s.To = 1, 4
	s.Mode = "detail"
	s.ObjectID = "keep-id"
	s.Key("r")
	s.Key("ctrlu")
	s.Key("q")
	if query, quit := s.Key("enter"); query || quit || !s.Searching || s.EditError == "" || s.From != 1 || s.To != 4 {
		t.Fatal("invalid range changed selection or closed editor")
	}
	s.Key("ctrlg")
	if s.Searching || s.Mode != "detail" || s.ObjectID != "keep-id" {
		t.Fatal("cancel discarded detail")
	}
	s.Key("r")
	s.Key("ctrlu")
	for _, r := range "2 6" {
		s.Key(string(r))
	}
	s.Key("enter")
	if s.From != 2 || s.To != 6 || s.ObjectID != "keep-id" {
		t.Fatal("range update lost current detail")
	}
	s.Key("n")
	for _, r := range "alpha beta" {
		s.Key(string(r))
	}
	s.Key("ctrlw")
	if s.Edit != "alpha " {
		t.Fatalf("word deletion failed: %q", s.Edit)
	}
	s.Key("ctrlu")
	if s.Edit != "" {
		t.Fatal("clear field failed")
	}
}

func TestCompactKeyboardHelpAndDecoder(t *testing.T) {
	var decoder Decoder
	if got := decoder.Feed([]byte{7, 21, 23}, false); !reflect.DeepEqual(got, []string{"ctrlg", "ctrlu", "ctrlw"}) {
		t.Fatal(got)
	}
	s := newTestState(t, 4)
	s.Resize(72, 20)
	s.Key("?")
	s.Key("b")
	if !strings.Contains(s.Render(), "Physical shares") {
		t.Fatal("short terminal cannot reach help end")
	}
	s.Key("t")
	if !strings.Contains(s.Render(), "60% KEYBOARD") {
		t.Fatal("help first page missing")
	}
	s.Key("c")
	if !strings.Contains(s.Render(), "COMMANDS") {
		t.Fatal("help cannot open command menu")
	}
	s.Key("e")
	s.Key("r")
	s.Key("ctrlu")
	s.Key("9")
	s.Key("enter")
	if !strings.Contains(s.Render(), "Error:") {
		t.Fatal("range validation hidden on short terminal")
	}
}
