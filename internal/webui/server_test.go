package webui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yckao/etcd-trend/internal/explore"
)

type fakeStore struct {
	query                      explore.Query
	objectID                   string
	curveFrom, curveTo, points int
	search, resource           string
}

func (f *fakeStore) Info() explore.Info {
	return explore.Info{WindowCount: 4, WindowNS: 1_000_000_000, DurationNS: 3_749_188_000, SourceVersion: "3.6.0"}
}
func (f *fakeStore) Curve(_ context.Context, a, b, n int) ([]explore.Point, error) {
	f.curveFrom, f.curveTo, f.points = a, b, n
	return []explore.Point{{Window: a - 1}, {Window: b - 1, DBSize: 4096}}, nil
}
func (f *fakeStore) Rankings(_ context.Context, q explore.Query) (explore.RankingResult, error) {
	f.query = q
	return explore.RankingResult{From: q.From, To: q.To, Scope: q.Scope, Metric: q.Metric, Total: 10, Limit: q.Limit, Offset: q.Offset}, nil
}
func (f *fakeStore) Objects(_ context.Context, s, r string, n, o int) (explore.ObjectList, error) {
	f.search, f.resource = s, r
	return explore.ObjectList{Total: 19, Limit: n, Offset: o}, nil
}
func (f *fakeStore) Object(_ context.Context, id string, a, b, n int) (explore.ObjectDetail, error) {
	f.objectID = id
	if id == "missing" {
		return explore.ObjectDetail{}, explore.ErrNotFound
	}
	return explore.ObjectDetail{ID: id, From: a, To: b}, nil
}
func perform(h http.Handler, method, path string, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "http://127.0.0.1:8080"+path, nil)
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestReadOnlyAPIAndRangeWiring(t *testing.T) {
	f := &fakeStore{}
	h := Handler(f)
	w := perform(h, "GET", "/api/rankings?from=1&to=4&scope=object&metric=free-net&resource=widgets&namespace=default&search=hot&limit=12&offset=24", nil)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if f.query != (explore.Query{From: 1, To: 4, Scope: "object", Metric: "free-net", Search: "hot", Resource: "widgets", Namespace: "default", Limit: 12, Offset: 24}) {
		t.Fatalf("wrong range/query: %+v", f.query)
	}
	w = perform(h, "GET", "/api/curve?from=2&to=4&points=600", nil)
	if w.Code != 200 || f.curveFrom != 2 || f.curveTo != 4 || f.points != 600 {
		t.Fatal("curve query lost range")
	}
	w = perform(h, "GET", "/api/object?id=abc%2B%2F%3D&from=2&to=3", nil)
	if w.Code != 200 || f.objectID != "abc+/=" {
		t.Fatal("opaque object ID changed")
	}
	var detail explore.ObjectDetail
	if err := json.Unmarshal(w.Body.Bytes(), &detail); err != nil || detail.From != 2 || detail.To != 3 {
		t.Fatalf("bad detail: %+v %v", detail, err)
	}
	w = perform(h, "GET", "/api/objects?search=baseline&resource=configmaps&limit=8&offset=8", nil)
	if w.Code != 200 || f.search != "baseline" || f.resource != "configmaps" {
		t.Fatal("all-object search not wired")
	}
	if w := perform(h, "GET", "/api/object?id=missing", nil); w.Code != 404 {
		t.Fatal("missing object should return404")
	}
}

func TestInvalidRequestsAndLocalOrigin(t *testing.T) {
	h := Handler(&fakeStore{})
	for _, p := range []string{"/api/curve?from=-1", "/api/curve?to=5", "/api/curve?from=2&to=2", "/api/curve?points=999999", "/api/rankings?limit=0", "/api/rankings?offset=-1", "/api/object"} {
		if w := perform(h, "GET", p, nil); w.Code != 400 {
			t.Fatalf("%s: status%d", p, w.Code)
		}
	}
	if w := perform(h, "POST", "/api/info", nil); w.Code != 405 || w.Header().Get("Allow") != "GET" {
		t.Fatal("write method allowed")
	}
	for _, origin := range []string{"https://evil.example", "http://127.0.0.1:9999", "null"} {
		if w := perform(h, "GET", "/api/info", map[string]string{"Origin": origin}); w.Code != 403 {
			t.Fatalf("accepted origin%s", origin)
		}
	}
	if w := perform(h, "GET", "/api/info", map[string]string{"Origin": "http://127.0.0.1:8080"}); w.Code != 200 {
		t.Fatal("same-origin request rejected")
	}
	r := httptest.NewRequest("GET", "http://rebinding.example:8080/api/info", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("nonloopback Host accepted")
	}
	for _, a := range []string{":8080", "0.0.0.0:8080", "192.168.1.10:8080", "example.com:8080", "[::]:8080"} {
		if LoopbackAddress(a) == nil {
			t.Fatal("accepted nonlocal listener", a)
		}
	}
	for _, a := range []string{"127.0.0.1:0", "127.1.2.3:8080", "[::1]:8080", "localhost:8080"} {
		if err := LoopbackAddress(a); err != nil {
			t.Fatal(a, err)
		}
	}
}

func TestTenantGroupAndEmptyLabelFilterWiring(t *testing.T) {
	f := &fakeStore{}
	w := perform(Handler(f), "GET", "/api/rankings?scope=object&namespace=team-a&label_key=example.test%2Ftenant&label_filter=true&label_value=&group_scope=namespace-label&group_value=%5B%22present%22%2C%22alpha%22%5D", nil)
	if w.Code != 200 || f.query.Namespace != "team-a" || f.query.LabelKey != "example.test/tenant" || !f.query.LabelFilter || f.query.LabelValue != "" || f.query.GroupScope != "namespace-label" || f.query.GroupValue != `["present","alpha"]` {
		t.Fatalf("lost tenant filter: status=%d query=%+v", w.Code, f.query)
	}
	if w := perform(Handler(f), "GET", "/api/rankings?label_filter=maybe", nil); w.Code != 400 {
		t.Fatal("invalid label filter accepted")
	}
}

func TestEmbeddedAssetsAreOfflineAndConfined(t *testing.T) {
	h := Handler(&fakeStore{})
	for _, p := range []string{"/", "/app.js", "/style.css"} {
		w := perform(h, "GET", p, nil)
		if w.Code != 200 {
			t.Fatal(p, w.Code)
		}
		if !strings.Contains(w.Header().Get("Content-Security-Policy"), "connect-src 'self'") {
			t.Fatal("missing offline CSP")
		}
		if strings.Contains(w.Body.String(), "cdn.") || strings.Contains(w.Body.String(), "https://") {
			t.Fatal("external asset dependency", p)
		}
	}
	for _, p := range []string{"/../report.json", "/assets/", "/analysis/summary.json", "/api/file?path=/etc/passwd"} {
		if w := perform(h, "GET", p, nil); w.Code != 404 {
			t.Fatal("unrecognized path exposed", p, w.Code)
		}
	}
	w := perform(h, "GET", "/app.js", nil)
	if strings.Contains(w.Body.String(), "innerHTML") || strings.Contains(w.Body.String(), "eval(") {
		t.Fatal("dynamic rendering must use text-safe DOM methods")
	}
}
