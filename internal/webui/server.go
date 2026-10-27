// Package webui serves an embedded, read-only offline replay explorer.
package webui

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/yckao/etcd-trend/internal/explore"
)

//go:embed assets/index.html assets/style.css assets/app.js
var assets embed.FS

type Store interface {
	Info() explore.Info
	Curve(context.Context, int, int, int) ([]explore.Point, error)
	Rankings(context.Context, explore.Query) (explore.RankingResult, error)
	Objects(context.Context, string, string, int, int) (explore.ObjectList, error)
	Object(context.Context, string, int, int, int) (explore.ObjectDetail, error)
}

// LoopbackAddress rejects wildcard/LAN listeners. Viewing always stays local.
func LoopbackAddress(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("listen must be a loopback host:port: %w", err)
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("listen address must use a loopback IP or localhost")
	}
	return nil
}

// Handler accepts only loopback Host headers and same-origin browser requests.
// No API mutates the capture, accepts arbitrary paths, or initiates networking.
func Handler(store Store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; font-src 'self'; base-uri 'none'; object-src 'none'; frame-ancestors 'none'; form-action 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Cache-Control", "no-store")
		if !loopbackHost(r.Host) {
			fail(w, http.StatusForbidden, "loopback Host required")
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" {
			u, err := url.Parse(origin)
			if err != nil || u.Scheme != "http" || u.Host != r.Host || u.User != nil {
				fail(w, http.StatusForbidden, "same-origin access required")
				return
			}
		}
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			fail(w, http.StatusMethodNotAllowed, "read-only GET requests required")
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/api/") {
			name, contentType := "", ""
			switch r.URL.Path {
			case "/":
				name, contentType = "index.html", "text/html; charset=utf-8"
			case "/app.js":
				name, contentType = "app.js", "text/javascript; charset=utf-8"
			case "/style.css":
				name, contentType = "style.css", "text/css; charset=utf-8"
			default:
				fail(w, http.StatusNotFound, "not found")
				return
			}
			data, err := assets.ReadFile("assets/" + name)
			if err != nil {
				fail(w, http.StatusInternalServerError, "embedded asset unavailable")
				return
			}
			w.Header().Set("Content-Type", contentType)
			_, _ = w.Write(data)
			return
		}
		q := r.URL.Query()
		for _, key := range []string{"search", "resource", "namespace", "id", "label_key", "label_value", "group_scope", "group_value"} {
			if len(q.Get(key)) > 16384 {
				fail(w, http.StatusBadRequest, "query value too long")
				return
			}
		}
		if r.URL.Path == "/api/info" {
			respond(w, store.Info())
			return
		}
		from, to, err := interval(q, store.Info().WindowCount)
		if err != nil {
			fail(w, http.StatusBadRequest, err.Error())
			return
		}
		limit, err := integer(q, "limit", 20, 1, 100)
		if err != nil {
			fail(w, http.StatusBadRequest, err.Error())
			return
		}
		offset, err := integer(q, "offset", 0, 0, 100000000)
		if err != nil {
			fail(w, http.StatusBadRequest, err.Error())
			return
		}
		points, err := integer(q, "points", 800, 16, 4000)
		if err != nil {
			fail(w, http.StatusBadRequest, err.Error())
			return
		}
		var result any
		switch r.URL.Path {
		case "/api/curve":
			result, err = store.Curve(r.Context(), from, to, points)
		case "/api/rankings":
			labelFilter := q.Get("label_filter") == "true"
			if value := q.Get("label_filter"); value != "" && value != "true" && value != "false" {
				fail(w, http.StatusBadRequest, "label_filter must be true or false")
				return
			}
			scope, metric := q.Get("scope"), q.Get("metric")
			if scope == "" {
				scope = "resource"
			}
			if metric == "" {
				metric = "writes"
			}
			result, err = store.Rankings(r.Context(), explore.Query{From: from, To: to, Scope: scope, Metric: metric, Search: q.Get("search"), Resource: q.Get("resource"), Namespace: q.Get("namespace"), LabelKey: q.Get("label_key"), LabelValue: q.Get("label_value"), LabelFilter: labelFilter, GroupScope: q.Get("group_scope"), GroupValue: q.Get("group_value"), Limit: limit, Offset: offset})
		case "/api/objects":
			result, err = store.Objects(r.Context(), q.Get("search"), q.Get("resource"), limit, offset)
		case "/api/object":
			if q.Get("id") == "" {
				fail(w, http.StatusBadRequest, "object id required")
				return
			}
			result, err = store.Object(r.Context(), q.Get("id"), from, to, points)
		default:
			fail(w, http.StatusNotFound, "API route not found")
			return
		}
		if err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, explore.ErrNotFound) {
				status = http.StatusNotFound
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				status = http.StatusRequestTimeout
			}
			fail(w, status, err.Error())
			return
		}
		respond(w, result)
	})
}

func loopbackHost(hostport string) bool {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		host = hostport
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
func interval(q url.Values, count int) (int, int, error) {
	from, err := integer(q, "from", 0, 0, count-1)
	if err != nil {
		return 0, 0, err
	}
	to, err := integer(q, "to", count, 1, count)
	if err != nil {
		return 0, 0, err
	}
	if to <= from {
		return 0, 0, fmt.Errorf("range must satisfy 0 <= from < to <= window count")
	}
	return from, to, nil
}
func integer(q url.Values, key string, fallback, min, max int) (int, error) {
	if q.Get(key) == "" {
		if fallback < min || fallback > max {
			return 0, fmt.Errorf("no valid %s range", key)
		}
		return fallback, nil
	}
	n, err := strconv.Atoi(q.Get(key))
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("%s must be an integer from %d to %d", key, min, max)
	}
	return n, nil
}
func respond(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(value)
}
func fail(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
