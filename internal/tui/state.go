// Package tui provides an offline terminal explorer over the shared query store.
package tui

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/yckao/etcd-trend/internal/explore"
)

type Store interface {
	Info() explore.Info
	Curve(context.Context, int, int, int) ([]explore.Point, error)
	Rankings(context.Context, explore.Query) (explore.RankingResult, error)
	Objects(context.Context, string, string, int, int) (explore.ObjectList, error)
	Object(context.Context, string, int, int, int) (explore.ObjectDetail, error)
}

type Config struct {
	From, To                        int
	Metric, Scope                   string
	Search, ObjectID                string
	Width, Height                   int
	Namespace, LabelKey, LabelValue string
	LabelFilter, Metadata           bool
}

var Scopes = []string{"resource", "object", "namespace", "api-group", "kind", "label", "namespace-label", "controller", "manager"}

var Metrics = []string{"writes", "history", "retained", "compacted", "db-growth", "free-growth", "free-net"}

type Data struct {
	Curve   []explore.Point
	Ranking explore.RankingResult
	Objects explore.ObjectList
	Detail  *explore.ObjectDetail
	Err     error
}

type State struct {
	Info                                                               explore.Info
	From, To, Width, Height                                            int
	Metric, Scope, Resource, Search, ObjectID                          string
	Mode, ReturnMode                                                   string
	Selected, Offset                                                   int
	DetailOffset                                                       int
	Searching, Help, Loading                                           bool
	Edit                                                               string
	EditKind                                                           string
	EditError, Menu                                                    string
	MenuSelected, MenuOffset, HelpOffset                               int
	Namespace, LabelKey, LabelValue, GroupScope, GroupValue, GroupName string
	LabelFilter, MetadataView                                          bool
	Data                                                               Data
}

func NewState(info explore.Info, cfg Config) (*State, error) {
	if cfg.To < 0 {
		cfg.To = info.WindowCount
	}
	if cfg.Width == 0 {
		cfg.Width = 120
	}
	if cfg.Height == 0 {
		cfg.Height = 36
	}
	if cfg.Metric == "" {
		cfg.Metric = "writes"
	}
	if cfg.Scope == "" {
		cfg.Scope = "resource"
	}
	validMetric := false
	for _, m := range Metrics {
		if m == cfg.Metric {
			validMetric = true
		}
	}
	if info.WindowCount < 1 || cfg.From < 0 || cfg.From >= cfg.To || cfg.To > info.WindowCount {
		return nil, fmt.Errorf("range must satisfy 0 <= from < to <= %d", info.WindowCount)
	}
	if !validMetric || !explore.ValidScope(cfg.Scope) {
		return nil, fmt.Errorf("invalid metric or scope")
	}
	s := &State{Info: info, From: cfg.From, To: cfg.To, Width: cfg.Width, Height: cfg.Height, Metric: cfg.Metric, Scope: cfg.Scope, Search: cfg.Search, Mode: "ranking", ReturnMode: "ranking", Namespace: cfg.Namespace, LabelKey: cfg.LabelKey, LabelValue: cfg.LabelValue, LabelFilter: cfg.LabelFilter, MetadataView: cfg.Metadata}
	if s.LabelKey == "" && len(info.LabelKeys) > 0 {
		s.LabelKey = info.LabelKeys[0]
	}
	if cfg.Search != "" {
		s.Mode = "objects"
	}
	if cfg.ObjectID != "" {
		s.Mode = "detail"
		s.ObjectID = cfg.ObjectID
	}
	return s, nil
}

func (s *State) PageSize() int {
	n := s.Height - 18
	if n < 1 {
		n = 1
	}
	if n > 100 {
		n = 100
	}
	return n
}

func (s *State) rowCount() int {
	if s.Mode == "objects" {
		return len(s.Data.Objects.Items)
	}
	return len(s.Data.Ranking.Items)
}
func (s *State) total() int {
	if s.Mode == "objects" {
		return s.Data.Objects.Total
	}
	return s.Data.Ranking.Total
}
func (s *State) resetPage() { s.Selected, s.Offset, s.DetailOffset = 0, 0, 0 }

// Key returns whether a new query is needed and whether the UI should quit.
// Text keys contain a single UTF-8 rune; named keys are emitted by the decoder.
func (s *State) Key(key string) (query, quit bool) {
	if key == "ctrlc" {
		return false, true
	}
	if s.Searching {
		switch key {
		case "esc", "ctrlg":
			s.Searching = false
			s.EditError = ""
		case "enter":
			switch s.EditKind {
			case "range":
				from, to, err := s.parseRange(s.Edit)
				if err != nil {
					s.EditError = err.Error()
					return false, false
				}
				s.From, s.To = from, to
			case "namespace":
				s.Namespace = s.Edit
				s.Mode = "ranking"
			case "label":
				s.LabelValue = s.Edit
				s.LabelFilter = true
				s.Mode = "ranking"
			default:
				s.Search = s.Edit
				s.Mode = "objects"
			}
			s.Searching = false
			if s.EditKind != "range" {
				s.ObjectID = ""
			}
			s.EditError = ""
			s.resetPage()
			return true, false
		case "ctrlu":
			s.Edit = ""
			s.EditError = ""
		case "ctrlw":
			value := strings.TrimRight(s.Edit, " ")
			if at := strings.LastIndexByte(value, ' '); at >= 0 {
				s.Edit = value[:at+1]
			} else {
				s.Edit = ""
			}
			s.EditError = ""
		case "backspace":
			s.EditError = ""
			if len(s.Edit) > 0 {
				_, n := utf8.DecodeLastRuneInString(s.Edit)
				s.Edit = s.Edit[:len(s.Edit)-n]
			}
		default:
			if utf8.RuneCountInString(key) == 1 && len(s.Edit) < 4096 {
				r, _ := utf8.DecodeRuneInString(key)
				if r >= 32 && r != 127 {
					s.Edit += key
				}
			}
		}
		return false, false
	}
	if key == "q" {
		return false, true
	}
	if s.Menu != "" {
		return s.menuKey(key)
	}
	if key == "?" {
		s.Help = !s.Help
		s.HelpOffset = 0
		return false, false
	}
	if s.Help {
		switch key {
		case "esc", "e", "ctrlg", "enter":
			s.Help = false
		case "c":
			s.openMenu("commands")
		case "j", "down":
			s.HelpOffset = min(max(0, len(helpLines)-1), s.HelpOffset+1)
		case "k", "up":
			s.HelpOffset = max(0, s.HelpOffset-1)
		case "d", "pgdown":
			s.HelpOffset = min(max(0, len(helpLines)-1), s.HelpOffset+max(1, s.Height-4))
		case "u", "pgup":
			s.HelpOffset = max(0, s.HelpOffset-max(1, s.Height-4))
		case "t", "home":
			s.HelpOffset = 0
		case "b", "end":
			s.HelpOffset = max(0, len(helpLines)-max(1, s.Height-4))
		}
		return false, false
	}

	changedRange := false
	switch key {
	case "c":
		s.openMenu("commands")
		return false, false
	case "r":
		s.Searching = true
		s.EditKind = "range"
		s.Edit = fmt.Sprintf("%d %d", s.From, s.To)
		s.EditError = ""
		return false, false
	case "left", "h":
		if s.From > 0 {
			s.From--
			s.To--
			changedRange = true
		}
	case "right", "l":
		if s.To < s.Info.WindowCount {
			s.From++
			s.To++
			changedRange = true
		}
	case "[":
		if s.From > 0 {
			s.From--
			changedRange = true
		}
	case "]":
		if s.From+1 < s.To {
			s.From++
			changedRange = true
		}
	case "{":
		if s.To > s.From+1 {
			s.To--
			changedRange = true
		}
	case "}":
		if s.To < s.Info.WindowCount {
			s.To++
			changedRange = true
		}
	case "a":
		s.From, s.To = 0, s.Info.WindowCount
		changedRange = true
	case "m":
		for i, m := range Metrics {
			if m == s.Metric {
				s.Metric = Metrics[(i+1)%len(Metrics)]
				break
			}
		}
		s.resetPage()
		return true, false
	case "g":
		for i, scope := range Scopes {
			if scope == s.Scope {
				for n := 1; n <= len(Scopes); n++ {
					next := Scopes[(i+n)%len(Scopes)]
					if (next == "label" || next == "namespace-label") && len(s.Info.LabelKeys) == 0 {
						continue
					}
					s.Scope = next
					break
				}
				break
			}
		}
		s.GroupScope, s.GroupValue, s.GroupName = "", "", ""
		s.Mode = "ranking"
		s.ObjectID = ""
		s.resetPage()
		return true, false
	case "L":
		if len(s.Info.LabelKeys) == 0 {
			return false, false
		}
		for i, key := range s.Info.LabelKeys {
			if key == s.LabelKey {
				s.LabelKey = s.Info.LabelKeys[(i+1)%len(s.Info.LabelKeys)]
				break
			}
		}
		s.GroupScope, s.GroupValue, s.GroupName = "", "", ""
		s.LabelFilter = false
		s.LabelValue = ""
		s.Mode = "ranking"
		s.resetPage()
		return true, false
	case "n":
		s.Searching = true
		s.EditError = ""
		s.EditKind = "namespace"
		s.Edit = s.Namespace
		return false, false
	case "v":
		if len(s.Info.LabelKeys) == 0 {
			return false, false
		}
		s.Searching = true
		s.EditError = ""
		s.EditKind = "label"
		s.Edit = s.LabelValue
		return false, false
	case "x":
		s.Namespace, s.Resource, s.Search, s.GroupScope, s.GroupValue, s.GroupName, s.LabelValue = "", "", "", "", "", "", ""
		s.LabelFilter = false
		s.Mode = "ranking"
		s.ObjectID = ""
		s.resetPage()
		return true, false
	case "i":
		if s.Mode == "detail" {
			s.MetadataView = !s.MetadataView
			s.DetailOffset = 0
		}
		return false, false
	case "tab":
		if s.Scope == "resource" {
			s.Scope = "object"
		} else {
			s.Scope = "resource"
		}
		s.Mode = "ranking"
		s.ObjectID = ""
		s.GroupScope, s.GroupValue, s.GroupName = "", "", ""
		s.resetPage()
		return true, false
	case "o":
		s.Mode = "objects"
		s.ObjectID = ""
		s.resetPage()
		return true, false
	case "/":
		s.Searching = true
		s.EditError = ""
		s.EditKind = "search"
		s.Edit = s.Search
		return false, false
	case "esc", "e", "ctrlg":
		if s.Mode == "detail" {
			s.Mode = s.ReturnMode
			s.ObjectID = ""
			return true, false
		}
		if s.GroupScope != "" {
			s.Scope = s.GroupScope
			s.GroupScope, s.GroupValue, s.GroupName = "", "", ""
		} else if s.Resource != "" {
			s.Resource = ""
			s.Scope = "resource"
		} else if s.Search != "" {
			s.Search = ""
		} else {
			s.Mode = "ranking"
		}
		s.resetPage()
		return true, false
	case "enter":
		if s.Loading {
			return false, false
		}
		if s.Mode == "ranking" && s.Selected < len(s.Data.Ranking.Items) {
			r := s.Data.Ranking.Items[s.Selected]
			if r.Group != nil {
				s.GroupScope = r.Group.Scope
				s.GroupValue = r.Group.Key
				s.GroupName = r.Group.Value
				s.Scope = "object"
				// The selected group token carries the group constraint. Its
				// display search need not match any underlying object identity.
				s.Search = ""
				s.resetPage()
				return true, false
			}
			if s.Scope == "resource" {
				s.Resource = r.Identity.Resource
				s.Scope = "object"
				s.resetPage()
				return true, false
			}
			s.ObjectID = r.ID
			s.ReturnMode = s.Mode
			s.Mode = "detail"
			s.DetailOffset = 0
			return true, false
		}
		if s.Mode == "objects" && s.Selected < len(s.Data.Objects.Items) {
			s.ObjectID = s.Data.Objects.Items[s.Selected].ID
			s.ReturnMode = s.Mode
			s.Mode = "detail"
			s.DetailOffset = 0
			return true, false
		}
	case "down", "j":
		if s.Mode == "detail" {
			if s.Data.Detail != nil && s.DetailOffset+1 < s.detailCount() {
				s.DetailOffset++
			}
			return false, false
		}
		if s.Selected+1 < s.rowCount() {
			s.Selected++
		} else if s.Offset+s.rowCount() < s.total() {
			s.Offset += s.PageSize()
			s.Selected = 0
			return true, false
		}
	case "up", "k":
		if s.Mode == "detail" {
			s.DetailOffset = max(0, s.DetailOffset-1)
			return false, false
		}
		if s.Selected > 0 {
			s.Selected--
		} else if s.Offset > 0 {
			s.Offset = max(0, s.Offset-s.PageSize())
			s.Selected = 0
			return true, false
		}
	case "pgdown", "d":
		if s.Mode == "detail" {
			if s.Data.Detail != nil && s.detailCount() > 0 {
				s.DetailOffset = min(s.detailCount()-1, s.DetailOffset+max(1, s.Height-24))
			}
			return false, false
		}
		if s.Offset+s.PageSize() < s.total() {
			s.Offset += s.PageSize()
			s.Selected = 0
			return true, false
		}
	case "pgup", "u":
		if s.Mode == "detail" {
			s.DetailOffset = max(0, s.DetailOffset-max(1, s.Height-24))
			return false, false
		}
		if s.Offset > 0 {
			s.Offset = max(0, s.Offset-s.PageSize())
			s.Selected = 0
			return true, false
		}
	case "home", "t":
		if s.Mode == "detail" {
			s.DetailOffset = 0
			return false, false
		}
		s.resetPage()
		return true, false
	case "end", "b":
		if s.Mode == "detail" {
			if s.Data.Detail != nil {
				s.DetailOffset = max(0, s.detailCount()-max(1, s.Height-24))
			}
			return false, false
		}
		if s.total() > 0 {
			s.Offset = (s.total() - 1) / s.PageSize() * s.PageSize()
			s.Selected = 0
			return true, false
		}
	}
	if changedRange {
		s.resetPage()
		return true, false
	}
	return false, false
}

func (s *State) Resize(width, height int) bool {
	if s.Width == width && s.Height == height {
		return false
	}
	s.Width, s.Height = width, height
	s.resetPage()
	return true
}

// Query operates on an immutable state snapshot and can run off the input loop.
func (s State) Query(ctx context.Context, store Store) Data {
	var d Data
	d.Curve, d.Err = store.Curve(ctx, 0, s.Info.WindowCount, min(1000, max(24, s.Width*2)))
	if d.Err != nil {
		return d
	}
	switch s.Mode {
	case "objects":
		d.Objects, d.Err = store.Objects(ctx, s.Search, s.Resource, s.PageSize(), s.Offset)
	case "detail":
		var detail explore.ObjectDetail
		detail, d.Err = store.Object(ctx, s.ObjectID, s.From, s.To, min(1000, max(24, s.Width*2)))
		if d.Err == nil {
			d.Detail = &detail
		}
	default:
		d.Ranking, d.Err = store.Rankings(ctx, explore.Query{From: s.From, To: s.To, Scope: s.Scope, Metric: s.Metric, Search: s.Search, Resource: s.Resource, Namespace: s.Namespace, LabelKey: s.LabelKey, LabelValue: s.LabelValue, LabelFilter: s.LabelFilter, GroupScope: s.GroupScope, GroupValue: s.GroupValue, Limit: s.PageSize(), Offset: s.Offset})
	}
	return d
}

func metricLabel(metric string) string {
	switch metric {
	case "writes":
		return "Value bytes written [measured]"
	case "history":
		return "Positive net history growth [measured MVCC]"
	case "retained":
		return "Positive net retained growth [measured MVCC]"
	case "compacted":
		return "Compacted MVCC bytes [measured]"
	case "db-growth":
		return "DB growth association [estimated]"
	case "free-growth":
		return "Gross free growth association [estimated]"
	case "free-net":
		return "Net free growth after reuse [estimated]"
	}
	return strings.ToUpper(metric)
}

func (s *State) detailCount() int {
	if s.Data.Detail == nil {
		return 0
	}
	if s.MetadataView {
		return len(s.metadataLines())
	}
	return len(s.Data.Detail.Points)
}
