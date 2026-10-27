package tui

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type command struct{ Label, Action, Value string }

func (s *State) commands() []command {
	switch s.Menu {
	case "group":
		var items []command
		for _, scope := range Scopes {
			if (scope == "label" || scope == "namespace-label") && len(s.Info.LabelKeys) == 0 {
				continue
			}
			items = append(items, command{Label: scope, Action: "scope", Value: scope})
		}
		return items
	case "metric":
		var items []command
		for _, metric := range Metrics {
			items = append(items, command{Label: metricLabel(metric), Action: "metric", Value: metric})
		}
		return items
	case "label":
		var items []command
		for _, key := range s.Info.LabelKeys {
			items = append(items, command{Label: key, Action: "label", Value: key})
		}
		return items
	}
	return []command{
		{Label: "Select time range...", Action: "key", Value: "r"},
		{Label: "Choose grouping...", Action: "menu", Value: "group"},
		{Label: "Choose ranking metric...", Action: "menu", Value: "metric"},
		{Label: "Choose recorded label key...", Action: "menu", Value: "label"},
		{Label: "Edit namespace filter...", Action: "key", Value: "n"},
		{Label: "Edit exact label value...", Action: "key", Value: "v"},
		{Label: "Search all objects...", Action: "key", Value: "/"},
		{Label: "Clear filters", Action: "key", Value: "x"},
		{Label: "Show all object identities", Action: "key", Value: "o"},
		{Label: "Toggle object metadata / metrics", Action: "key", Value: "i"},
		{Label: "Select all time windows", Action: "key", Value: "a"},
		{Label: "Keyboard help", Action: "key", Value: "?"},
		{Label: "Quit", Action: "key", Value: "q"},
	}
}

func (s *State) openMenu(menu string) {
	s.Menu = menu
	s.MenuSelected = 0
	s.MenuOffset = 0
	s.Help = false
}
func (s *State) menuPageSize() int { return max(1, s.Height-7) }
func (s *State) menuKey(key string) (bool, bool) {
	items := s.commands()
	switch key {
	case "esc", "e", "ctrlg":
		s.Menu = ""
		return false, false
	case "c":
		s.openMenu("commands")
		return false, false
	case "j", "down":
		s.MenuSelected = min(max(0, len(items)-1), s.MenuSelected+1)
	case "k", "up":
		s.MenuSelected = max(0, s.MenuSelected-1)
	case "d", "pgdown":
		s.MenuSelected = min(max(0, len(items)-1), s.MenuSelected+s.menuPageSize())
	case "u", "pgup":
		s.MenuSelected = max(0, s.MenuSelected-s.menuPageSize())
	case "t", "home":
		s.MenuSelected = 0
	case "b", "end":
		s.MenuSelected = max(0, len(items)-1)
	case "enter":
		if len(items) == 0 {
			return false, false
		}
		selected := items[s.MenuSelected]
		switch selected.Action {
		case "menu":
			s.openMenu(selected.Value)
			return false, false
		case "key":
			s.Menu = ""
			return s.Key(selected.Value)
		case "scope":
			s.Scope = selected.Value
			s.GroupScope, s.GroupValue, s.GroupName = "", "", ""
		case "metric":
			s.Metric = selected.Value
		case "label":
			s.LabelKey = selected.Value
			s.LabelValue = ""
			s.LabelFilter = false
			s.GroupScope, s.GroupValue, s.GroupName = "", "", ""
		}
		s.Menu = ""
		s.Mode = "ranking"
		s.ObjectID = ""
		s.resetPage()
		return true, false
	}
	if s.MenuSelected < s.MenuOffset {
		s.MenuOffset = s.MenuSelected
	}
	if s.MenuSelected >= s.MenuOffset+s.menuPageSize() {
		s.MenuOffset = s.MenuSelected - s.menuPageSize() + 1
	}
	return false, false
}

func (s *State) menuLines() []string {
	title := map[string]string{"commands": "COMMANDS", "group": "CHOOSE GROUPING", "metric": "CHOOSE METRIC", "label": "CHOOSE LABEL KEY"}[s.Menu]
	lines := []string{"", title + "  |  j/k select, Enter apply, e back"}
	items := s.commands()
	for i := s.MenuOffset; i < len(items) && i < s.MenuOffset+s.menuPageSize(); i++ {
		mark := "  "
		if i == s.MenuSelected {
			mark = "> "
		}
		lines = append(lines, mark+Safe(items[i].Label))
	}
	if len(items) == 0 {
		lines = append(lines, "  No label keys recorded. Replay with --label-key KEY.")
	}
	for len(lines) < s.Height-3 {
		lines = append(lines, "")
	}
	return append(lines, "d/u pages  t/b first/last  c command menu  q quit")
}

// parseRange accepts either integer window boundaries or elapsed durations.
// Time selection expands to covering windows, like dragging the Web UI chart.
func (s *State) parseRange(value string) (int, int, error) {
	parts := strings.Fields(value)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("enter two bounds: 10 20 (windows) or 2m 5m (elapsed time)")
	}
	from, errA := strconv.Atoi(parts[0])
	to, errB := strconv.Atoi(parts[1])
	if errA == nil && errB == nil {
		if from < 0 || to <= from || to > s.Info.WindowCount {
			return 0, 0, fmt.Errorf("windows must satisfy 0 <= start < end <= %d", s.Info.WindowCount)
		}
		return from, to, nil
	}
	a, e1 := time.ParseDuration(parts[0])
	b, e2 := time.ParseDuration(parts[1])
	if e1 != nil || e2 != nil || a < 0 || b <= a || int64(b) > s.Info.DurationNS || s.Info.WindowNS <= 0 {
		return 0, 0, fmt.Errorf("use elapsed times within 0s..%s, for example 2m 5m", duration(s.Info.DurationNS))
	}
	from = int(int64(a) / s.Info.WindowNS)
	end := int64(b) / s.Info.WindowNS
	if int64(b)%s.Info.WindowNS != 0 {
		end++
	}
	to = min(s.Info.WindowCount, int(end))
	if from >= to || from >= s.Info.WindowCount {
		return 0, 0, fmt.Errorf("range must include at least one window")
	}
	return from, to, nil
}
