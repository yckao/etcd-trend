package tui

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yckao/etcd-trend/internal/analysis"
)

// Safe makes every data-derived string terminal-inert and single-line. Even
// binary/unclassified keys cannot emit OSC/CSI sequences or spoof new rows.
func Safe(s string) string {
	q := strconv.QuoteToASCII(s)
	return q[1 : len(q)-1]
}
func clip(s string, width int) string {
	if width <= 0 {
		return ""
	}
	if len(s) <= width {
		return s
	}
	if width < 4 {
		return s[:width]
	}
	return s[:width-3] + "..."
}
func bytes(n int64) string {
	a := math.Abs(float64(n))
	unit := "B"
	for _, u := range []string{"KiB", "MiB", "GiB", "TiB"} {
		if a < 1024 {
			break
		}
		a /= 1024
		unit = u
	}
	if n < 0 {
		a = -a
	}
	if unit == "B" {
		return fmt.Sprintf("%d B", n)
	}
	return fmt.Sprintf("%.2f %s", a, unit)
}
func duration(n int64) string { return time.Duration(n).Round(time.Millisecond).String() }
func (s *State) timeAt(window int) int64 {
	if window >= s.Info.WindowCount {
		return s.Info.DurationNS
	}
	return int64(window) * s.Info.WindowNS
}

func (s *State) Render() string {
	w, h := s.Width, s.Height
	if w < 20 {
		w = 20
	}
	if h < 5 {
		h = 5
	}
	lines := []string{"ETCD TREND  /  OFFLINE EXPLORER", fmt.Sprintf("Source %s  |  %d windows  |  %d objects  |  revision %d", Safe(s.Info.SourceVersion), s.Info.WindowCount, s.Info.ObjectCount, s.Info.FinalRevision)}
	if s.Width < 72 || s.Height < 20 {
		lines = append(lines, "Terminal is too small for the explorer.", "Resize to at least 72 columns x 20 rows.", "q: quit    Ctrl-C: exit and restore terminal")
		return frame(lines, w, h)
	}
	if s.Menu != "" {
		return frame(append(lines, s.menuLines()...), w, h)
	}
	if s.Help {
		end := min(len(helpLines), s.HelpOffset+max(1, h-4))
		start := min(s.HelpOffset, end)
		lines = append(lines, helpLines[start:end]...)
		for len(lines) < h-1 {
			lines = append(lines, "")
		}
		return frame(append(lines, "j/k scroll  d/u pages  t/b first/last  e back  c commands"), w, h)
	}
	lines = append(lines, fmt.Sprintf("Selected [%d,%d)  %s -> %s  |  r range   h/l shift   a all", s.From, s.To, duration(s.timeAt(s.From)), duration(s.timeAt(s.To))))
	if !s.Searching {
		lines = append(lines, s.overview()...)
	}
	lines = append(lines, strings.Repeat("-", w))
	if s.Searching {
		lines = append(lines, s.editLines()...)
	} else if s.Loading {
		lines = append(lines, "Querying selected interval...")
	} else if s.Data.Err != nil {
		lines = append(lines, "Selected interval data is unavailable.", Safe(s.Data.Err.Error()))
	} else if s.Mode == "detail" {
		lines = append(lines, s.detailLines()...)
	} else {
		lines = append(lines, s.tableLines()...)
	}
	for len(lines) < h-3 {
		lines = append(lines, "")
	}
	if len(lines) > h-3 {
		lines = lines[:h-3]
	}
	status := "Ready. Physical shares are estimates; free ratio = reusable free / DB size."
	if s.Data.Err != nil {
		status = "ERROR: " + Safe(s.Data.Err.Error())
	}
	if s.Loading {
		status = "Loading selected range... (q cancels and exits)"
	}
	if s.Searching {
		status = "Enter apply  Ctrl-U clear  Ctrl-W delete word  Ctrl-G cancel"
		if s.EditError != "" {
			status = "Invalid input; edit the bounds or press Ctrl-G to cancel."
		}
	}
	lines = append(lines, status, "j/k rows  d/u pages  t/b first/last  Enter open  e back  r range", "c commands  g group  m metric  n namespace  v label value  q quit")

	return frame(lines, w, h)
}

func frame(lines []string, width, height int) string {
	if len(lines) > height {
		lines = lines[:height]
	}
	for i := range lines {
		lines[i] = clip(lines[i], width)
	}
	return strings.Join(lines, "\n") + "\n"
}

func (s *State) overview() []string {
	points := s.Data.Curve
	if len(points) == 0 {
		return []string{"GLOBAL WINDOW-ENDPOINT CURVES", "DB      (loading)", "In-use  (loading)", "Free    (loading)", "Ratio   (loading)", ""}
	}
	w := max(12, s.Width-39)
	times := make([]int64, len(points))
	db := make([]float64, len(points))
	used := make([]float64, len(points))
	free := make([]float64, len(points))
	ratio := make([]float64, len(points))
	var scale float64
	for i, p := range points {
		times[i] = p.TimeNS
		db[i] = float64(p.DBSize)
		used[i] = float64(p.DBSizeInUse)
		free[i] = float64(p.FreeBytes)
		ratio[i] = p.FragmentationPercent
		scale = max(scale, db[i])
	}
	last := points[len(points)-1]
	return []string{
		fmt.Sprintf("GLOBAL WINDOW-ENDPOINT CURVES  byte scale 0..%s", bytes(int64(scale))),
		fmt.Sprintf("DB      |%s| %s", spark(times, db, w, scale), bytes(last.DBSize)),
		fmt.Sprintf("In-use  |%s| %s", spark(times, used, w, scale), bytes(last.DBSizeInUse)),
		fmt.Sprintf("Free    |%s| %s", spark(times, free, w, scale), bytes(last.FreeBytes)),
		fmt.Sprintf("Ratio %% |%s| %.2f%% (0..100%%)", spark(times, ratio, w, 100), last.FragmentationPercent),
		fmt.Sprintf("Time     %s  %s", duration(times[0]), strings.Repeat(" ", max(0, w-len(duration(times[len(times)-1]))-3))+duration(times[len(times)-1])),
	}
}

func (s *State) tableLines() []string {
	filter := ""
	if s.Resource != "" {
		filter += " resource=" + Safe(s.Resource)
	}
	if s.Namespace != "" {
		filter += " ns=" + Safe(s.Namespace)
	}
	if s.LabelKey != "" {
		filter += " label=" + Safe(s.LabelKey)
	}
	if s.LabelFilter {
		filter += " value=" + Safe(s.LabelValue)
	}
	if s.GroupScope != "" {
		filter += " group=" + Safe(s.GroupName)
	}
	if s.Search != "" {
		filter += " search=" + Safe(s.Search)
	}
	if s.Mode == "objects" {
		lines := []string{"OBJECT CATALOG (all keys; tenant filters do not apply)", fmt.Sprintf("Rows %d..%d / %d  |  Enter opens full selected-range history", s.Offset+1, s.Offset+len(s.Data.Objects.Items), s.Data.Objects.Total), fmt.Sprintf("  %-*s %12s %12s", max(10, s.Width-31), "STORAGE KEY", "FINAL LIVE", "FINAL HIST")}
		for i, r := range s.Data.Objects.Items {
			mark := "  "
			if i == s.Selected {
				mark = "> "
			}
			lines = append(lines, fmt.Sprintf("%s%-*s %12s %12s", mark, max(10, s.Width-31), clip(identityKey(r.Identity), max(10, s.Width-31)), bytes(r.Final.LiveValueBytes), bytes(r.Final.HistoryBytes)))
		}
		if len(s.Data.Objects.Items) == 0 && !s.Loading {
			lines = append(lines, "  No matching objects. Esc clears a resource/search filter.")
		}
		return lines
	}
	r := s.Data.Ranking
	t := r.Totals
	startFree := t.PhysicalStart.DBSize - t.PhysicalStart.DBSizeInUse
	endFree := t.PhysicalEnd.DBSize - t.PhysicalEnd.DBSizeInUse
	lines := []string{
		fmt.Sprintf("RANGE actual: DB %+d B  free %+d B | unattributed DB/free/reuse %d/%d/%d B", t.PhysicalEnd.DBSize-t.PhysicalStart.DBSize, endFree-startFree, t.UnattributedDBGrowthBytes, t.UnattributedFreeGrowthBytes, t.UnattributedFreeReuseBytes),
		strings.ToUpper(s.Scope) + " RANKING: " + metricLabel(s.Metric) + filter,
		fmt.Sprintf("%d contributors | denominator %d B: %s", r.Total, r.DenominatorBytes, Safe(r.DenominatorLabel)),
		fmt.Sprintf("  %-*s %14s %8s", max(10, s.Width-28), "IDENTITY", "BYTES", "SHARE"),
	}
	for i, item := range r.Items {
		name := Safe(item.Identity.Resource)
		if item.Group != nil {
			name = Safe(item.Group.Value)
		} else if s.Scope == "object" {
			name = identityKey(item.Identity)
		}
		mark := "  "
		if i == s.Selected {
			mark = "> "
		}
		lines = append(lines, fmt.Sprintf("%s%-*s %14d %7.2f%%", mark, max(10, s.Width-28), clip(name, max(10, s.Width-28)), item.Bytes, item.SharePercent))
	}
	if len(r.Items) == 0 && !s.Loading {
		lines = append(lines, "  No positive contributors for this interval/filter. Press o for all objects.")
	}
	return lines
}

func identityKey(id analysis.Identity) string {
	if id.KeyEncoding == "base64" {
		return "base64:" + Safe(id.Key)
	}
	return Safe(id.Key)
}

func (s *State) detailLines() []string {
	d := s.Data.Detail
	if d == nil {
		return []string{"OBJECT DETAIL", "Loading selected object..."}
	}
	if s.MetadataView {
		data := s.metadataLines()
		end := min(len(data), s.DetailOffset+max(1, s.Height-18))
		start := min(s.DetailOffset, end)
		return append([]string{"OBJECT " + identityKey(d.Identity), "Full object history for this interval.", "Ranking tenant, group and label filters do not apply.", "KUBERNETES METADATA AT INTERVAL END  |  j/k scroll  i returns to metrics"}, data[start:end]...)
	}
	m := d.Metrics
	lines := []string{"OBJECT " + identityKey(d.Identity), "Full object history for this interval.", "Ranking tenant, group and label filters do not apply.", fmt.Sprintf("%s / ns=%s / name=%s | mapping=%s", Safe(d.Identity.Resource), Safe(d.Identity.Namespace), Safe(d.Identity.Name), Safe(d.Identity.Confidence)), fmt.Sprintf("Selected stock: live %s -> %s | history %s -> %s", bytes(d.Start.LiveValueBytes), bytes(d.End.LiveValueBytes), bytes(d.Start.HistoryBytes), bytes(d.End.HistoryBytes))}
	var times []int64
	var live, hist []float64
	var scale float64
	for _, p := range d.Points {
		times = append(times, p.TimeNS)
		live = append(live, float64(p.Stock.LiveValueBytes))
		hist = append(hist, float64(p.Stock.HistoryBytes))
		scale = max(scale, float64(p.Stock.LiveValueBytes), float64(p.Stock.HistoryBytes))
	}
	w := max(10, s.Width-24)
	lines = append(lines, fmt.Sprintf("Live    |%s|", spark(times, live, w, scale)), fmt.Sprintf("History |%s|", spark(times, hist, w, scale)))
	lines = append(lines, fmt.Sprintf("Measured: puts=%d deletes=%d writes=%d B compacted=%d B (%d records)", m.Puts, m.Deletes, m.ValueWriteBytes, m.CompactedBytes, m.CompactedRecords), fmt.Sprintf("Estimated: DB +%d B | free +%d B / reuse %d B / net %+d B", m.EstimatedDBGrowthBytes, m.EstimatedFreeGrowthBytes, m.EstimatedFreeReuseBytes, m.EstimatedFreeGrowthBytes-m.EstimatedFreeReuseBytes), fmt.Sprintf("Raft indexes %d..%d | %d boundary points | sampled=%t | i: Kubernetes metadata", d.FirstIndex, d.LastIndex, d.TotalPoints, d.Sampled))
	lines = append(lines, "BOUNDARY       LIVE         HISTORY      WRITTEN       COMPACTED    EST.NET FREE")
	for i := s.DetailOffset; i < len(d.Points) && i < s.DetailOffset+max(1, s.Height-24); i++ {
		p := d.Points[i]
		lines = append(lines, fmt.Sprintf("%-12s %12s %12s %12s %12s %+12d", duration(p.TimeNS), bytes(p.Stock.LiveValueBytes), bytes(p.Stock.HistoryBytes), bytes(p.Metrics.ValueWriteBytes), bytes(p.Metrics.CompactedBytes), p.Metrics.EstimatedFreeGrowthBytes-p.Metrics.EstimatedFreeReuseBytes))
	}
	return lines
}

// spark rebins boundary samples by time, preserving the maximum visible sample
// in each terminal cell. Empty cells carry the last boundary value forward.
func spark(times []int64, values []float64, width int, scale float64) string {
	const levels = " .:-=+*#%@"
	if width < 1 {
		return ""
	}
	if len(values) == 0 {
		return strings.Repeat(" ", width)
	}
	maxima := make([]float64, width)
	seen := make([]bool, width)
	first, last := times[0], times[len(times)-1]
	for i, v := range values {
		col := 0
		if last > first {
			col = int(float64(times[i]-first) / float64(last-first) * float64(width-1))
		}
		col = max(0, min(width-1, col))
		if !seen[col] || v > maxima[col] {
			maxima[col] = v
		}
		seen[col] = true
	}
	b := make([]byte, width)
	previous := values[0]
	for i, v := range maxima {
		if !seen[i] {
			v = previous
		} else {
			previous = v
		}
		n := 0
		if scale > 0 {
			n = int(math.Round(v / scale * float64(len(levels)-1)))
		}
		n = max(0, min(len(levels)-1, n))
		b[i] = levels[n]
	}
	return string(b)
}

var helpLines = []string{
	"60% KEYBOARD: every action is reachable from c (commands)", "",
	"NAVIGATION",
	"  j/k           Rows or detail scroll; d/u next/previous page",
	"  t/b           First/last page or detail boundary",
	"  Enter         Open selected group or object; e returns",
	"  c             Command menu: j/k select, Enter apply, e back",
	"", "TIME SELECTION",
	"  r             Edit bounds: 10 20 windows, or elapsed times 2m 5m",
	"  h/l           Shift selected interval left/right; a selects all",
	"", "GROUPS AND FILTERS",
	"  c             Choose grouping, metric or label key directly",
	"  g / m / L     Cycle grouping / metric / recorded label key",
	"  n / v         Edit namespace / exact object-label value",
	"  / or o        Search / open all-key catalog (no tenant filters)",
	"  x             Clear filters; i toggles object metadata/details",
	"", "TEXT EDITING",
	"  Ctrl-U        Clear field; Ctrl-W removes the previous word",
	"  Enter         Apply; Ctrl-G or Esc cancels; q remains input text",
	"", "EXISTING ALIASES",
	"  Arrows, PgUp/PgDn, Home/End, Esc, Tab and [ ] { } still work",
	"  ?             Help; q or Ctrl-C exits",
	"", "Metadata attribution uses each analysis-window endpoint.",
	"Manager sets are hints, not authenticated writers; owners are immediate.",
	"Object detail shows full history; ranking filters do not apply.",
	"Physical shares are estimates. Free ratio = reusable free / DB size.",
}

func (s *State) editLines() []string {
	title, hint := "SEARCH OBJECTS", "Search all recorded object identities."
	switch s.EditKind {
	case "namespace":
		title = "NAMESPACE FILTER"
		hint = "Exact namespace; leave empty to select all."
	case "label":
		title = "OBJECT LABEL VALUE"
		hint = "Exact value; an empty string is a valid label value."
	case "range":
		title = "SELECT TIME RANGE"
		hint = "Window bounds: START END (end is exclusive)."
	}
	lines := []string{title, hint}
	if s.EditKind == "range" {
		lines = append(lines, "Or elapsed times: 2m 5m (snaps to covering windows).")
	}
	if s.EditKind == "label" {
		lines = append(lines, "Key: "+Safe(s.LabelKey))
	}
	value := Safe(s.Edit)
	if len(value) > s.Width-6 {
		value = "..." + value[len(value)-(s.Width-9):]
	}
	lines = append(lines, "", "> "+value+"_", "")
	if s.EditError != "" {
		lines = append(lines, "Error: "+Safe(s.EditError))
	}
	return lines
}

func (s *State) metadataLines() []string {
	d := s.Data.Detail
	if d == nil {
		return nil
	}
	m := d.Metadata
	var lines []string
	add := func(label, value string) {
		text := Safe(value)
		if text == "" {
			text = "(none recorded)"
		}
		text = Safe(label) + ": " + text
		width := max(12, s.Width)
		for len(text) > width {
			lines = append(lines, text[:width])
			text = "  " + text[width:]
		}
		lines = append(lines, text)
	}

	status := m.Status
	if status == "" {
		status = "not-recorded"
	}
	add("Status", status)
	if m.APIVersion != "" || m.Kind != "" {
		add("API / kind", m.APIVersion+" / "+m.Kind)
	} else {
		add("API / kind", "")
	}
	add("Namespace", m.Namespace)
	add("UID", m.UID)
	if d.MetadataStart.UID != m.UID {
		add("UID at start", d.MetadataStart.UID)
	}
	if m.IdentityMismatch {
		add("Identity", "metadata disagrees with storage path")
	}
	if m.Controller != nil {
		add("Controller", m.Controller.APIVersion+" "+m.Controller.Kind+"/"+m.Controller.Name)
		add("Controller UID", m.Controller.UID)
	} else {
		add("Controller", "")
	}
	add("Manager set", strings.Join(m.Managers, ", "))
	keys := make([]string, 0, len(m.Labels))
	for key := range m.Labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		add("Labels", "none of the configured keys")
	}
	for _, key := range keys {
		labelValue := m.Labels[key]
		if labelValue == "" {
			labelValue = `"" (empty value)`
		}
		add("Label "+key, labelValue)
		if value, ok := d.MetadataStart.Labels[key]; !ok || value != m.Labels[key] {
			before := "(missing)"
			if ok {
				before = value
			}
			add("  at interval start", before)
		}
	}
	var removed []string
	for key := range d.MetadataStart.Labels {
		if _, ok := m.Labels[key]; !ok {
			removed = append(removed, key)
		}
	}
	sort.Strings(removed)
	for _, key := range removed {
		add("Removed label "+key, d.MetadataStart.Labels[key])
	}
	if m.Revision > 0 {
		add("Observed", fmt.Sprintf("revision %d at %s", m.Revision, duration(m.VirtualNS)))
	} else {
		add("Observed", "not recorded")
	}
	add("Semantics", "Latest known metadata at interval end; managers are not authenticated writers.")
	return lines
}
