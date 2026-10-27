package explore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/yckao/etcd-trend/internal/analysis"
	bolt "go.etcd.io/bbolt"
)

const attribution = "Object metrics and stock deltas are grouped by the latest observed metadata at each analysis-window endpoint. Changes within one window are not split. Namespace labels use the Namespace object at that endpoint. Compaction and physical estimates follow the object's then-current classification, not the labels of every historical record. Controller groups partition immediate owner references by dependent namespace; owner scope and ancestor chains are not resolved. Manager groups are observed managedFields sets, not authenticated writers."

func ValidScope(scope string) bool {
	switch scope {
	case "object", "resource", "namespace", "api-group", "kind", "label", "namespace-label", "controller", "manager":
		return true
	}
	return false
}

func (s *Store) validateGrouping(q Query) error {
	if !ValidScope(q.Scope) {
		return fmt.Errorf("unknown aggregation scope %q", q.Scope)
	}
	if q.GroupScope != "" && (!ValidScope(q.GroupScope) || q.GroupScope == "object" || q.GroupValue == "") {
		return fmt.Errorf("group filter requires a non-object scope and its exact group key")
	}
	if q.GroupScope == "" && q.GroupValue != "" {
		return fmt.Errorf("group_value requires group_scope")
	}
	usesLabel := q.Scope == "label" || q.Scope == "namespace-label" || q.GroupScope == "label" || q.GroupScope == "namespace-label" || q.LabelFilter || q.LabelValue != ""
	if !usesLabel {
		return nil
	}
	if q.LabelKey == "" {
		return fmt.Errorf("label aggregation/filter requires label_key; replay with --label-key KEY to record selected labels")
	}
	for _, key := range s.info.LabelKeys {
		if key == q.LabelKey {
			return nil
		}
	}
	return fmt.Errorf("label key %q was not recorded; replay with --label-key KEY (available keys: %s)", q.LabelKey, strings.Join(s.info.LabelKeys, ", "))
}

func metadataOrUnknown(m analysis.Metadata) analysis.Metadata {
	if m.Status == "" {
		m.Status = "not-recorded"
	}
	return m
}

func validateMetadata(m analysis.Metadata, labelKeys []string) error {
	switch m.Status {
	case "decoded-json", "decoded-protobuf", "unavailable", "opaque", "encrypted", "malformed":
	default:
		return fmt.Errorf("invalid recorded metadata status %q", m.Status)
	}
	if m.Revision < 0 || m.VirtualNS < 0 {
		return fmt.Errorf("invalid metadata observation time or revision")
	}
	for key := range m.Labels {
		allowed := false
		for _, selected := range labelKeys {
			if selected == key {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("metadata contains a label absent from the recorded allowlist")
		}
	}
	return nil
}

func decoded(m analysis.Metadata) bool {
	return !m.IdentityMismatch && (m.Status == "decoded-json" || m.Status == "decoded-protobuf")
}

func namespace(id analysis.Identity, m analysis.Metadata) (string, bool) {
	if !m.IdentityMismatch && m.Namespace != "" {
		return m.Namespace, true
	}
	if id.Namespace != "" {
		return id.Namespace, true
	}
	return "", id.Scope == "cluster"
}

func group(scope, status, value, labelKey string, identity ...string) Group {
	parts := []string{scope, status, labelKey}
	parts = append(parts, identity...)
	key, _ := json.Marshal(parts)
	return Group{Scope: scope, Status: status, Value: value, LabelKey: labelKey, Key: string(key)}
}

func unavailable(scope, labelKey string, m analysis.Metadata) Group {
	status := metadataOrUnknown(m).Status
	if m.IdentityMismatch {
		status = "identity-mismatch"
	}
	return group(scope, "unavailable", "(metadata unavailable: "+status+")", labelKey, status)
}

// rowGroup returns one group per row: even unknown or missing metadata remains
// represented, and multi-manager objects never multiply attributed bytes.
func rowGroup(tx *bolt.Tx, row analysis.Row, win int, scope, labelKey string) (Group, error) {
	m, id := row.Metadata, row.Identity
	ns, nsKnown := namespace(id, m)
	switch scope {
	case "resource":
		return group(scope, "present", id.Resource, "", id.Resource, id.APIGroup), nil
	case "namespace":
		if ns != "" {
			return group(scope, "present", ns, "", ns), nil
		}
		if nsKnown {
			return group(scope, "cluster-scoped", "(cluster scoped)", ""), nil
		}
		return group(scope, "unavailable", "(unknown namespace)", ""), nil
	case "api-group":
		if decoded(m) {
			value := m.APIGroup
			if value == "" {
				value = "(core)"
			}
			return group(scope, "present", value, "", m.APIGroup), nil
		}
		if id.APIGroup != "" {
			return group(scope, "path-hint", id.APIGroup+" (path hint)", "", id.APIGroup), nil
		}
		return unavailable(scope, "", m), nil
	case "kind":
		if !decoded(m) || m.Kind == "" {
			return unavailable(scope, "", m), nil
		}
		g := m.APIGroup
		if g == "" {
			g = "core"
		}
		return group(scope, "present", g+"/"+m.Kind, "", m.APIGroup, m.Kind), nil
	case "label":
		if !decoded(m) {
			return unavailable(scope, labelKey, m), nil
		}
		if value, ok := m.Labels[labelKey]; ok {
			display := value
			if value == "" {
				display = "(empty value)"
			}
			return group(scope, "present", display, labelKey, value), nil
		}
		return group(scope, "missing", "(label missing)", labelKey), nil
	case "namespace-label":
		if ns == "" {
			if nsKnown {
				return group(scope, "cluster-scoped", "(cluster scoped)", labelKey), nil
			}
			return group(scope, "unavailable", "(unknown namespace)", labelKey), nil
		}
		var objectID string
		if err := getJSON(tx.Bucket(bucketNamespaces), []byte(ns), &objectID); err != nil {
			return Group{}, err
		}
		if objectID == "" || objectID == "ambiguous" {
			return group(scope, "unavailable", "(namespace metadata unavailable)", labelKey), nil
		}
		stock, metadata, err := objectAt(tx, objectID, win)
		if err != nil {
			return Group{}, err
		}
		if stock.LiveObjects == 0 {
			return group(scope, "unavailable", "(namespace not live at window end)", labelKey), nil
		}
		if !decoded(metadata) {
			return unavailable(scope, labelKey, metadata), nil
		}
		if value, ok := metadata.Labels[labelKey]; ok {
			display := value
			if value == "" {
				display = "(empty value)"
			}
			return group(scope, "present", display, labelKey, value), nil
		}
		return group(scope, "missing", "(namespace label missing)", labelKey), nil
	case "controller":
		if !decoded(m) {
			return unavailable(scope, "", m), nil
		}
		if m.Controller == nil {
			return group(scope, "missing", "(no controller owner)", ""), nil
		}
		owner := m.Controller
		prefix := "dependent ns=" + ns
		if ns == "" && nsKnown {
			prefix = "cluster dependent"
		}
		if ns == "" && !nsKnown {
			prefix = "dependent namespace unknown"
		}
		display := prefix + " / " + owner.APIVersion + " / " + owner.Kind + " / " + owner.Name
		if owner.UID != "" {
			display += " [" + owner.UID + "]"
		}
		return group(scope, "present", display, "", ns, owner.APIVersion, owner.Kind, owner.Name, owner.UID), nil
	case "manager":
		if !decoded(m) {
			return unavailable(scope, "", m), nil
		}
		if len(m.Managers) == 0 {
			return group(scope, "missing", "(no managedFields managers)", ""), nil
		}
		managers := append([]string{}, m.Managers...)
		sort.Strings(managers)
		unique := managers[:0]
		for _, manager := range managers {
			if len(unique) == 0 || unique[len(unique)-1] != manager {
				unique = append(unique, manager)
			}
		}
		return group(scope, "present", strings.Join(unique, " + "), "", unique...), nil
	}
	return Group{}, fmt.Errorf("unsupported group scope %q", scope)
}

// objectAt uses metadata no newer than the requested window endpoint. Final
// inventories are deliberately not consulted for historical namespace joins.
func objectAt(tx *bolt.Tx, id string, win int) (analysis.Stock, analysis.Metadata, error) {
	var c catalog
	if err := getJSON(tx.Bucket(bucketObjects), []byte(id), &c); err != nil {
		return analysis.Stock{}, analysis.Metadata{}, err
	}
	stock, metadata := c.Baseline, metadataOrUnknown(c.BaselineMetadata)
	cursor := tx.Bucket(bucketHistory).Cursor()
	key, value := cursor.Seek(historyKey(id, win+1))
	if key == nil {
		key, value = cursor.Last()
	} else {
		key, value = cursor.Prev()
	}
	prefix := append([]byte(id), 0)
	if key != nil && bytes.HasPrefix(key, prefix) {
		var row analysis.Row
		if err := json.Unmarshal(value, &row); err != nil {
			return stock, metadata, err
		}
		stock, metadata = row.End, metadataOrUnknown(row.Metadata)
	}
	return stock, metadata, nil
}

func indexNamespace(w *writer, id analysis.Identity, m analysis.Metadata, objectID string) error {
	name := ""
	if id.Resource == "namespaces" && id.Scope == "cluster" {
		name = id.Name
	}
	if decoded(m) && m.Kind == "Namespace" && m.APIGroup == "" && m.Namespace == "" {
		name = m.Name
	}
	if name == "" {
		return nil
	}
	var old string
	if err := getJSON(w.tx.Bucket(bucketNamespaces), []byte(name), &old); err != nil {
		return err
	}
	if old != "" && old != objectID {
		objectID = "ambiguous"
	}
	return w.putJSON(bucketNamespaces, []byte(name), objectID, false)
}

func matchesMetadata(id analysis.Identity, m analysis.Metadata, search, resource string) bool {
	if resource != "" && id.Resource != resource {
		return false
	}
	if matches(id, search, resource) {
		return true
	}
	needle := strings.ToLower(search)
	for _, value := range []string{m.Name, m.Namespace, m.UID, m.APIVersion, m.Kind} {
		if strings.Contains(strings.ToLower(value), needle) {
			return true
		}
	}
	for key, value := range m.Labels {
		if strings.Contains(strings.ToLower(key+"="+value), needle) {
			return true
		}
	}
	return false
}
