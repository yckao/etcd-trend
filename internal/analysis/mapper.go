package analysis

import (
	"encoding/base64"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Identity labels a storage key without decoding the value or contacting a
// Kubernetes API. Resource is the storage prefix, not a verified API resource
// or GVR. The complete key remains the object identity across delete/recreate.
// KeyEncoding is always present so binary keys cannot collide with UTF-8 labels.
type Identity struct {
	Key         string `json:"key,omitempty"`
	KeyEncoding string `json:"key_encoding,omitempty"`
	Resource    string `json:"resource"`
	APIGroup    string `json:"api_group,omitempty"`
	Namespace   string `json:"namespace,omitempty"`
	Name        string `json:"name,omitempty"`
	Scope       string `json:"scope"`
	Confidence  string `json:"confidence"`
	Source      string `json:"source"`
}

// Identify recognizes the default /registry layouts, including the built-in
// two-segment service prefixes and DNS-like group/resource prefixes used by
// SimpleStorageFactory (including custom resources). Prefix overrides and
// arbitrary custom KeyFuncs cannot be inferred from WAL alone.
//
// References, pinned to Kubernetes v1.34.0:
//   - pkg/kubeapiserver/default_storage_factory_builder.go
//   - staging/src/k8s.io/apiserver/pkg/server/storage/storage_factory.go
//   - staging/src/k8s.io/apiserver/pkg/server/options/etcd.go (SimpleStorageFactory)
//   - staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go
func Identify(key []byte) Identity {
	id := Identity{Resource: "(unclassified)", Scope: "unknown", Confidence: "unclassified", Source: "raw-key"}
	if !utf8.Valid(key) {
		id.Key, id.KeyEncoding = base64.StdEncoding.EncodeToString(key), "base64"
		return id
	}
	id.Key, id.KeyEncoding = string(key), "utf-8"
	if !strings.HasPrefix(id.Key, "/registry/") {
		return id
	}
	parts := strings.Split(strings.TrimPrefix(id.Key, "/registry/"), "/")
	for _, part := range parts {
		if !pathSegment(part) {
			return id
		}
	}
	if len(parts) < 2 {
		return id
	}
	// RangeAllocation records share the registry root but are allocator state,
	// not ordinary Kubernetes resource instances.
	if parts[0] == "ranges" {
		if len(parts) == 2 {
			id.Resource, id.Name, id.Scope = "ranges", parts[1], "internal"
			id.Confidence, id.Source = "known-storage-prefix", "kubernetes-storage-key"
		}
		return id
	}
	prefixLen := 1
	if parts[0] == "services" && (parts[1] == "specs" || parts[1] == "endpoints") {
		if len(parts) != 4 {
			return id
		}
		prefixLen = 2
	} else if dnsGroup(parts[0]) {
		// Without discovery, a DNS-like leading component plus the supported
		// suffix shape is evidence of the group/resource layout, not proof.
		if len(parts) != 3 && len(parts) != 4 {
			return id
		}
		prefixLen = 2
		id.APIGroup = parts[0]
	}
	if !resourceSegment(parts[prefixLen-1]) {
		id.APIGroup = ""
		return id
	}
	remaining := parts[prefixLen:]
	switch len(remaining) {
	case 1:
		id.Scope, id.Name = "cluster", remaining[0]
	case 2:
		if !dnsLabel(remaining[0]) {
			id.APIGroup = ""
			return id
		}
		id.Scope, id.Namespace, id.Name = "namespaced", remaining[0], remaining[1]
	default:
		id.APIGroup = ""
		return id
	}
	id.Resource = strings.Join(parts[:prefixLen], "/")
	id.Confidence, id.Source = "path-layout", "kubernetes-storage-key"
	return id
}

func pathSegment(s string) bool {
	if s == "" || s == "." || s == ".." || strings.Contains(s, "%") {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func resourceSegment(s string) bool {
	// Default resource names and custom-resource plurals are lowercase DNS
	// labels. A dot belongs to the separate group component, not the resource.
	return dnsLabel(s)
}

func dnsGroup(s string) bool {
	if !strings.Contains(s, ".") || len(s) > 253 {
		return false
	}
	for _, part := range strings.Split(s, ".") {
		if !dnsLabel(part) {
			return false
		}
	}
	return true
}

func dnsLabel(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i, b := range []byte(s) {
		if b >= 'a' && b <= 'z' || b >= '0' && b <= '9' {
			continue
		}
		if b != '-' || i == 0 || i == len(s)-1 {
			return false
		}
	}
	return true
}
