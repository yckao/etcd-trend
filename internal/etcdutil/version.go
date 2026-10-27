package etcdutil

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// SourceVersion identifies a stable etcd source release accepted by the
// compatibility adapters. The physical simulation still uses its pinned engine.
type SourceVersion struct {
	Version string
	Major   int
	Minor   int
	Patch   int
}

var stableVersion = regexp.MustCompile(`^v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$`)

func ParseSourceVersion(s string) (SourceVersion, error) {
	var v SourceVersion
	m := stableVersion.FindStringSubmatch(s)
	if m == nil {
		return v, fmt.Errorf("source etcd version %q must be a stable 3.4.x, 3.5.x, or 3.6.x release", s)
	}
	parts := []*int{&v.Major, &v.Minor, &v.Patch}
	for i, p := range parts {
		n, err := strconv.Atoi(m[i+1])
		if err != nil {
			return v, fmt.Errorf("invalid version %q: %w", s, err)
		}
		*p = n
	}
	if v.Major != 3 || v.Minor < 4 || v.Minor > 6 {
		return v, fmt.Errorf("source etcd version %q is outside supported stable 3.4.x–3.6.x", s)
	}
	v.Version = strings.TrimPrefix(s, "v")
	return v, nil
}
