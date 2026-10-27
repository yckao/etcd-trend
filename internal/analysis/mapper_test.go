package analysis

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestIdentifyStorageLayouts(t *testing.T) {
	for _, tc := range []struct {
		key, resource, group, namespace, name, scope string
	}{
		{"/registry/pods/default/web-1", "pods", "", "default", "web-1", "namespaced"},
		{"/registry/configmaps/kube-system/large", "configmaps", "", "kube-system", "large", "namespaced"},
		{"/registry/namespaces/default", "namespaces", "", "", "default", "cluster"},
		{"/registry/minions/worker.example.com", "minions", "", "", "worker.example.com", "cluster"},
		{"/registry/services/specs/default/web", "services/specs", "", "default", "web", "namespaced"},
		{"/registry/services/endpoints/default/web", "services/endpoints", "", "default", "web", "namespaced"},
		{"/registry/controllers/default/rc", "controllers", "", "default", "rc", "namespaced"},
		{"/registry/ingress/default/public", "ingress", "", "default", "public", "namespaced"},
		{"/registry/deployments/default/web", "deployments", "", "default", "web", "namespaced"},
		{"/registry/clusterroles/system:discovery", "clusterroles", "", "", "system:discovery", "cluster"},
		{"/registry/example.com/widgets/default/blue", "example.com/widgets", "example.com", "default", "blue", "namespaced"},
		{"/registry/example.com/widgets/cluster-blue", "example.com/widgets", "example.com", "", "cluster-blue", "cluster"},
		{"/registry/ranges/serviceips", "ranges", "", "", "serviceips", "internal"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			id := Identify([]byte(tc.key))
			if id.Key != tc.key || id.KeyEncoding != "utf-8" || id.Resource != tc.resource || id.APIGroup != tc.group || id.Namespace != tc.namespace || id.Name != tc.name || id.Scope != tc.scope {
				t.Fatalf("unexpected identity: %+v", id)
			}
			if id.Confidence == "unclassified" || id.Source != "kubernetes-storage-key" {
				t.Fatalf("missing mapping provenance: %+v", id)
			}
		})
	}
}

func TestIdentifyAmbiguousAndMalformedKeys(t *testing.T) {
	for _, key := range []string{
		"/custom/pods/default/p", "/registry", "/registry/", "/registry/pods",
		"/registry//default/p", "/registry/pods//p", "/registry/pods/default/",
		"/registry/pods/default/p/extra", "/registry/services/specs/default",
		"/registry/services/endpoints", "/registry/example.com/widgets",
		"/registry/unknown/multi/prefix/default/p", "/registry/pods/UPPER/p",
		"/registry/pods/default/..", "/registry/pods/default/a%2fb",
		"/registry/pods/default/a\x00b", "/registry/ranges/a/b",
	} {
		id := Identify([]byte(key))
		if id.Key != key || id.Resource != "(unclassified)" || id.Scope != "unknown" || id.Name != "" || id.APIGroup != "" {
			t.Fatalf("misclassified %q: %+v", key, id)
		}
	}
}

func TestIdentifyPreservesBinaryKeys(t *testing.T) {
	key := []byte{'/', 0xff, 0xfe, '\x00'}
	id := Identify(key)
	if id.KeyEncoding != "base64" || id.Resource != "(unclassified)" {
		t.Fatalf("incorrect binary-key identity: %+v", id)
	}
	encoded, err := json.Marshal(id)
	if err != nil {
		t.Fatal(err)
	}
	var roundtrip Identity
	if err = json.Unmarshal(encoded, &roundtrip); err != nil {
		t.Fatal(err)
	}
	raw, err := base64.StdEncoding.DecodeString(roundtrip.Key)
	if err != nil || string(raw) != string(key) {
		t.Fatalf("key did not survive JSON: %q %v", raw, err)
	}
	text := Identify([]byte(id.Key))
	if text.Key != id.Key || text.KeyEncoding == id.KeyEncoding {
		t.Fatal("encoding discriminator must distinguish printable base64 keys")
	}
}
