package loadtest

import (
	"encoding/json"
	"testing"

	"github.com/yckao/etcd-trend/internal/analysis"
)

func TestKubernetesObjectsHaveBoundedPayloadAndAttributionMetadata(t *testing.T) {
	for phase, resource := range phaseResources {
		key, data, err := workloadObject("tenant-alpha", resource, 7, 12345, phase, 32768, 3)
		if err != nil {
			t.Fatal(err)
		}
		if len(data) != 32768 {
			t.Fatalf("phase %d: payload size %d", phase, len(data))
		}
		storagePrefix := resource
		if resource == "widgets.example.test" {
			storagePrefix = "example.test/widgets"
		}
		if key != "/registry/"+storagePrefix+"/tenant-alpha/workload-007" {
			t.Fatal(key)
		}
		var obj struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
			Metadata   struct {
				Name, Namespace, UID string
				Labels               map[string]string
				ManagedFields        []struct{ Manager string }
				OwnerReferences      []struct {
					Kind, Name, UID string
					Controller      bool
				}
			}
		}
		if err := json.Unmarshal(data, &obj); err != nil {
			t.Fatal(err)
		}
		if obj.APIVersion == "" || obj.Kind == "" || obj.Metadata.Namespace != "tenant-alpha" || obj.Metadata.UID == "" {
			t.Fatalf("incomplete metadata: %+v", obj)
		}
		identity := analysis.Identify([]byte(key))
		decoded := analysis.DecodeMetadata(data, []string{"tenant"})
		if identity.Scope != "namespaced" || identity.Namespace != decoded.Namespace || identity.Name != decoded.Name || identity.Resource != storagePrefix || decoded.Status != "decoded-json" {
			t.Fatalf("generated %s key and metadata disagree: key=%s identity=%+v metadata=%+v", obj.Kind, key, identity, decoded)
		}
		if resource == "widgets.example.test" && (identity.APIGroup != "example.test" || decoded.APIGroup != identity.APIGroup) {
			t.Fatalf("custom-resource group mismatch: identity=%+v metadata=%+v", identity, decoded)
		}
		wantTenant := "alpha"
		if phase == 4 {
			wantTenant = "alpha-migrated"
		}
		if obj.Metadata.Labels["tenant"] != wantTenant || obj.Metadata.Labels["app.kubernetes.io/name"] != "workload" {
			t.Fatalf("labels: %v", obj.Metadata.Labels)
		}
		if len(obj.Metadata.ManagedFields) == 0 || obj.Metadata.ManagedFields[0].Manager != phaseManagers[phase] {
			t.Fatal("manager missing")
		}
		if resource == "pods" {
			if len(obj.Metadata.ManagedFields) != 2 || len(obj.Metadata.OwnerReferences) != 1 || obj.Metadata.OwnerReferences[0].Kind != "ReplicaSet" || !obj.Metadata.OwnerReferences[0].Controller {
				t.Fatal("pod owner/manager metadata missing")
			}
		}
	}
}

func TestEventIncarnationChangesOnlyWhenRequested(t *testing.T) {
	_, a, err := workloadObject("tenant-delta", "events", 3, 1, 3, 4096, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, b, err := workloadObject("tenant-delta", "events", 3, 999, 3, 4096, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, c, err := workloadObject("tenant-delta", "events", 3, 1000, 3, 4096, 1)
	if err != nil {
		t.Fatal(err)
	}
	uid := func(data []byte) string {
		var v struct{ Metadata struct{ UID string } }
		if err := json.Unmarshal(data, &v); err != nil {
			t.Fatal(err)
		}
		return v.Metadata.UID
	}
	if uid(a) != uid(b) || uid(a) == uid(c) {
		t.Fatal("UID must remain stable between recreations")
	}
}
