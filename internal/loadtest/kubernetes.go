package loadtest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Phase describes real elapsed workload time, not a synthetic replay clock.
type Phase struct {
	Number            int    `json:"number"`
	StartNS           int64  `json:"start_ns"`
	DominantNamespace string `json:"dominant_namespace"`
	DominantResource  string `json:"dominant_resource"`
	Manager           string `json:"manager"`
}

var namespaces = []string{"tenant-alpha", "tenant-beta", "tenant-gamma", "tenant-delta"}
var phaseResources = []string{"configmaps", "pods", "widgets.example.test", "events", "configmaps"}
var phaseManagers = []string{"config-sync", "kubelet", "widget-operator", "event-controller", "rollout-controller"}

func labels(ns, app string, phase int) map[string]string {
	tenant := strings.TrimPrefix(ns, "tenant-")
	if phase == 4 && ns == "tenant-alpha" {
		tenant = "alpha-migrated"
	}
	return map[string]string{"tenant": tenant, "team": "team-" + tenant, "app.kubernetes.io/name": app,
		"app.kubernetes.io/part-of": "platform-" + tenant, "app.kubernetes.io/managed-by": "fixture"}
}

func metadata(ns, name, uid, manager, api string, phase int) map[string]any {
	m := map[string]any{"name": name, "uid": uid, "labels": labels(ns, "workload", phase),
		"managedFields": []any{map[string]any{"manager": manager, "operation": "Update", "apiVersion": api, "fieldsType": "FieldsV1", "fieldsV1": map[string]any{}}}}
	if ns != "" {
		m["namespace"] = ns
	}
	return m
}

func owner(ns, kind, name string) []any {
	return []any{map[string]any{"apiVersion": "apps/v1", "kind": kind, "name": name, "uid": ns + "-" + strings.ToLower(kind) + "-" + name, "controller": true, "blockOwnerDeletion": true}}
}

func seedKubernetes(ctx context.Context, c *clientv3.Client, s *Summary) error {
	for _, ns := range namespaces {
		objects := []struct {
			key   string
			value map[string]any
		}{
			{"/registry/namespaces/" + ns, map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": metadata("", ns, ns+"-uid", "tenant-controller", "v1", 0), "status": map[string]any{"phase": "Active"}}},
			{"/registry/deployments/" + ns + "/workload", map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": metadata(ns, "workload", ns+"-deployment-workload", "deployment-controller", "apps/v1", 0), "spec": map[string]any{"replicas": 3, "selector": map[string]any{"matchLabels": map[string]string{"app": "workload"}}, "template": map[string]any{"metadata": map[string]any{"labels": map[string]string{"app": "workload"}}, "spec": map[string]any{"containers": []any{map[string]any{"name": "pause", "image": "registry.k8s.io/pause:3.10"}}}}}}},
			{"/registry/replicasets/" + ns + "/workload-rs", map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "metadata": metadata(ns, "workload-rs", ns+"-replicaset-workload-rs", "replicaset-controller", "apps/v1", 0), "spec": map[string]any{"replicas": 3}}},
			{"/registry/configmaps/" + ns + "/quiet", map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": metadata(ns, "quiet", ns+"-quiet", "bootstrap", "v1", 0), "data": map[string]string{"stable": "unchanged"}}},
		}
		objects[0].value["metadata"].(map[string]any)["labels"] = labels(ns, "namespace", 0)
		objects[2].value["metadata"].(map[string]any)["ownerReferences"] = owner(ns, "Deployment", "workload")
		for _, obj := range objects {
			if _, err := putJSON(ctx, c, s, obj.key, obj.value); err != nil {
				return err
			}
		}
	}
	// These exercise explicit unknown/encrypted classifications without secrets.
	for key, value := range map[string]string{"/registry/secrets/tenant-delta/opaque": "k8s:enc:secretbox:v1:synthetic-key:fixture-ciphertext", "/fixture/non-kubernetes": "synthetic opaque value"} {
		if _, err := c.Put(ctx, key, value); err != nil {
			return err
		}
		s.ValueBytesWritten += int64(len(value))
		s.Updates++
	}
	return nil
}

func putJSON(ctx context.Context, c *clientv3.Client, s *Summary, key string, value any) (int64, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}
	r, err := c.Put(ctx, key, string(b))
	if err != nil {
		return 0, err
	}
	s.Updates++
	s.ValueBytesWritten += int64(len(b))
	return r.Header.Revision, nil
}

// Logical resource names in phase summaries need not equal storage prefixes.
// Custom resources use the group/resource prefix, followed by namespace/name.
func workloadKey(ns, resource string, slot int) string {
	storagePrefix := resource
	if resource == "widgets.example.test" {
		storagePrefix = "example.test/widgets"
	}
	return fmt.Sprintf("/registry/%s/%s/workload-%03d", storagePrefix, ns, slot)
}

func workloadObject(ns, resource string, slot, iteration, phase, size, incarnation int) (string, []byte, error) {
	name := fmt.Sprintf("workload-%03d", slot)
	api, kind := "v1", "ConfigMap"
	switch resource {
	case "pods":
		kind = "Pod"
	case "events":
		kind = "Event"
	case "widgets.example.test":
		api = "example.test/v1"
		kind = "Widget"
	}
	uid := fmt.Sprintf("%s-%s-%03d", ns, kind, slot)
	if resource == "events" {
		uid += fmt.Sprintf("-%d", incarnation)
	}
	m := metadata(ns, name, uid, phaseManagers[phase], api, phase)
	m["resourceVersion"] = fmt.Sprint(iteration + 1)
	m["generation"] = iteration + 1
	obj := map[string]any{"apiVersion": api, "kind": kind, "metadata": m}
	var payload map[string]any
	field := "payload"
	switch resource {
	case "pods":
		m["ownerReferences"] = owner(ns, "ReplicaSet", "workload-rs")
		if slot%7 == 0 {
			m["managedFields"] = append(m["managedFields"].([]any), map[string]any{"manager": "deployment-controller", "operation": "Update", "apiVersion": api, "fieldsType": "FieldsV1", "fieldsV1": map[string]any{}})
		}
		obj["spec"] = map[string]any{"nodeName": fmt.Sprintf("node-%d", slot%6), "serviceAccountName": "workload", "containers": []any{map[string]any{"name": "pause", "image": "registry.k8s.io/pause:3.10"}}}
		payload = map[string]any{"phase": "Running"}
		obj["status"] = payload
		field = "message"
	case "configmaps":
		payload = map[string]any{}
		obj["data"] = payload
	case "widgets.example.test":
		payload = map[string]any{"sequence": iteration}
		obj["spec"] = payload
	case "events":
		obj["involvedObject"] = map[string]any{"apiVersion": "v1", "kind": "Pod", "namespace": ns, "name": name, "uid": uid}
		obj["reason"] = "FixtureUpdated"
		obj["type"] = "Normal"
		obj["count"] = iteration + 1
		payload = obj
		field = "message"
	}
	payload[field] = ""
	b, err := json.Marshal(obj)
	if err != nil {
		return "", nil, err
	}
	if len(b) > size {
		return "", nil, fmt.Errorf("object metadata requires %d bytes, exceeds value size %d", len(b), size)
	}
	payload[field] = strings.Repeat(string(rune('a'+iteration%26)), size-len(b))
	b, err = json.Marshal(obj)
	return workloadKey(ns, resource, slot), b, err
}

func runKubernetes(ctx context.Context, c *clientv3.Client, cfg Config, s *Summary, started time.Time) error {
	if err := seedKubernetes(ctx, c, s); err != nil {
		return err
	}
	phaseFile, err := os.OpenFile(filepath.Join(cfg.Dir, "phases.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer phaseFile.Close()
	enc := json.NewEncoder(phaseFile)
	nextCompact := cfg.CompactionInterval
	lastPhase := -1
	incarnations := map[string]int{}
	for iteration := 0; time.Since(started) < cfg.Duration; iteration++ {
		elapsed := time.Since(started)
		phase := int(elapsed * 5 / cfg.Duration)
		if phase > 4 {
			phase = 4
		}
		if phase != lastPhase {
			p := Phase{phase, int64(elapsed), namespaces[phase%4], phaseResources[phase], phaseManagers[phase]}
			s.Phases = append(s.Phases, p)
			if err := enc.Encode(p); err != nil {
				return err
			}
			_ = phaseFile.Sync()
			fmt.Fprintf(os.Stderr, "workload phase %d/5: %s %s, elapsed %s\n", phase+1, p.DominantNamespace, p.DominantResource, elapsed.Round(time.Second))
			if phase == 4 {
				m := metadata("", namespaces[0], namespaces[0]+"-uid", "tenant-controller", "v1", phase)
				m["labels"] = labels(namespaces[0], "namespace", phase)
				if _, err := putJSON(ctx, c, s, "/registry/namespaces/"+namespaces[0], map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": m}); err != nil {
					return err
				}
			}
			lastPhase = phase
		}
		ns := namespaces[phase%4]
		if iteration%10 == 0 {
			ns = namespaces[(iteration/10)%4]
		}
		resource := phaseResources[phase]
		if iteration%13 == 0 {
			resource = "pods"
		}
		plannedKey := workloadKey(ns, resource, iteration%cfg.Keys)
		key, value, err := workloadObject(ns, resource, iteration%cfg.Keys, iteration, phase, cfg.ValueBytes, incarnations[plannedKey])
		if err != nil {
			return err
		}
		r, err := c.Put(ctx, key, string(value))
		if err != nil {
			return err
		}
		s.Updates++
		s.ValueBytesWritten += int64(len(value))
		if resource == "events" && iteration%37 == 0 {
			if _, err = c.Delete(ctx, key); err != nil {
				return err
			}
			incarnations[key]++
		}
		if time.Since(started) >= nextCompact {
			if _, err = c.Compact(ctx, r.Header.Revision-1, clientv3.WithCompactPhysical()); err != nil {
				return err
			}
			nextCompact += cfg.CompactionInterval
		}
		due := time.Duration(float64(s.ValueBytesWritten) / float64(cfg.BytesPerSecond) * float64(time.Second))
		if delay := due - time.Since(started); delay > 0 {
			t := time.NewTimer(delay)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			}
		}
	}
	return nil
}
