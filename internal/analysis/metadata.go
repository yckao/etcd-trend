package analysis

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
)

// Metadata contains only the selected Kubernetes attribution fields. It never
// retains an object's spec, status, data, annotations, or managed field sets.
// Revision/VirtualNS identify the last observed value, not an authenticated
// writer. Managers describes the managedFields manager set, not the last actor.
type Metadata struct {
	Status           string            `json:"status"`
	APIVersion       string            `json:"api_version,omitempty"`
	APIGroup         string            `json:"api_group,omitempty"`
	Kind             string            `json:"kind,omitempty"`
	Name             string            `json:"name,omitempty"`
	Namespace        string            `json:"namespace,omitempty"`
	UID              string            `json:"uid,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`
	Controller       *OwnerReference   `json:"controller,omitempty"`
	Managers         []string          `json:"managers,omitempty"`
	Revision         int64             `json:"revision,omitempty"`
	VirtualNS        int64             `json:"virtual_ns,omitempty"`
	IdentityMismatch bool              `json:"identity_mismatch,omitempty"`
}

type OwnerReference struct {
	APIVersion string `json:"api_version"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
}

// NormalizeLabelKeys validates and sorts the extraction allowlist. An empty
// allowlist collects no labels. The same keys apply to Namespace objects.
func NormalizeLabelKeys(keys []string) ([]string, error) {
	unique := map[string]bool{}
	for _, key := range keys {
		key = strings.TrimSpace(key)
		parts := strings.Split(key, "/")
		name := parts[len(parts)-1]
		if len(parts) > 2 || len(parts) == 2 && (!dnsGroup(parts[0]) && !dnsLabel(parts[0])) || len(name) == 0 || len(name) > 63 {
			return nil, fmt.Errorf("invalid label key %q", key)
		}
		for i, c := range name {
			alnum := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
			if !alnum && (i == 0 || i == len(name)-1 || c != '_' && c != '-' && c != '.') {
				return nil, fmt.Errorf("invalid label key %q", key)
			}
		}
		unique[key] = true
	}
	if len(unique) > 64 {
		return nil, fmt.Errorf("at most 64 label keys may be extracted")
	}
	var result []string
	for key := range unique {
		result = append(result, key)
	}
	sort.Strings(result)
	return result, nil
}

// DecodeMetadata recognizes plain Kubernetes JSON and the k8s\x00 protobuf
// runtime.Unknown envelope. Protobuf persisted objects use ObjectMeta field 1.
// Field numbers are defined by Kubernetes apimachinery v0.31.0:
// pkg/runtime/generated.proto; pkg/apis/meta/v1/generated.proto; and the
// Kubernetes API generated.proto schemas. Unknown body fields are skipped.
// The callback's value is borrowed; only selected metadata strings are retained.
func DecodeMetadata(value []byte, labelKeys []string) Metadata {
	if bytes.HasPrefix(value, []byte("k8s:enc:")) {
		return Metadata{Status: "encrypted"}
	}
	var result Metadata
	var err error
	trimmed := bytes.TrimSpace(value)
	switch {
	case bytes.HasPrefix(value, []byte{'k', '8', 's', 0}):
		result, err = decodeProtobufMetadata(value[4:], labelKeys)
	case len(trimmed) > 0 && trimmed[0] == '{':
		if !utf8.Valid(trimmed) {
			return Metadata{Status: "malformed"}
		}
		result, err = decodeJSONMetadata(trimmed, labelKeys)
	default:
		return Metadata{Status: "opaque"}
	}
	if err != nil {
		// No partial field recovery: a malformed value must not become a tenant.
		return Metadata{Status: "malformed"}
	}
	if result.APIVersion == "" || result.Kind == "" || result.Name == "" {
		return Metadata{Status: "opaque"}
	}
	if at := strings.IndexByte(result.APIVersion, '/'); at >= 0 {
		result.APIGroup = result.APIVersion[:at]
	}
	sort.Strings(result.Managers)
	result.Managers = compactStrings(result.Managers)
	return result
}

func decodeJSONMetadata(value []byte, labelKeys []string) (Metadata, error) {
	var object struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Metadata   struct {
			Name, Namespace, UID string
			Labels               map[string]string
			OwnerReferences      []struct {
				APIVersion, Kind, Name, UID string
				Controller                  bool
			}
			ManagedFields []struct{ Manager string }
		}
	}
	if err := json.Unmarshal(value, &object); err != nil {
		return Metadata{}, err
	}
	m := Metadata{Status: "decoded-json", APIVersion: object.APIVersion, Kind: object.Kind, Name: object.Metadata.Name, Namespace: object.Metadata.Namespace, UID: object.Metadata.UID}
	for _, key := range labelKeys {
		if v, ok := object.Metadata.Labels[key]; ok {
			if m.Labels == nil {
				m.Labels = map[string]string{}
			}
			m.Labels[key] = v
		}
	}
	for _, owner := range object.Metadata.OwnerReferences {
		if owner.Controller {
			if m.Controller != nil {
				return Metadata{}, fmt.Errorf("multiple controller references")
			}
			m.Controller = &OwnerReference{APIVersion: owner.APIVersion, Kind: owner.Kind, Name: owner.Name, UID: owner.UID}
		}
	}
	for _, entry := range object.Metadata.ManagedFields {
		if entry.Manager != "" {
			m.Managers = append(m.Managers, entry.Manager)
		}
	}
	return m, nil
}

type protoField struct {
	Number protowire.Number
	Type   protowire.Type
	Bytes  []byte
	Uint   uint64
}

// walkProto visits borrowed fields and skips payloads without decoding spec or
// data. It rejects invalid wire types, truncated fields and invalid tags.
func walkProto(data []byte, visit func(protoField) error) error {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 || num <= 0 {
			return fmt.Errorf("invalid protobuf tag")
		}
		data = data[n:]
		field := protoField{Number: num, Type: typ}
		switch typ {
		case protowire.BytesType:
			field.Bytes, n = protowire.ConsumeBytes(data)
		case protowire.VarintType:
			field.Uint, n = protowire.ConsumeVarint(data)
		case protowire.Fixed32Type:
			_, n = protowire.ConsumeFixed32(data)
		case protowire.Fixed64Type:
			_, n = protowire.ConsumeFixed64(data)
		default:
			return fmt.Errorf("unsupported protobuf wire type")
		}
		if n < 0 {
			return fmt.Errorf("truncated protobuf field")
		}
		if err := visit(field); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func fieldString(f protoField) (string, error) {
	if f.Type != protowire.BytesType || !utf8.Valid(f.Bytes) {
		return "", fmt.Errorf("invalid protobuf string")
	}
	return string(f.Bytes), nil
}
func fieldMessage(f protoField, visit func(protoField) error) error {
	if f.Type != protowire.BytesType {
		return fmt.Errorf("invalid protobuf message")
	}
	return walkProto(f.Bytes, visit)
}

func decodeProtobufMetadata(value []byte, labelKeys []string) (Metadata, error) {
	m := Metadata{Status: "decoded-protobuf"}
	var raw []byte
	var contentEncoding, contentType string
	err := walkProto(value, func(f protoField) (err error) {
		switch f.Number {
		case 1:
			return fieldMessage(f, func(t protoField) (err error) {
				switch t.Number {
				case 1:
					m.APIVersion, err = fieldString(t)
				case 2:
					m.Kind, err = fieldString(t)
				}
				return err
			})
		case 2:
			if f.Type != protowire.BytesType {
				return fmt.Errorf("invalid protobuf body")
			}
			raw = f.Bytes
		case 3:
			contentEncoding, err = fieldString(f)
		case 4:
			contentType, err = fieldString(f)
		}
		return err
	})
	if err != nil {
		return m, err
	}
	if len(raw) == 0 {
		return m, fmt.Errorf("missing protobuf body")
	}
	if contentEncoding != "" || contentType != "" && contentType != "application/vnd.kubernetes.protobuf" {
		return Metadata{Status: "opaque"}, nil
	}
	err = walkProto(raw, func(f protoField) error {
		if f.Number != 1 {
			return nil
		}
		return fieldMessage(f, func(field protoField) (err error) {
			switch field.Number {
			case 1:
				m.Name, err = fieldString(field)
			case 3:
				m.Namespace, err = fieldString(field)
			case 5:
				m.UID, err = fieldString(field)
			case 11:
				var key, value string
				err = fieldMessage(field, func(entry protoField) (err error) {
					if entry.Number == 1 {
						key, err = fieldString(entry)
					} else if entry.Number == 2 {
						value, err = fieldString(entry)
					}
					return err
				})
				for _, wanted := range labelKeys {
					if key == wanted {
						if m.Labels == nil {
							m.Labels = map[string]string{}
						}
						m.Labels[key] = value
					}
				}
			case 13:
				owner, controller, ownerErr := decodeOwner(field)
				if ownerErr != nil {
					return ownerErr
				}
				if controller {
					if m.Controller != nil {
						return fmt.Errorf("multiple controller references")
					}
					m.Controller = &owner
				}
			case 17:
				err = fieldMessage(field, func(entry protoField) error {
					if entry.Number != 1 {
						return nil
					}
					manager, err := fieldString(entry)
					if manager != "" {
						m.Managers = append(m.Managers, manager)
					}
					return err
				})
			}
			return err
		})
	})
	return m, err
}

func decodeOwner(f protoField) (owner OwnerReference, controller bool, err error) {
	err = fieldMessage(f, func(field protoField) (err error) {
		switch field.Number {
		case 1:
			owner.Kind, err = fieldString(field)
		case 3:
			owner.Name, err = fieldString(field)
		case 4:
			owner.UID, err = fieldString(field)
		case 5:
			owner.APIVersion, err = fieldString(field)
		case 6:
			if field.Type != protowire.VarintType {
				return fmt.Errorf("invalid controller boolean")
			}
			controller = field.Uint != 0
		}
		return err
	})
	return
}

func compactStrings(in []string) []string {
	out := in[:0]
	for _, value := range in {
		if len(out) == 0 || out[len(out)-1] != value {
			out = append(out, value)
		}
	}
	return out
}
