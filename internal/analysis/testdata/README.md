# Kubernetes metadata fixtures

`upstream-pod.pb` and `upstream-pod.json` are synthetic Pod objects generated on
2026-10-28 using **k8s.io/api v0.31.0** and **k8s.io/apimachinery v0.31.0**. The
protobuf fixture uses the upstream `serializer/protobuf.Serializer`, including
the `k8s\x00` prefix and `runtime.Unknown` envelope; it is not encoded by this
repository's metadata decoder or a matching hand-built wire encoder.

`generate.go.txt` is the exact generator. `generate.go.mod.txt` records its
isolated module dependencies. Copy these to `main.go` and `go.mod` in a new
temporary directory, run `go mod tidy`, and run the generator with this fixture
directory's absolute path as its only argument. The fixtures are created with
mode 0600. Neither module is a dependency of the application.

The object intentionally contains an unselected label, an annotation and an
environment value. Tests verify that none appear in extracted metadata or the
analysis output. All contents are synthetic.

The wire-field definitions used by the decoder are documented upstream:

- [runtime.Unknown and TypeMeta](https://github.com/kubernetes/apimachinery/blob/v0.31.0/pkg/runtime/generated.proto)
- [ObjectMeta, OwnerReference and ManagedFieldsEntry](https://github.com/kubernetes/apimachinery/blob/v0.31.0/pkg/apis/meta/v1/generated.proto)
- [Pod metadata field](https://github.com/kubernetes/api/blob/v0.31.0/core/v1/generated.proto)
- [Protobuf envelope serializer](https://github.com/kubernetes/apimachinery/blob/v0.31.0/pkg/runtime/serializer/protobuf/protobuf.go)

The decoder extracts common metadata from persisted objects whose protobuf
schema places `ObjectMeta` in field 1. This does not qualify every historical or
custom protobuf schema. Unsupported encodings and non-object values remain
opaque, and encrypted values remain encrypted; no decryption is attempted.
