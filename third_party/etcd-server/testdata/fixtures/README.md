# Public upstream test fixtures

These six certificate/key files are copied unchanged from the public etcd
v3.6.0 source commit `f5d605a93abe2fdfa13880aab0f1f3a4203628dd`, under
[`tests/fixtures`](https://github.com/etcd-io/etcd/tree/f5d605a93abe2fdfa13880aab0f1f3a4203628dd/tests/fixtures).
They are intentionally public, insecure test credentials for the preserved
upstream JWT tests. They are not application credentials and must not be used
for a deployed service.

`provenance.json` records each original URL, upstream Git blob SHA1 and SHA256.
The files retain the upstream Apache 2.0 licensing documented in
`../../LICENSE`. Tests reference this directory using package-relative paths,
so they also work from a downloaded Go module outside a Git checkout. Runtime
binaries do not embed these fixtures, and the final container image copies only
the compiled binary and license material.
