package replay

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"os"
)

// observationStream keeps only a fixed output buffer and incremental digest.
type observationStream struct {
	file    *os.File
	buffer  *bufio.Writer
	encoder *json.Encoder
	digest  hash.Hash
	closed  bool
}

func newObservationStream(path string) (*observationStream, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	b := bufio.NewWriterSize(io.MultiWriter(f, h), 256*1024)
	return &observationStream{file: f, buffer: b, encoder: json.NewEncoder(b), digest: h}, nil
}
func (s *observationStream) Write(v any) error { return s.encoder.Encode(v) }
func (s *observationStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return errors.Join(s.buffer.Flush(), s.file.Sync(), s.file.Close())
}
func (s *observationStream) Hash() string { return hex.EncodeToString(s.digest.Sum(nil)) }
