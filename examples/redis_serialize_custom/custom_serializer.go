package main

import (
	"bytes"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/vmihailenco/msgpack/v5"
)

var (
	ErrInvalidKeyLength    = errors.New("invalid key length, must be 16, 24, or 32 bytes")
	ErrDecryptionFailed    = errors.New("decryption failed")
	ErrDecompressionFailed = errors.New("decompression failed")
)

type CompressionAlgorithm string

const (
	CompressionNone CompressionAlgorithm = "none"
	CompressionGzip CompressionAlgorithm = "gzip"
)

type EncryptionAlgorithm string

const (
	EncryptionNone EncryptionAlgorithm = "none"
	EncryptionAES  EncryptionAlgorithm = "aes"
)

type BaseSerializer string

const (
	BaseJSON        BaseSerializer = "json"
	BaseMessagePack BaseSerializer = "msgpack"
)

type CustomSerializeOptions struct {
	Compression      CompressionAlgorithm
	Encryption       EncryptionAlgorithm
	EncryptionKey    []byte
	BaseSerializer   BaseSerializer
	CompressionLevel int
}

type CustomSerialize struct {
	options *CustomSerializeOptions
}

func NewCustomSerialize(opts *CustomSerializeOptions) (*CustomSerialize, error) {
	if opts == nil {
		opts = &CustomSerializeOptions{
			Compression:    CompressionNone,
			Encryption:     EncryptionNone,
			BaseSerializer: BaseJSON,
		}
	}

	if opts.Encryption == EncryptionAES {
		if len(opts.EncryptionKey) != 16 && len(opts.EncryptionKey) != 24 && len(opts.EncryptionKey) != 32 {
			return nil, ErrInvalidKeyLength
		}
	}

	if opts.CompressionLevel == 0 {
		opts.CompressionLevel = gzip.DefaultCompression
	}

	return &CustomSerialize{options: opts}, nil
}

func (s *CustomSerialize) Marshal(v interface{}) ([]byte, error) {
	var data []byte
	var err error

	data, err = s.baseMarshal(v)
	if err != nil {
		return nil, err
	}

	if s.options.Compression != CompressionNone {
		data, err = s.compress(data)
		if err != nil {
			return nil, err
		}
	}

	if s.options.Encryption != EncryptionNone {
		data, err = s.encrypt(data)
		if err != nil {
			return nil, err
		}
	}

	return data, nil
}

func (s *CustomSerialize) Unmarshal(data []byte, v interface{}) error {
	var err error

	if s.options.Encryption != EncryptionNone {
		data, err = s.decrypt(data)
		if err != nil {
			return err
		}
	}

	if s.options.Compression != CompressionNone {
		data, err = s.decompress(data)
		if err != nil {
			return err
		}
	}

	return s.baseUnmarshal(data, v)
}

func (s *CustomSerialize) ContentType() string {
	contentType := "application/custom"

	switch s.options.BaseSerializer {
	case BaseMessagePack:
		contentType += "-msgpack"
	case BaseJSON:
		contentType += "-json"
	}

	if s.options.Compression != CompressionNone {
		contentType += "+gzip"
	}

	if s.options.Encryption != EncryptionNone {
		contentType += "+encrypted"
	}

	return contentType
}

func (s *CustomSerialize) baseMarshal(v interface{}) ([]byte, error) {
	switch s.options.BaseSerializer {
	case BaseMessagePack:
		return msgpack.Marshal(v)
	case BaseJSON:
		fallthrough
	default:
		return json.Marshal(v)
	}
}

func (s *CustomSerialize) baseUnmarshal(data []byte, v interface{}) error {
	switch s.options.BaseSerializer {
	case BaseMessagePack:
		return msgpack.Unmarshal(data, v)
	case BaseJSON:
		fallthrough
	default:
		return json.Unmarshal(data, v)
	}
}

func (s *CustomSerialize) compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, s.options.CompressionLevel)
	if err != nil {
		return nil, err
	}

	if _, err := w.Write(data); err != nil {
		w.Close()
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (s *CustomSerialize) decompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, ErrDecompressionFailed
	}
	defer r.Close()

	result, err := io.ReadAll(r)
	if err != nil {
		return nil, ErrDecompressionFailed
	}

	return result, nil
}

func (s *CustomSerialize) encrypt(data []byte) ([]byte, error) {
	block, err := aes.NewCipher(s.options.EncryptionKey)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	encrypted := gcm.Seal(nil, nonce, data, nil)

	result := append(nonce, encrypted...)

	return result, nil
}

func (s *CustomSerialize) decrypt(data []byte) ([]byte, error) {
	block, err := aes.NewCipher(s.options.EncryptionKey)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return nil, ErrDecryptionFailed
	}

	nonce := data[:nonceSize]
	encrypted := data[nonceSize:]

	decrypted, err := gcm.Open(nil, nonce, encrypted, nil)
	if err != nil {
		return nil, ErrDecryptionFailed
	}

	return decrypted, nil
}

func (s *CustomSerialize) DescribePipeline() string {
	var steps []string

	steps = append(steps, fmt.Sprintf("Base: %s", s.options.BaseSerializer))

	if s.options.Compression != CompressionNone {
		steps = append(steps, fmt.Sprintf("Compression: %s (level %d)",
			s.options.Compression, s.options.CompressionLevel))
	}

	if s.options.Encryption != EncryptionNone {
		steps = append(steps, fmt.Sprintf("Encryption: %s", s.options.Encryption))
	}

	return "Pipeline: " + strings.Join(steps, " → ")
}
