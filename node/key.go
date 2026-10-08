package node

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	irohkey "github.com/tmc/go-iroh/key"
)

const (
	// seedHexLen is the length of a key's seed written in hex.
	seedHexLen = 2 * irohkey.SeedSize
	// maxKeyFile bounds what is read of a key file. A seed with white
	// space around it fits many times over.
	maxKeyFile = 1024
	// keyFileSpace is the white space allowed around the seed.
	keyFileSpace = " \t\r\n"
)

// LoadOrCreateKey reads the key file at path: 64 hex characters and a line
// feed. A missing file is created with a new key, mode 0600, and the
// directories missing on the way to it with mode 0700.
//
// It is the format the key files of transport-iroh, dstore and clamp have.
// Hex in either case is read, and so is white space around it. A file of
// any other shape is an error that names the path and repeats none of the
// contents, and so is a seed of zeros, which is what a placeholder holds
// and no secret. Such a file is left as it is: writing a new key over it
// would silently change the endpoint ID.
//
// Programs that share a path may call it at the same moment: one of them
// creates the file and all of them return its key. The file is written
// under a temporary name and then hard-linked into place, so it has its
// name only once it holds the whole key; this takes a file system with
// hard links.
func LoadOrCreateKey(path string) (irohkey.SecretKey, error) {
	sk, err := readKey(path)
	if !errors.Is(err, fs.ErrNotExist) {
		return sk, err
	}
	if err := createKey(path); err != nil && !errors.Is(err, fs.ErrExist) {
		return irohkey.SecretKey{}, err
	}
	return readKey(path)
}

// readKey reads and parses the key file at path. A missing file is an
// error that wraps fs.ErrNotExist.
func readKey(path string) (irohkey.SecretKey, error) {
	f, err := os.Open(path)
	if err != nil {
		return irohkey.SecretKey{}, fmt.Errorf("read key file: %w", err)
	}
	defer f.Close()
	content, err := io.ReadAll(io.LimitReader(f, maxKeyFile+1))
	if err != nil {
		return irohkey.SecretKey{}, fmt.Errorf("read key file: %w", err)
	}
	sk, err := parseKey(content)
	if err != nil {
		return irohkey.SecretKey{}, fmt.Errorf("key file %s: %w", path, err)
	}
	return sk, nil
}

// parseKey parses the contents of a key file. Its errors say what is wrong
// with the contents and repeat none of them, because they may be most of a
// key.
func parseKey(content []byte) (irohkey.SecretKey, error) {
	if len(content) > maxKeyFile {
		return irohkey.SecretKey{}, fmt.Errorf("more than %d bytes, want %d hex characters", maxKeyFile, seedHexLen)
	}
	text := bytes.Trim(content, keyFileSpace)
	if len(text) != seedHexLen {
		return irohkey.SecretKey{}, fmt.Errorf("%d characters, want %d of hex", len(text), seedHexLen)
	}
	var seed [irohkey.SeedSize]byte
	if _, err := hex.Decode(seed[:], text); err != nil {
		return irohkey.SecretKey{}, errors.New("not hex")
	}
	if seed == [irohkey.SeedSize]byte{} {
		return irohkey.SecretKey{}, errors.New("the seed is all zeros, which is no key")
	}
	return irohkey.NewSecretKey(seed), nil
}

// createKey writes a new key to path. It fails with an error that wraps
// fs.ErrExist when there is a file at path already.
func createKey(path string) error {
	sk, err := irohkey.GenerateSecretKey()
	if err != nil {
		return fmt.Errorf("create key file %s: %w", path, err)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create key file: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".key-*")
	if err != nil {
		return fmt.Errorf("create key file: %w", err)
	}
	defer os.Remove(tmp.Name())

	seed := sk.Bytes()
	_, err = tmp.WriteString(hex.EncodeToString(seed[:]) + "\n")
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("create key file: %w", err)
	}
	if err := os.Link(tmp.Name(), path); err != nil {
		return fmt.Errorf("create key file: %w", err)
	}
	return nil
}
