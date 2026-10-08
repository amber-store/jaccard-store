package node

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	irohkey "github.com/tmc/go-iroh/key"
)

func TestLoadOrCreateKeyCreatesFileAndReadsItBack(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "config", "jaccard-store")
	path := filepath.Join(dir, "client.key")

	created, err := LoadOrCreateKey(path)
	if err != nil {
		t.Fatal(err)
	}
	if created.IsZero() {
		t.Fatal("created key is the zero key")
	}

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	seed := created.Bytes()
	if want := hex.EncodeToString(seed[:]) + "\n"; string(content) != want {
		t.Fatalf("key file is %d bytes, want the seed as 64 hex characters and a line feed", len(content))
	}
	if runtime.GOOS != "windows" {
		if mode := fileMode(t, path); mode != 0o600 {
			t.Fatalf("key file mode is %o, want 600", mode)
		}
		if mode := fileMode(t, dir); mode != 0o700 {
			t.Fatalf("key directory mode is %o, want 700", mode)
		}
	}

	again, err := LoadOrCreateKey(path)
	if err != nil {
		t.Fatal(err)
	}
	if again.Bytes() != created.Bytes() {
		t.Fatal("second load returned another key")
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 {
		t.Fatalf("key directory holds %d entries (error %v), want the key file alone", len(entries), err)
	}
}

func TestLoadOrCreateKeyReadsExistingFile(t *testing.T) {
	want, err := irohkey.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	seed := want.Bytes()
	text := hex.EncodeToString(seed[:])

	for name, content := range map[string]string{
		"canonical":       text + "\n",
		"upper case":      strings.ToUpper(text) + "\n",
		"no line feed":    text,
		"carriage return": text + "\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "server.key")
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			got, err := LoadOrCreateKey(path)
			if err != nil {
				t.Fatal(err)
			}
			if got.Public() != want.Public() {
				t.Fatal("loaded key differs from the one in the file")
			}
		})
	}
}

func TestLoadOrCreateKeyRefusesOtherShapes(t *testing.T) {
	sk, err := irohkey.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	seed := sk.Bytes()
	text := hex.EncodeToString(seed[:])

	for name, content := range map[string]string{
		"empty":            "",
		"line feed only":   "\n",
		"too short":        text[:62] + "\n",
		"too long":         text + "ab\n",
		"not hex":          "zz" + text[2:] + "\n",
		"two lines":        text + "\n" + text + "\n",
		"space inside":     text[:32] + " " + text[32:] + "\n",
		"all zero seed":    strings.Repeat("0", 64) + "\n",
		"far too large":    strings.Repeat(text, 64) + "\n",
		"binary seed":      string(seed[:]),
		"prefixed with 0x": "0x" + text + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "server.key")
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			got, err := LoadOrCreateKey(path)
			if err == nil {
				t.Fatal("no error")
			}
			if !got.IsZero() {
				t.Fatal("a key came back beside the error")
			}
			if !strings.Contains(err.Error(), path) {
				t.Fatalf("error %q does not name the path", err)
			}
			for _, secret := range []string{text, text[:16], text[2:18], text[48:], string(seed[:8])} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("error %q echoes the contents of the file", err)
				}
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(after) != content {
				t.Fatal("the refused file was rewritten")
			}
		})
	}
}

func TestLoadOrCreateKeyConcurrentFirstUse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared", "client.key")

	const loaders = 16
	keys := make([]irohkey.SecretKey, loaders)
	errs := make([]error, loaders)
	var wg sync.WaitGroup
	for i := range loaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			keys[i], errs[i] = LoadOrCreateKey(path)
		}()
	}
	wg.Wait()

	for i := range loaders {
		if errs[i] != nil {
			t.Fatalf("loader %d: %v", i, errs[i])
		}
		if keys[i].Bytes() != keys[0].Bytes() {
			t.Fatalf("loader %d got another key than loader 0", i)
		}
	}
}

func TestLoadOrCreateKeyReportsUnreadablePath(t *testing.T) {
	dir := t.TempDir()
	if _, err := LoadOrCreateKey(dir); err == nil || !strings.Contains(err.Error(), dir) {
		t.Fatalf("a directory in place of the key file: error %v, want one naming the path", err)
	}
}

func fileMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode().Perm()
}
