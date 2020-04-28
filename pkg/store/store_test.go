package store

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func AssetEqual(t *testing.T, expected, actual interface{}) {
	if expected != actual {
		fmt.Printf("Expected `%t`. Got `%t`\n", expected, actual)
		t.Errorf("Expected `%#v`. Got `%#v`\n", expected, actual)
	}
}

func TestStoreGet(t *testing.T) {
	var ok bool

	tmpDir, _ := ioutil.TempDir("", "test_dir")
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test_store_get")

	s := NewStore(dbPath)
	defer s.Close()

	ok = s.Set([]byte("key"), []byte("val"))
	AssetEqual(t, true, ok)
	ok = s.Set([]byte("key_1"), []byte("val_1"))
	AssetEqual(t, true, ok)
	ok = s.Set([]byte("key_2"), []byte("val_2"))
	AssetEqual(t, true, ok)

	value, ok := s.Get([]byte("key_2"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "val_2", string(value))

	value, ok = s.Get([]byte("key"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "val", string(value))

	value, ok = s.Get([]byte("key_1"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "val_1", string(value))
}

func TestStoreDelete(t *testing.T) {
	var ok bool

	s := NewStore("/tmp/badger_tes")
	defer s.Close()

	ok = s.Set([]byte("key"), []byte("val"))
	AssetEqual(t, true, ok)
	ok = s.Set([]byte("key_1"), []byte("val_1"))
	AssetEqual(t, true, ok)
	ok = s.Set([]byte("key_2"), []byte("val_2"))
	AssetEqual(t, true, ok)

	value, ok := s.Get([]byte("key_2"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "val_2", string(value))

	value, ok = s.Get([]byte("key"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "val", string(value))

	value, ok = s.Get([]byte("key_1"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "val_1", string(value))

	ok = s.Delete([]byte("key_2"))
	AssetEqual(t, true, ok)

	value, ok = s.Get([]byte("key_2"))
	AssetEqual(t, false, ok)

	ok = s.Delete([]byte("key"))
	AssetEqual(t, true, ok)

	value, ok = s.Get([]byte("key"))
	AssetEqual(t, false, ok)

	ok = s.Delete([]byte("key_1"))
	AssetEqual(t, true, ok)

	value, ok = s.Get([]byte("key_1"))
	AssetEqual(t, false, ok)
}
