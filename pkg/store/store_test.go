package store

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func AssetEqual(t *testing.T, expected, actual interface{}) {
	t.Helper()

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

	ok = s.Set([]byte("key"), Value{Version: uuid.New(), Value: []byte("val")})
	AssetEqual(t, true, ok)
	ok = s.Set([]byte("key_1"), Value{Version: uuid.New(), Value: []byte("val_1")})
	AssetEqual(t, true, ok)
	ok = s.Set([]byte("key_2"), Value{Version: uuid.New(), Value: []byte("val_2")})
	AssetEqual(t, true, ok)

	value, ok := s.Get([]byte("key_2"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "val_2", string(value.Value))

	value, ok = s.Get([]byte("key"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "val", string(value.Value))

	value, ok = s.Get([]byte("key_1"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "val_1", string(value.Value))
}

func TestStoreSetOldVersion(t *testing.T) {
	var ok bool

	tmpDir, _ := ioutil.TempDir("", "test_dir")
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test_store_set_old_version")

	s := NewStore(dbPath)
	defer s.Close()

	ok = s.Set([]byte("key"), s.MakeValue([]byte("val")))
	AssetEqual(t, true, ok)
	ok = s.Set([]byte("key_1"), Value{Version: uuid.MustParse("4cb90188-8978-11ea-8dca-acde48001122"), Value: []byte("val_1")})
	AssetEqual(t, true, ok)

	ok = s.Set([]byte("key_2"), s.MakeValue([]byte("val_2")))
	AssetEqual(t, true, ok)

	value, ok := s.Get([]byte("key"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "val", string(value.Value))

	value, ok = s.Get([]byte("key_1"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "val_1", string(value.Value))

	value, ok = s.Get([]byte("key_2"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "val_2", string(value.Value))

	ok = s.Set([]byte("key"), Value{Version: uuid.MustParse("58c6d91e-8978-11ea-8dca-acde48001122"), Value: []byte("new val but old version")})
	AssetEqual(t, true, ok)
	value, ok = s.Get([]byte("key"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "val", string(value.Value))

	ok = s.Set([]byte("key"), s.MakeValue([]byte("new val and new version")))
	AssetEqual(t, true, ok)
	value, ok = s.Get([]byte("key"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "new val and new version", string(value.Value))

	ok = s.Set([]byte("key_1"), s.MakeValue([]byte("val_1 a new version")))
	AssetEqual(t, true, ok)
	value, ok = s.Get([]byte("key_1"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "val_1 a new version", string(value.Value))

	ok = s.Set([]byte("key_2"), Value{Version: uuid.MustParse("003d4bce-8979-11ea-8dca-acde48001122"), Value: []byte("val_2 TOO old version")})
	AssetEqual(t, true, ok)
	value, ok = s.Get([]byte("key_2"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "val_2", string(value.Value))

	ok = s.Set([]byte("key_2"), s.MakeValue([]byte("val_2 and super new version")))
	AssetEqual(t, true, ok)
	value, ok = s.Get([]byte("key_2"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "val_2 and super new version", string(value.Value))
}

func TestStoreDelete(t *testing.T) {
	var ok bool

	s := NewStore("/tmp/badger_tes")
	defer s.Close()

	ok = s.Set([]byte("key"), s.MakeValue([]byte("val")))
	AssetEqual(t, true, ok)

	ok = s.Set([]byte("key_1"), Value{Version: uuid.MustParse("4cb90188-8978-11ea-8dca-acde48001122"), Value: []byte("val_1")})
	AssetEqual(t, true, ok)

	ok = s.Set([]byte("key_2"), s.MakeValue([]byte("val_2")))
	AssetEqual(t, true, ok)

	value, ok := s.Get([]byte("key_2"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "val_2", string(value.Value))

	value, ok = s.Get([]byte("key"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "val", string(value.Value))

	value, ok = s.Get([]byte("key_1"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "val_1", string(value.Value))

	ok = s.Delete([]byte("key_2"), Value{Version: uuid.MustParse("3fe34cec-8983-11ea-8dca-acde48001122"), Value: []byte{}})
	AssetEqual(t, true, ok)

	value, ok = s.Get([]byte("key_2"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "val_2", string(value.Value))

	ok = s.Delete([]byte("key"), Value{Version: uuid.MustParse("3fe34cec-8983-11ea-8dca-acde48001122"), Value: []byte{}})
	AssetEqual(t, true, ok)

	value, ok = s.Get([]byte("key"))
	AssetEqual(t, true, ok)
	AssetEqual(t, "val", string(value.Value))

	ok = s.Delete([]byte("key_1"), s.MakeValue([]byte{}))
	AssetEqual(t, true, ok)

	value, ok = s.Get([]byte("key_1"))
	AssetEqual(t, false, ok)
}
