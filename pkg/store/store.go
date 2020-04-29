package store

import (
	"bytes"
	"encoding/gob"
	"log"

	"github.com/dgraph-io/badger"
	"github.com/google/uuid"
)

type Store struct {
	db *badger.DB
}

type Value struct {
	Value   []byte
	Version uuid.UUID
}

func NewStore(dbPath string) *Store {
	var err error
	s := Store{}

	s.db, err = badger.Open(badger.DefaultOptions(dbPath))
	if err != nil {
		log.Fatal(err)
	}

	return &s
}

func (s *Store) Close() {
	s.db.Close()
}

func (s *Store) MakeValue(value []byte) Value {
	u, _ := uuid.NewUUID()
	return Value{Version: u, Value: value}
}

func (s *Store) Set(key []byte, value Value) bool {
	var valueBuffer bytes.Buffer

	enc := gob.NewEncoder(&valueBuffer)
	err := enc.Encode(value)

	err = s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)

		if err == nil {
			var oldValue Value
			valCopy, err := item.ValueCopy(nil)

			if err != nil {
				return err
			}

			buf := bytes.NewBuffer(valCopy)
			dec := gob.NewDecoder(buf)
			err = dec.Decode(&oldValue)

			if err != nil {
				return err
			}
			if oldValue.Version.Time() < value.Version.Time() {
				err = txn.Set(key, valueBuffer.Bytes())

				if err != nil {
					return err
				}
			}
		} else {
			err = txn.Set(key, valueBuffer.Bytes())

			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return false
	}

	return true
}

func (s *Store) Get(key []byte) (Value, bool) {
	var value Value

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		valCopy, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		buf := bytes.NewBuffer(valCopy)
		dec := gob.NewDecoder(buf)
		err = dec.Decode(&value)

		if err != nil {
			return err
		}

		return err
	})

	if err != nil {
		return value, false
	}

	return value, true
}

func (s *Store) Delete(key []byte, value Value) bool {
	err := s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)

		if err == nil {
			var oldValue Value
			valCopy, err := item.ValueCopy(nil)

			if err != nil {
				return err
			}

			buf := bytes.NewBuffer(valCopy)
			dec := gob.NewDecoder(buf)
			err = dec.Decode(&oldValue)

			if err != nil {
				return err
			}

			if oldValue.Version.Time() < value.Version.Time() {
				err := txn.Delete(key)

				if err != nil {
					return err
				}
			}

		} else {

			err := txn.Delete(key)

			if err != nil {
				return err
			}
		}

		return err
	})

	if err != nil {
		return false
	}

	return true
}
