package store

import (
	"log"

	"github.com/dgraph-io/badger"
)

type Store struct {
	db *badger.DB
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

func (s *Store) Set(key, value []byte) bool {
	err := s.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, value)

		if err != nil {
			return err
		}

		return err
	})

	if err != nil {
		return false
	}

	return true
}

func (s *Store) Get(key []byte) ([]byte, bool) {
	var valCopy []byte

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		valCopy, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}

		return err
	})

	if err != nil {
		return valCopy, false
	}

	return valCopy, true
}

func (s *Store) Delete(key []byte) bool {
	err := s.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)

		if err != nil {
			return err
		}

		return err
	})

	if err != nil {
		return false
	}

	return true
}
