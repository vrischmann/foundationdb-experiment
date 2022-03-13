package main

import (
	"encoding/binary"
	"errors"
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"golang.org/x/sync/errgroup"
)

func incrementCounter(db fdb.Database, key string) error {
	tx, err := db.CreateTransaction()
	if err != nil {
		log.Fatal(err)
	}

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], 1)

	for {
		tx.Add(fdb.Key(key), buf[:])

		f := tx.Commit()

		err := f.Get()
		if err == nil {
			break
		}

		log.Printf("commit failed, err: %v", err)

		var fdbErr fdb.Error
		if errors.Is(err, &fdbErr) {
			err = tx.OnError(fdbErr).Get()
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	fdb.MustAPIVersion(630)

	db, err := fdb.OpenDefault()
	if err != nil {
		log.Fatal(err)
	}

	var eg errgroup.Group
	for i := 0; i < 10; i++ {
		eg.Go(func() error {
			for i := 0; i < 200000; i++ {
				err := incrementCounter(db, "vincent")
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	err = eg.Wait()
	if err != nil {
		log.Fatal(err)
	}
}
