package main

import (
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func main() {
	fdb.MustAPIVersion(630)

	db, err := fdb.OpenDefault()
	if err != nil {
		log.Fatal(err)
	}
}
