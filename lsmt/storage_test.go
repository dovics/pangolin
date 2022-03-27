package lsmt_test

import (
	"testing"

	"github.com/dovics/db"
	"github.com/google/uuid"
)

func TestMemStorage(t *testing.T) {
	testDB, err := db.OpenDB(db.DefaultOption(uuid.NewString()))
	if err != nil {
		t.Fatal(err)
	}

	for i := int64(0); i < 1000; i++ {
		err := testDB.Insert(i, i)
		if err != nil {
			t.Error(err)
		}
	}
}
