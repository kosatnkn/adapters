package mysql_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/kosatnkn/db"
	"github.com/kosatnkn/db/mysql"
)

// NOTE: you will have to create a db named sample and add the following table to it
//
// | sample 					|
// | -------------------------- |
// | id (int, autoincrement)	|
// | name (varchar)				|
// | password (varchar) 		|
//

// newDBAdapter creates a new db adapter pointing to the test db.
func newDBAdapter(t *testing.T) db.AdapterInterface {

	cfg := mysql.Config{
		Host:     "127.0.0.1",
		Port:     3306,
		Database: "sample",
		User:     "root",
		Password: "root",
		PoolSize: 10,
		Check:    true,
	}

	a, err := mysql.NewAdapter(cfg)
	if err != nil {
		t.Fatalf("Cannot create adapter. Error: %v", err)
	}

	return a
}

// TestSelect tests select query.
func TestSelect(t *testing.T) {

	db := newDBAdapter(t)

	q := "select * from sample"

	r, err := db.Query(context.Background(), q, nil)
	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	need := reflect.TypeOf(make([]map[string]interface{}, 0))
	got := reflect.TypeOf(r)

	if got != need {
		t.Errorf("Need %d, got %d", need, got)
	}
}

// TestSelectBulk tests select query sent to QueryBulk()
func TestSelectBulk(t *testing.T) {

	db := newDBAdapter(t)

	q := "select * from sample"

	_, err := db.QueryBulk(context.Background(), q, nil)
	if err == nil {
		t.Errorf("Need error, got nil")
	}

	need := "Select queries are not allowed. Use Query() instead"
	got := err.Error()

	if got != need {
		t.Errorf("Need %s, got %s", need, got)
	}
}
