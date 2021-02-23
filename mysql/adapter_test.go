// +build integration
// +build mysql

package mysql_test

import (
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
