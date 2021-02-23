// +build integration
// +build mysql

package db_test

import (
	"context"
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

// newTxAdapter creates a new transaction adapter.
func newTxAdapter(a db.AdapterInterface) db.TxAdapterInterface {

	return db.NewTxAdapter(a)
}

// clearTestTable clears all data from the test table.
func clearTestTable(t *testing.T) {

	adapter := newDBAdapter(t)
	defer adapter.Destruct()

	adapter.Query(context.Background(), `truncate sample`, nil)

	t.Log("Table truncated")
}

// TestSingleTxSuccess tests for successfull operation of executing multiple queries
// using the same transaction.
func TestSingleTxSuccess(t *testing.T) {

	clearTestTable(t)

	adapter := newDBAdapter(t)
	defer adapter.Destruct()

	tx := newTxAdapter(adapter)

	q1 := `insert into sample(name, password) values ('Success Data 1', 'pwd1')`
	q2 := `insert into sample(name, password) values ('Success Data 2', 'pwd2')`
	q3 := `insert into sample(name, password) values ('Success Data 3', 'pwd3')`

	r, err := tx.Wrap(context.Background(), func(ctx context.Context) (interface{}, error) {

		r, err := adapter.Query(ctx, q1, nil)
		if err != nil {
			return nil, err
		}

		r, err = adapter.Query(ctx, q2, nil)
		if err != nil {
			return nil, err
		}

		r, err = adapter.Query(ctx, q3, nil)
		if err != nil {
			return nil, err
		}

		return r, err
	})
	if err != nil {
		t.Error("Error running query")
	}

	result, ok := r.([]map[string]interface{})
	if !ok {
		t.Fatal("Result type mismatch")
	}

	need := 3
	got := int(result[0]["last_insert_id"].(int64))

	if got != need {
		t.Errorf("Need %d, got %d", need, got)
	}
}

// TestSingleTxFail tests for rolling back of the transaction when one query of the
// list fails.
func TestSingleTxFail(t *testing.T) {

	clearTestTable(t)

	adapter := newDBAdapter(t)
	defer adapter.Destruct()

	tx := newTxAdapter(adapter)

	q1 := `insert into sample(name, password) values ('Success Query 1', 'pwd1')`
	q2 := `insert into non_existant_table(name, password) values ('Data to non existant table', 'pwd')`
	q3 := `insert into sample(name, password) values ('Success Query 3', 'pwd3')`

	_, err := tx.Wrap(context.Background(), func(ctx context.Context) (interface{}, error) {

		r, err := adapter.Query(ctx, q1, nil)
		if err != nil {
			return nil, err
		}

		r, err = adapter.Query(ctx, q2, nil)
		if err != nil {
			return nil, err
		}

		r, err = adapter.Query(ctx, q3, nil)
		if err != nil {
			return nil, err
		}

		return r, err
	})
	if err == nil {
		t.Errorf("Need error, got nil")
	}

	need := `Error 1146: Table 'sample.non_existant_table' doesn't exist`
	got := err.Error()
	if need != got {
		t.Errorf("Need %s, got %s", need, got)
	}
}

// TestMultipleTxSuccess tests for successfull execution of multiple transactions.
func TestMultipleTxSuccess(t *testing.T) {

	clearTestTable(t)

	adapter := newDBAdapter(t)
	defer adapter.Destruct()

	tx := newTxAdapter(adapter)

	ctx := context.Background()

	q1 := `insert into sample(name, password) values ('Success Data 1', 'pwd1')`
	q2 := `insert into sample(name, password) values ('Success Data 2', 'pwd2')`

	// run q1
	r, err := tx.Wrap(ctx, func(ctx context.Context) (interface{}, error) {

		r, err := adapter.Query(ctx, q1, nil)
		if err != nil {
			return nil, err
		}

		return r, err
	})
	if err != nil {
		t.Error("Error running query 1")
	}

	result, ok := r.([]map[string]interface{})
	if !ok {
		t.Fatal("Result type mismatch")
	}

	need := 1
	got := int(result[0]["last_insert_id"].(int64))

	if got != need {
		t.Errorf("Need %d, got %d", need, got)
	}

	// run q2
	r, err = tx.Wrap(ctx, func(ctx context.Context) (interface{}, error) {

		r, err = adapter.Query(ctx, q2, nil)
		if err != nil {
			return nil, err
		}

		return r, err
	})
	if err != nil {
		t.Error("Error running query 2")
	}

	result, ok = r.([]map[string]interface{})
	if !ok {
		t.Fatal("Result type mismatch")
	}

	need = 2
	got = int(result[0]["last_insert_id"].(int64))

	if got != need {
		t.Errorf("Need %d, got %d", need, got)
	}

	// check whether all data is inserted
	r, err = adapter.Query(context.Background(), `select count(*) as count from sample`, nil)
	result, ok = r.([]map[string]interface{})
	if !ok {
		t.Fatal("Result type mismatch")
	}

	need = 2
	got = int(result[0]["count"].(int64))

	if got != need {
		t.Errorf("Need %d, got %d", need, got)
	}
}

// TestMultipleTxFail tests for multiple transactions in which one of them fails.
func TestMultipleTxFail(t *testing.T) {

	clearTestTable(t)

	adapter := newDBAdapter(t)
	defer adapter.Destruct()

	tx := newTxAdapter(adapter)

	ctx := context.Background()

	q1 := `insert into sample(name, password) values (no quotes around this string, 'pwd')` // failing query
	q2 := `insert into sample(name, password) values ('Success Data 2', 'pwd2')`

	// run q1 (failing query)
	r, err := tx.Wrap(ctx, func(ctx context.Context) (interface{}, error) {

		r, err := adapter.Query(ctx, q1, nil)
		if err != nil {
			return nil, err
		}

		return r, err
	})
	if err == nil {
		t.Errorf("Need error, got nil")
	}

	errNeed := `Error 1064`
	errGot := err.Error()[:10]
	if errNeed != errGot {
		t.Errorf("Need %s, got %s", errNeed, errGot)
	}

	// run q2
	r, err = tx.Wrap(ctx, func(ctx context.Context) (interface{}, error) {

		r, err = adapter.Query(ctx, q2, nil)
		if err != nil {
			return nil, err
		}

		return r, err
	})
	if err != nil {
		t.Error("Error running query 2")
	}

	result, ok := r.([]map[string]interface{})
	if !ok {
		t.Fatal("Result type mismatch")
	}

	need := 1
	got := int(result[0]["last_insert_id"].(int64))

	if got != need {
		t.Errorf("Need %d, got %d", need, got)
	}

	// check whether all data is inserted
	r, err = adapter.Query(context.Background(), `select count(*) as count from sample`, nil)
	result, ok = r.([]map[string]interface{})
	if !ok {
		t.Fatal("Result type mismatch")
	}

	need = 1
	got = int(result[0]["count"].(int64))

	if got != need {
		t.Errorf("Need %d, got %d", need, got)
	}
}

// TestNestedTxSuccess tests for successful execution of nested transactions.
func TestNestedTxSuccess(t *testing.T) {

	clearTestTable(t)

	adapter := newDBAdapter(t)
	defer adapter.Destruct()

	tx := newTxAdapter(adapter)

	ctx := context.Background()

	q1 := `insert into sample(name, password) values ('Success Data 1', 'pwd1')`
	q2 := `insert into sample(name, password) values ('Success Data 2', 'pwd2')`

	// run q1
	r, err := tx.Wrap(ctx, func(ctx context.Context) (interface{}, error) {

		r1, err1 := adapter.Query(ctx, q1, nil)
		if err1 != nil {
			return nil, err1
		}

		// run q2
		r2, err2 := tx.Wrap(ctx, func(ctx context.Context) (interface{}, error) {

			r2, err2 := adapter.Query(ctx, q2, nil)
			if err2 != nil {
				return nil, err2
			}

			return r2, err2
		})
		if err2 != nil {
			t.Error("Error running query 2")
		}

		result2, ok2 := r2.([]map[string]interface{})
		if !ok2 {
			t.Fatal("Result type mismatch")
		}

		need2 := 2
		got2 := int(result2[0]["last_insert_id"].(int64))

		if got2 != need2 {
			t.Errorf("Need %d, got %d", need2, got2)
		}

		// HERE: return results of q1
		return r1, err1
	})
	if err != nil {
		t.Errorf("Error running query 1: %s", err.Error())
	}

	result, ok := r.([]map[string]interface{})
	if !ok {
		t.Fatal("Result type mismatch")
	}

	need := 1
	got := int(result[0]["last_insert_id"].(int64))

	if got != need {
		t.Errorf("Need %d, got %d", need, got)
	}

	// check whether all data is inserted
	r, err = adapter.Query(context.Background(), `select count(*) as count from sample`, nil)
	result, ok = r.([]map[string]interface{})
	if !ok {
		t.Fatal("Result type mismatch")
	}

	need = 2
	got = int(result[0]["count"].(int64))

	if got != need {
		t.Errorf("Need %d, got %d", need, got)
	}
}

// TestNestedTxInnerFail tests for the failure of inner operation of the nested transactions.
func TestNestedTxInnerFail(t *testing.T) {

	clearTestTable(t)

	adapter := newDBAdapter(t)
	defer adapter.Destruct()

	tx := newTxAdapter(adapter)

	ctx := context.Background()

	q1 := `insert into sample(name, password) values ('Success Data 1', 'pwd1')`
	q2 := `insert into sample(name, password) values (no quotes around this string, 'pwd')` // failing query

	// run q1
	r, err := tx.Wrap(ctx, func(ctx context.Context) (interface{}, error) {

		r1, err1 := adapter.Query(ctx, q1, nil)
		if err1 != nil {
			return nil, err1
		}

		// run q2
		_, err2 := tx.Wrap(ctx, func(ctx context.Context) (interface{}, error) {

			r2, err2 := adapter.Query(ctx, q2, nil)
			if err2 != nil {
				return nil, err2
			}

			return r2, err2
		})
		if err2 == nil {
			t.Errorf("Need error, got nil")
		}

		errNeed := `Error 1064`
		errGot := err2.Error()[:10]
		if errNeed != errGot {
			t.Errorf("Need %s, got %s", errNeed, errGot)
		}

		// HERE: return results of q1
		return r1, err1
	})
	if err != nil {
		t.Errorf("Error running query 1: %s", err.Error())
	}

	result, ok := r.([]map[string]interface{})
	if !ok {
		t.Fatal("Result type mismatch")
	}

	need := 1
	got := int(result[0]["last_insert_id"].(int64))

	if got != need {
		t.Errorf("Need %d, got %d", need, got)
	}

	// check whether all data is inserted
	r, err = adapter.Query(context.Background(), `select count(*) as count from sample`, nil)
	result, ok = r.([]map[string]interface{})
	if !ok {
		t.Fatal("Result type mismatch")
	}

	need = 0
	got = int(result[0]["count"].(int64))

	if got != need {
		t.Errorf("Need %d, got %d", need, got)
	}
}

// TestNestedTxOuterFail tests for the failure of outer operation of the nested transactions.
func TestNestedTxOuterFail(t *testing.T) {

	clearTestTable(t)

	adapter := newDBAdapter(t)
	defer adapter.Destruct()

	tx := newTxAdapter(adapter)

	ctx := context.Background()

	q1 := `insert into sample(name, password) values (no quotes around this string, 'pwd')` // failing query
	q2 := `insert into sample(name, password) values ('Success Data 2', 'pwd2')`

	// run q1
	r, err := tx.Wrap(ctx, func(ctx context.Context) (interface{}, error) {

		r1, err1 := adapter.Query(ctx, q1, nil)
		if err1 != nil {
			return nil, err1
		}

		// run q2
		r2, err2 := tx.Wrap(ctx, func(ctx context.Context) (interface{}, error) {

			r2, err2 := adapter.Query(ctx, q2, nil)
			if err2 != nil {
				return nil, err2
			}

			return r2, err2
		})
		if err2 != nil {
			t.Error("Error running query 2")
		}

		result2, ok2 := r2.([]map[string]interface{})
		if !ok2 {
			t.Fatal("Result type mismatch")
		}

		need2 := 2
		got2 := int(result2[0]["last_insert_id"].(int64))

		if got2 != need2 {
			t.Errorf("Need %d, got %d", need2, got2)
		}

		// HERE: return results of q1
		return r1, err1
	})
	if err == nil {
		t.Errorf("Need error, got nil")
	}

	errNeed := `Error 1064`
	errGot := err.Error()[:10]
	if errNeed != errGot {
		t.Errorf("Need %s, got %s", errNeed, errGot)
	}

	// check whether all data is inserted
	r, err = adapter.Query(context.Background(), `select count(*) as count from sample`, nil)
	result, ok := r.([]map[string]interface{})
	if !ok {
		t.Fatal("Result type mismatch")
	}

	need := 0
	got := int(result[0]["count"].(int64))

	if got != need {
		t.Errorf("Need %d, got %d", need, got)
	}
}
