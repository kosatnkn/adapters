package db

import (
	"context"
	"database/sql"
)

// AdapterInterface is implemented by all database adapters.
type AdapterInterface interface {

	// Ping checks wether the database is accessible.
	Ping() error

	// Query runs a query and return the result.
	Query(ctx context.Context, query string, params map[string]interface{}) ([]map[string]interface{}, error)

	// QueryBulk runs a query using an array of parameters.
	//
	// This query is intended to do bulk inserts, updates and deletes. Using this for selects will result in an error.
	QueryBulk(ctx context.Context, query string, params []map[string]interface{}) ([]map[string]interface{}, error)

	// NewTransaction creates a new database transaction.
	NewTransaction() (*sql.Tx, error)

	// Destruct will close the database adapter releasing all resources.
	Destruct() error
}
