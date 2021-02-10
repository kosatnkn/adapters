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
	Query(ctx context.Context, query string, parameters map[string]interface{}) ([]map[string]interface{}, error)

	// NewTransaction creates a new database transaction.
	NewTransaction() (*sql.Tx, error)

	// Destruct will close the database adapter releasing all resources.
	Destruct()
}
