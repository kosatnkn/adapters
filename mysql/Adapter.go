package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	// database driver for mysql
	_ "github.com/go-sql-driver/mysql"

	"github.com/kosatnkn/db"
	"github.com/kosatnkn/db/internal"
)

// Adapter is used to communicate with a MySQL/MariaDB database.
type Adapter struct {
	cfg      Config
	pool     *sql.DB
	pqPrefix string
}

// NewAdapter creates a new MySQL adapter instance.
func NewAdapter(cfg Config) (db.AdapterInterface, error) {

	connString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)

	db, err := sql.Open("mysql", connString)
	if err != nil {
		return nil, err
	}

	// pool configurations
	db.SetMaxOpenConns(cfg.PoolSize)
	//db.SetMaxIdleConns(2)
	//db.SetConnMaxLifetime(time.Hour)

	a := &Adapter{
		cfg:      cfg,
		pool:     db,
		pqPrefix: "?",
	}

	// check whether the db is accessible
	if cfg.Check {
		return a, a.Ping()
	}

	return a, nil
}

// Ping checks wether the database is accessible.
func (a *Adapter) Ping() error {

	return a.pool.Ping()
}

// Query runs a query and returns the result.
func (a *Adapter) Query(ctx context.Context, query string, params map[string]interface{}) ([]map[string]interface{}, error) {

	convertedQuery, placeholders := a.convertQuery(query)

	reorderedParams, err := a.reorderParameters(params, placeholders)
	if err != nil {
		return nil, err
	}

	stmt, err := a.prepareStatement(ctx, convertedQuery)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	// check whether the query is a select statement
	if strings.ToLower(convertedQuery[:1]) == "s" {

		rows, err := stmt.Query(reorderedParams...)
		if err != nil {
			return nil, err
		}

		return a.prepareDataSet(rows)
	}

	result, err := stmt.Exec(reorderedParams...)
	if err != nil {
		return nil, err
	}

	return a.prepareResultSet(result)
}

// QueryBulk runs a query using an array of parameters.
//
// This query is intended to do bulk inserts, updates and deletes. Using this for selects will result in an error.
func (a *Adapter) QueryBulk(ctx context.Context, query string, params []map[string]interface{}) ([]map[string]interface{}, error) {

	convertedQuery, placeholders := a.convertQuery(query)

	// check whether the query is a select statement
	if strings.ToLower(convertedQuery[:6]) == "select" {
		return nil, fmt.Errorf("Select queries are not allowed. Use Query() instead")
	}

	stmt, err := a.prepareStatement(ctx, convertedQuery)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	var lastID int64
	var affRows int64

	for _, pms := range params {

		reorderedParams, err := a.reorderParameters(pms, placeholders)
		if err != nil {
			return nil, err
		}

		result, err := stmt.Exec(reorderedParams...)
		if err != nil {
			return nil, err
		}

		lastID, _ = result.LastInsertId()
		ar, _ := result.RowsAffected()
		affRows += ar
	}

	return a.formatResultSet(lastID, affRows), nil
}

// NewTransaction creates a new database transaction.
func (a *Adapter) NewTransaction() (*sql.Tx, error) {

	return a.pool.Begin()
}

// Destruct will close the MySQL adapter releasing all resources.
func (a *Adapter) Destruct() error {

	return a.pool.Close()
}

// convertQuery converts the named parameter query to a placeholder query that MySQL library understands.
//
// This will return the query and a slice of strings containing named parameter name in the order that they are found
// in the query.
func (a *Adapter) convertQuery(query string) (string, []string) {

	query = strings.TrimSpace(query)
	exp := regexp.MustCompile(`\` + a.pqPrefix + `\w+`)

	namedParams := exp.FindAllString(query, -1)

	for i := 0; i < len(namedParams); i++ {
		namedParams[i] = strings.TrimPrefix(namedParams[i], a.pqPrefix)
	}

	query = exp.ReplaceAllString(query, "?")

	return query, namedParams
}

// reorderParameters reorders the parameters map in the order of named parameters slice.
func (a *Adapter) reorderParameters(params map[string]interface{}, namedParams []string) ([]interface{}, error) {

	var reorderedParams []interface{}

	for _, param := range namedParams {

		// return an error if a named parameter is missing from params
		paramValue, isParamExist := params[param]

		if !isParamExist {
			return nil, fmt.Errorf("parameter '%s' is missing", param)
		}

		reorderedParams = append(reorderedParams, paramValue)
	}

	return reorderedParams, nil
}

// prepareStatement creates a prepared statement using the query.
//
// Checks whether there is a transaction attached to the context.
// If so use that transaction to prepare statement else use the pool.
func (a *Adapter) prepareStatement(ctx context.Context, query string) (*sql.Stmt, error) {

	tx := ctx.Value(internal.TxKey)
	if tx != nil {
		return tx.(*sql.Tx).Prepare(query)
	}

	return a.pool.Prepare(query)
}

// prepareDataSet creates a dataset using the output of a SELECT statement.
//
// Source: https://kylewbanks.com/blog/query-result-to-map-in-golang
func (a *Adapter) prepareDataSet(rows *sql.Rows) ([]map[string]interface{}, error) {

	defer rows.Close()

	var data []map[string]interface{}
	cols, _ := rows.Columns()

	// create a slice of interface{}'s to represent each column
	// and a second slice to contain pointers to each item in the columns slice
	columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))

	for i := range columns {
		columnPointers[i] = &columns[i]
	}

	for rows.Next() {
		// scan the result into the column pointers
		err := rows.Scan(columnPointers...)
		if err != nil {
			return nil, err
		}

		// create our map, and retrieve the value for each column from the pointers slice
		// storing it in the map with the name of the column as the key
		row := make(map[string]interface{})

		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			row[colName] = *val
		}

		data = append(data, row)
	}

	return data, nil
}

// prepareResultSet creates a resultset using the result of Exec()
func (a *Adapter) prepareResultSet(result sql.Result) ([]map[string]interface{}, error) {

	id, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	aff, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}

	return a.formatResultSet(id, aff), nil
}

// formatResultSet creates a resultset using last insert id and affected rows.
func (a *Adapter) formatResultSet(id, aff int64) []map[string]interface{} {

	data := make([]map[string]interface{}, 0)
	row := make(map[string]interface{})

	row["affected_rows"] = aff
	row["last_insert_id"] = id

	return append(data, row)
}
