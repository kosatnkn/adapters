package db

import (
	"context"
	"database/sql"

	"github.com/kosatnkn/db/internal"
)

// TxAdapter is used to handle postgres db transactions.
type TxAdapter struct {
	dba AdapterInterface
}

// NewTxAdapter creates a new Postgres transaction adapter instance.
func NewTxAdapter(dba AdapterInterface) TxAdapterInterface {

	return &TxAdapter{
		dba: dba,
	}
}

// Wrap runs the content of the function in a single transaction.
func (a *TxAdapter) Wrap(ctx context.Context, fn func(ctx context.Context) (interface{}, error)) (interface{}, error) {

	// attach a transaction to context
	ctx, err := a.attachTx(ctx)
	if err != nil {
		return nil, err
	}

	// get a reference to the attached transaction
	tx := ctx.Value(internal.TxKey).(*sql.Tx)

	// run function
	res, err := fn(ctx)

	// decide whether to commit or rollback
	// NOTE: Here we deliberately avoid catching errors from Commit() and Rollback().
	//		 This is because the sql package does not give a method to check whether
	//		 a transaction has already completed or not.
	//		 When executing nested operations in a single transaction, either the leaf operation or the
	//		 earliest failing operation of the operation tree will close the transaction.
	//		 Since all operations prior to that operation also tries to close the transaction
	//		 it will always result in an error.
	//		 If we catch errors from Commit() and Rollback(), nested transactions
	// 		 will always fail because of this.
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	tx.Commit()

	return res, nil
}

// attachTx attaches a database transaction to the context.
//
// This will first check to see whether there is a transaction already in the context.
// Having a transaction already attached to context probably means that the calling function
// has been wrapped in a transaction in a previous stage.
// When this is the case use the existing attached transaction.
// Otherwise create a new transaction and attach.
func (a *TxAdapter) attachTx(ctx context.Context) (context.Context, error) {

	// check tx altready exists
	tx := ctx.Value(internal.TxKey)
	if tx != nil {
		return ctx, nil
	}

	// attach new tx
	tx, err := a.dba.NewTransaction()
	if err != nil {
		return nil, err
	}

	return context.WithValue(ctx, internal.TxKey, tx), nil
}
