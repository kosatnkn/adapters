package contex

// Context key type to be used with contexts.
type ctxKey string

// TxKey is the key to attach a database transaction to the context.
const TxKey ctxKey = "tx"
