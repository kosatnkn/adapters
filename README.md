# Database Adapters

A collection of database and database transaction adapters.

**Database**
- MySQL Adapter
- Postgres Adapter

**Transaction**
- Transaction Adapter (can be used with all database adapters)

## Test

### Unit Tests
Use following command to run all unit tests
```bash
go test -v -tags=unit ./...
```

### Integration Tests
Use following command to run integration tests

**MySQL**
```bash
go test -v -tags=integration,mysql ./...
```

**Postgres**
```bash
go test -v -tags=integration,postgres ./...
```
