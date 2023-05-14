# typeql

A horrible name (blame AI) for a library that uses generics to execute database queries. 

Marshals rows into structs using the `db` tag. For the struct field to be marshalled, it needs to contain the `db` tag.

### Example struct:
```go
type Foo struct {
    ID string `db:"id"` // will be marshalled
    Value int           // won't be marshalled
}
```

### Example usage:
```go
type Foo struct {
    ID    string `db:"id"`
    Value string `db:"value"`
}

const query = "SELECT * FROM foo WHERE value = $1;"

foos, err := tql.Query[Foo](context.Background(), db, query, "bar")
if err != nil { 
    // error handling
}

// do stuff with foos 
```

### Supports named parameters:
```sql
INSERT INTO foo (id) VALUES (:id);
```

### Example usage:
```go
type Foo struct {
    ID    string `db:"id"`
    Value string `db:"value"`
}

const stmt = "INSERT INTO foo (id, value) VALUES (:id, :value);"

foo := Foo {
    ID:    "foo",
    Value: "bar",
}

result, err := tql.Exec(context.Background(), db, stmt, foo)
if err != nil { 
    // error handling
}

// do stuff with result
```

## API
```go
QuerySingle[T any](ctx context.Context, q Querier, query string, params ...any) (result T, err error)
```
QuerySingle queries the database and returns the first result returned. If the query does not return any rows, returns an error. If the query produces more than one result, returns an error.

```go
QuerySingleOrDefault[T any](ctx context.Context, q Querier, def T, query string, params ...any) (result T, err error)
```
QuerySingleOrDefault queries the database and returns the first result returned. If the query does not return any rows, returns the provided default value. If the query produces more than 1 row, returns an error.

```go
QueryFirst[T any](ctx context.Context, q Querier, query string, params ...any) (result T, err error)
```
QueryFirst queries the database and returns the first result.

```go
QueryFirstOrDefault[T any](ctx context.Context, q Querier, def T, query string, params ...any) (result T, err error)
```
QueryFirst queries the database and returns the first result. If there are no rows returned, returns the provided default value.

```go
Query[T any](ctx context.Context, q Querier, query string, params ...any) (result []T, err error)
```
Query queries the database and returns all the returned rows.

```go
Exec(ctx context.Context, e Executor, query string, params ...any) (sql.Result, error) 
```
Exec executes a statement and returns an sql.Result or an error.

## Interfaces used
```go
type Executor interface {
    ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type Querier interface {
    QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
    QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}
```