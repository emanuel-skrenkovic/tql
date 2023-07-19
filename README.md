# tql

Simple convenience functions (with generics) around `database/sql`.
Designed to be used with existing `sql.DB`, `sql.Tx` types.

Marshals rows into structs using the `db` tag. For the struct field to be marshalled, it needs to contain the `db` tag.

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
```go
type Foo struct {
    ID    string `db:"id"`
    Value string `db:"value"`
}

foo := Foo {
    ID:    "foo",
    Value: "bar",
}

const stmt = "INSERT INTO foo (id, value) VALUES (:id, :value);"

result, err := tql.Exec(context.Background(), db, stmt, foo)
if err != nil { 
    // error handling
}

// do stuff with result
```

## API
```go
QuerySingle[T any](ctx context.Context, q Querier, query string, params ...any) (T, error)

QuerySingleOrDefault[T any](ctx context.Context, q Querier, def T, query string, params ...any) (T, error)

QueryFirst[T any](ctx context.Context, q Querier, query string, params ...any) (T, error)

QueryFirstOrDefault[T any](ctx context.Context, q Querier, def T, query string, params ...any) (T, error)

Query[T any](ctx context.Context, q Querier, query string, params ...any) ([]T, error)

Exec(ctx context.Context, e Executor, query string, params ...any) (sql.Result, error) 
```

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
