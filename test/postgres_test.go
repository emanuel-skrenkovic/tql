package main

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/eskrenkovic/tql"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func Test_Postgresql_QueryOne(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("postgres"))

	id := uuid.New()
	nullable := uuid.New()

	_, err := pgDB.Exec(fmt.Sprintf("INSERT INTO test VALUES ('%s', '%s');", id.String(), nullable.String()))
	require.NoError(t, err)

	// Act
	r, err := tql.QueryFirst[result](context.Background(), pgDB, "SELECT id, nullable FROM test;")

	// Assert
	require.NoError(t, err)
	require.Equal(t, id.String(), r.ID)
	require.NotNil(t, r.Nullable)
	require.Equal(t, nullable.String(), *r.Nullable)
}

func Test_Postgresql_QueryOne_String(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("postgres"))

	id := uuid.New()
	nullable := uuid.New()

	_, err := pgDB.Exec(fmt.Sprintf("INSERT INTO test (id, nullable) VALUES ('%s', '%s');", id.String(), nullable.String()))
	require.NoError(t, err)

	// Act
	r, err := tql.QueryFirst[string](context.Background(), pgDB, "SELECT id FROM test WHERE id = $1;", id)

	// Assert
	require.NoError(t, err)
	require.Equal(t, id.String(), r)
}

func Test_Postgresql_QueryOne_String_Pointer(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("postgres"))

	id := uuid.New()
	nullable := uuid.New()

	_, err := pgDB.Exec(fmt.Sprintf("INSERT INTO test (id, nullable) VALUES ('%s', '%s');", id.String(), nullable.String()))
	require.NoError(t, err)

	// Act
	r, err := tql.QueryFirst[*string](context.Background(), pgDB, "SELECT id FROM test WHERE id = $1;", id)

	// Assert
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Equal(t, id.String(), *r)
}

func Test_Postgresql_QueryOne_Int_Pointer(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("postgres"))

	id := uuid.New()
	nullable := uuid.New()

	_, err := pgDB.Exec(fmt.Sprintf("INSERT INTO test (id, nullable) VALUES ('%s', '%s');", id.String(), nullable.String()))
	require.NoError(t, err)

	// Act
	r, err := tql.QueryFirst[*int](context.Background(), pgDB, "SELECT 420;")

	// Assert
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Equal(t, 420, *r)
}

func Test_Postgresql_Query(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("postgres"))
	_, err := pgDB.Exec("INSERT INTO test (id, nullable) VALUES ('asdf', 'fdsa');")
	require.NoError(t, err)

	// Act
	r, err := tql.Query[result](context.Background(), pgDB, "SELECT id, nullable FROM test;")

	// Assert
	require.NoError(t, err)
	require.Len(t, r, 5)
	require.Equal(t, "asdf", r[4].ID)
	require.NotNil(t, r[4].Nullable)
	require.Equal(t, "fdsa", *r[4].Nullable)
}

func Test_Postgresql_Query_Basic_Type(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("postgres"))
	tx, _ := pgDB.BeginTx(context.Background(), &sql.TxOptions{})

	// Act
	r, err := tql.Query[string](context.Background(), tx, "SELECT id FROM test;")

	require.NoError(t, tx.Commit())

	// Assert
	require.NoError(t, err)
	require.Len(t, r, 5)
	require.Equal(t, "asdf", r[4])
	require.NotNil(t, r[4])
}

func Test_Postgresql_Query_Basic_Type_Pointer(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("postgres"))

	// Act
	r, err := tql.Query[*string](context.Background(), pgDB, "SELECT id FROM test;")

	// Assert
	require.NoError(t, err)
	require.Len(t, r, 5)
	require.Equal(t, "asdf", *r[4])
	require.NotNil(t, r[4])
}

func Test_Postgresql_Query_Basic_Type_Pointer_Null(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("postgres"))

	// Act
	r, err := tql.QueryFirst[*string](context.Background(), pgDB, "SELECT NULL;")

	// Assert
	require.NoError(t, err)
	require.Nil(t, r)
}

func Test_Postgresql_Query_Empty_Result(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("postgres"))

	_, err := pgDB.Exec("INSERT INTO test VALUES ('asdf', 'fdsa');")
	require.NoError(t, err)

	// Act
	r, err := tql.Query[result](context.Background(), pgDB, "SELECT * FROM test WHERE id = '';")

	// Assert
	require.NoError(t, err)
	require.NotNil(t, r)
}

func Test_Postgresql_Exec(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("postgres"))

	// Act
	const insertStmt = "INSERT INTO test (id, nullable) VALUES (:test, :test2);"
	_, err := tql.Exec(context.Background(), pgDB, insertStmt, map[string]any{
		"test":  "totally_new_id",
		"test2": "totally_new_id_2",
	})

	// Assert
	require.NoError(t, err)
	r, err := tql.QueryFirst[result](context.Background(), pgDB, "SELECT * FROM test WHERE id = $1;", "totally_new_id")

	require.NotEmpty(t, r)
	require.Equal(t, "totally_new_id", r.ID)
	require.Equal(t, "totally_new_id_2", *r.Nullable)
	require.NoError(t, err)
	require.NoError(t, err)
}

func Test_Postgresql_Exec_With_Struct(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("postgres"))

	// Act
	id := uuid.NewString()
	userID := uuid.NewString()
	params := struct {
		ID     string `db:"test"`
		UserID string `db:"test2"`
	}{
		ID:     id,
		UserID: userID,
	}
	const insertStmt = "INSERT INTO test (id, nullable) VALUES (:test, :test2);"
	_, err := tql.Exec(context.Background(), pgDB, insertStmt, params)

	// Assert
	require.NoError(t, err)
	r, err := tql.QueryFirst[result](context.Background(), pgDB, "SELECT * FROM test WHERE id = $1;", id)

	require.NotEmpty(t, r)
	require.Equal(t, id, r.ID)
	require.Equal(t, userID, *r.Nullable)
	require.NoError(t, err)
	require.NoError(t, err)
}

func Test_Postgresql_Exec_Not_Named(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("postgres"))

	id := uuid.NewString()
	userID := uuid.NewString()
	const insertStmt = "INSERT INTO test (id, nullable) VALUES ($1, $2);"

	// Act
	_, err := tql.Exec(context.Background(), pgDB, insertStmt, id, userID)

	// Assert
	require.NoError(t, err)
	r, err := tql.QueryFirst[result](context.Background(), pgDB, "SELECT * FROM test WHERE id = $1;", id)

	require.NotEmpty(t, r)
	require.Equal(t, id, r.ID)
	require.Equal(t, userID, *r.Nullable)
	require.NoError(t, err)
	require.NoError(t, err)
}

func Test_Postgresql_Exec_Mixed_Named_Positional(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("postgres"))

	id := uuid.NewString()
	userID := uuid.NewString()

	// Act
	const insertStmt = "INSERT INTO test (id, nullable) VALUES ($1, :test2);"
	_, err := tql.Exec(context.Background(), pgDB, insertStmt, id, userID, map[string]any{"test2": "asdf"})

	// Assert
	require.Error(t, err)
	require.Equal(t, "mixed positional and named parameters", err.Error())
	//require.ErrorIs(t, err, fmt.Errorf("mixed positional and named parameters"))

	r, err := tql.QueryFirst[result](context.Background(), pgDB, "SELECT * FROM test WHERE id = $1;", id)
	require.NoError(t, err)
	require.Empty(t, r)
}
