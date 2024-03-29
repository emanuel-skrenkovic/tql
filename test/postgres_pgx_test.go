package main

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/emanuel-skrenkovic/tql"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func Test_Postgresql_pgx_QueryFirstOrDefault_Returns_First_Result(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	id := uuid.New()
	nullable := uuid.New()

	_, err := pgxDB.Exec(fmt.Sprintf("INSERT INTO test VALUES ('%s', '%s');", id.String(), nullable.String()))
	require.NoError(t, err)

	d := result{
		ID: uuid.NewString(),
	}

	// Act
	r, err := tql.QueryFirstOrDefault[result](
		context.Background(),
		pgxDB,
		d,
		"SELECT id, nullable FROM test WHERE id = $1;",
		id,
	)
	// Assert
	require.NoError(t, err)
	require.Equal(t, id.String(), r.ID)
	require.NotNil(t, r.Nullable)
	require.Equal(t, nullable.String(), *r.Nullable)

	require.NotEqual(t, d.ID, r.ID)
}

func Test_Postgresql_pgx_QueryFirstOrDefault_Returns_Default_When_Query_Returns_No_Results(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	id := uuid.New()
	nullable := uuid.New()

	_, err := pgxDB.Exec(fmt.Sprintf("INSERT INTO test VALUES ('%s', '%s');", id.String(), nullable.String()))
	require.NoError(t, err)

	defaultNullable := uuid.NewString()
	d := result{
		ID:       uuid.NewString(),
		Nullable: &defaultNullable,
	}

	// Act
	r, err := tql.QueryFirstOrDefault[result](
		context.Background(),
		pgxDB,
		d,
		"SELECT id, nullable FROM test where id = $1;",
		uuid.NewString(),
	)

	// Assert
	require.NoError(t, err)
	require.NotEqual(t, id.String(), r.ID)
	require.NotEqual(t, nullable.String(), *r.Nullable)

	require.Equal(t, d.ID, r.ID)
	require.NotNil(t, r.Nullable)
	require.Equal(t, defaultNullable, *r.Nullable)
}

func Test_Postgresql_pgx_QueryFirstOrDefault_Returns_First_Result_When_Query_Returns_Multiple_Results(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	id := uuid.NewString()
	nullable := uuid.NewString()

	_, err := pgxDB.Exec(
		fmt.Sprintf(
			"INSERT INTO test VALUES ('%s', '%s'), ('%s', '%s');",
			id,
			nullable,
			uuid.NewString(),
			nullable,
		),
	)
	require.NoError(t, err)

	defaultNullable := uuid.NewString()
	d := result{
		ID:       uuid.NewString(),
		Nullable: &defaultNullable,
	}

	// Act
	r, err := tql.QueryFirstOrDefault[result](
		context.Background(),
		pgxDB,
		d,
		"SELECT id, nullable FROM test where nullable = $1;",
		nullable,
	)

	// Assert
	require.NoError(t, err)
	require.NotNil(t, r.Nullable)
	require.Equal(t, nullable, *r.Nullable)

	require.NotEqual(t, defaultNullable, *r.Nullable)
}

func Test_Postgresql_pgx_QuerySingle_Returns_sqlErrNoRows_When_Query_Returns_No_Results(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	id := uuid.New()
	nullable := uuid.New()

	_, err := pgxDB.Exec(fmt.Sprintf("INSERT INTO test VALUES ('%s', '%s');", id.String(), nullable.String()))
	require.NoError(t, err)

	// Act
	r, err := tql.QuerySingle[result](
		context.Background(),
		pgxDB,
		"SELECT id, nullable FROM test where id = $1;",
		uuid.NewString(),
	)

	// Assert
	require.ErrorIs(t, err, sql.ErrNoRows)
	require.Empty(t, r)
}

func Test_Postgresql_pgx_QuerySingle_Returns_tqlErrMultipleResults_When_Query_Returns_Multiple_Results(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	id := uuid.NewString()
	nullable := uuid.NewString()

	_, err := pgxDB.Exec(
		fmt.Sprintf(
			"INSERT INTO test VALUES ('%s', '%s'), ('%s', '%s');",
			id,
			nullable,
			uuid.NewString(),
			nullable,
		),
	)
	require.NoError(t, err)

	// Act
	r, err := tql.QuerySingle[result](
		context.Background(),
		pgxDB,
		"SELECT id, nullable FROM test where nullable = $1;",
		nullable,
	)

	// Assert
	require.ErrorIs(t, err, tql.ErrMultipleResults)
	require.Empty(t, r)
}

func Test_Postgresql_pgx_QuerySingleOrDefault_Returns_Result_When_Query_Returns_Single_Result(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	id := uuid.New()
	nullable := uuid.New()

	_, err := pgxDB.Exec(fmt.Sprintf("INSERT INTO test VALUES ('%s', '%s');", id.String(), nullable.String()))
	require.NoError(t, err)

	defaultNullable := uuid.NewString()
	d := result{
		ID:       uuid.NewString(),
		Nullable: &defaultNullable,
	}

	// Act
	r, err := tql.QuerySingleOrDefault[result](
		context.Background(),
		pgxDB,
		d,
		"SELECT id, nullable FROM test where id = $1;",
		id,
	)

	// Assert
	require.NoError(t, err)
	require.Equal(t, id.String(), r.ID)
	require.NotNil(t, r.Nullable)
	require.Equal(t, nullable.String(), *r.Nullable)

	require.NotEqual(t, d.ID, r.ID)
	require.NotEqual(t, defaultNullable, *r.Nullable)
}

func Test_Postgresql_pgx_QuerySingleOrDefault_Returns_Default_When_Query_Returns_No_Results(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	id := uuid.New()
	nullable := uuid.New()

	_, err := pgxDB.Exec(fmt.Sprintf("INSERT INTO test VALUES ('%s', '%s');", id.String(), nullable.String()))
	require.NoError(t, err)

	defaultNullable := uuid.NewString()
	d := result{
		ID:       uuid.NewString(),
		Nullable: &defaultNullable,
	}

	// Act
	r, err := tql.QuerySingleOrDefault[result](
		context.Background(),
		pgxDB,
		d,
		"SELECT id, nullable FROM test where id = $1;",
		uuid.NewString(),
	)

	// Assert
	require.NoError(t, err)
	require.NotEqual(t, id.String(), r.ID)
	require.NotEqual(t, nullable.String(), *r.Nullable)

	require.Equal(t, d.ID, r.ID)
	require.NotNil(t, r.Nullable)
	require.Equal(t, defaultNullable, *r.Nullable)
}

func Test_Postgresql_pgx_QuerySingleOrDefault_Returns_tqlErrMultipleResults_When_Query_Returns_Multiple_Results(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	id := uuid.NewString()
	nullable := uuid.NewString()

	_, err := pgxDB.Exec(
		fmt.Sprintf(
			"INSERT INTO test VALUES ('%s', '%s'), ('%s', '%s');",
			id,
			nullable,
			uuid.NewString(),
			nullable,
		),
	)
	require.NoError(t, err)

	defaultNullable := uuid.NewString()
	d := result{
		ID:       uuid.NewString(),
		Nullable: &defaultNullable,
	}

	// Act
	r, err := tql.QuerySingleOrDefault[result](
		context.Background(),
		pgxDB,
		d,
		"SELECT id, nullable FROM test where nullable = $1;",
		nullable,
	)

	// Assert
	require.ErrorIs(t, err, tql.ErrMultipleResults)
	require.Empty(t, r)
}

func Test_Postgresql_pgx_QueryOne(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	id := uuid.New()
	nullable := uuid.New()

	_, err := pgxDB.Exec(fmt.Sprintf("INSERT INTO test VALUES ('%s', '%s');", id.String(), nullable.String()))
	require.NoError(t, err)

	// Act
	r, err := tql.QueryFirst[result](context.Background(), pgxDB, "SELECT id, nullable FROM test WHERE id = $1;", id)

	// Assert
	require.NoError(t, err)
	require.Equal(t, id.String(), r.ID)
	require.NotNil(t, r.Nullable)
	require.Equal(t, nullable.String(), *r.Nullable)
}

func Test_Postgresql_pgx_QueryOne_With_Named_Parameters(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	id := uuid.New()
	nullable := uuid.New()

	_, err := pgxDB.Exec(fmt.Sprintf("INSERT INTO test (id, nullable) VALUES ('%s', '%s');", id.String(), nullable.String()))
	require.NoError(t, err)

	// Act
	r, err := tql.QueryFirst[*string](
		context.Background(),
		pgxDB,
		"SELECT id FROM test WHERE id = :id;",
		map[string]any{"id": id},
	)

	// Assert
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Equal(t, id.String(), *r)
}

func Test_Postgresql_pgx_QueryOne_String(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	id := uuid.New()
	nullable := uuid.New()

	_, err := pgxDB.Exec(fmt.Sprintf("INSERT INTO test (id, nullable) VALUES ('%s', '%s');", id.String(), nullable.String()))
	require.NoError(t, err)

	// Act
	r, err := tql.QueryFirst[string](context.Background(), pgxDB, "SELECT id FROM test WHERE id = $1;", id)

	// Assert
	require.NoError(t, err)
	require.Equal(t, id.String(), r)
}

func Test_Postgresql_pgx_QueryOne_String_Pointer(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	id := uuid.New()
	nullable := uuid.New()

	_, err := pgxDB.Exec(fmt.Sprintf("INSERT INTO test (id, nullable) VALUES ('%s', '%s');", id.String(), nullable.String()))
	require.NoError(t, err)

	// Act
	r, err := tql.QueryFirst[*string](context.Background(), pgxDB, "SELECT id FROM test WHERE id = $1;", id)

	// Assert
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Equal(t, id.String(), *r)
}

func Test_Postgresql_pgx_QueryOne_With_Mixed_Named_Positional_Parameters_Returns_Error(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	id := uuid.New()
	nullable := uuid.New()

	_, err := pgxDB.Exec(fmt.Sprintf("INSERT INTO test (id, nullable) VALUES ('%s', '%s');", id.String(), nullable.String()))
	require.NoError(t, err)

	// Act
	r, err := tql.QueryFirst[*string](
		context.Background(),
		pgxDB,
		"SELECT id FROM test WHERE id = :id;",
		map[string]any{"id": id},
	)

	// Assert
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Equal(t, id.String(), *r)
}

func Test_Postgresql_pgx_QueryOne_Int_Pointer(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	id := uuid.New()
	nullable := uuid.New()

	_, err := pgxDB.Exec(fmt.Sprintf("INSERT INTO test (id, nullable) VALUES ('%s', '%s');", id.String(), nullable.String()))
	require.NoError(t, err)

	// Act
	r, err := tql.QueryFirst[*int](context.Background(), pgxDB, "SELECT 420;")

	// Assert
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Equal(t, 420, *r)
}

func Test_Postgresql_pgx_Query(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	_, err := pgxDB.Exec("DELETE FROM test;")
	require.NoError(t, err)

	const insertStmt = `
		INSERT INTO 
		    test (id, nullable) 
		VALUES 
		    ($1, $2),
			($3, $4),
			($5, $6),
			($7, $8),
			($9, $10);`

	ids := make([]any, 10)
	for i := 0; i < 10; i++ {
		ids[i] = uuid.NewString()
	}

	_, err = pgxDB.Exec(insertStmt, ids...)
	require.NoError(t, err)

	// Act
	r, err := tql.Query[result](context.Background(), pgxDB, "SELECT id, nullable FROM test;")

	// Assert
	require.NoError(t, err)
	require.Len(t, r, 5)

	for _, result := range r {
		require.NotEmpty(t, result.ID)
		require.NotNil(t, result.Nullable)
	}
}

func Test_Postgresql_pgx_Query_Basic_Type_From_Tx(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	_, err := pgxDB.Exec("DELETE FROM test;")
	require.NoError(t, err)

	const insertStmt = `
		INSERT INTO 
		    test (id, nullable) 
		VALUES 
		    ($1, $2);`

	id := uuid.NewString()
	nullable := uuid.NewString()

	_, err = pgxDB.Exec(insertStmt, id, nullable)
	require.NoError(t, err)

	tx, _ := pgxDB.BeginTx(context.Background(), &sql.TxOptions{})

	// Act
	r, err := tql.QueryFirst[string](context.Background(), tx, "SELECT id FROM test;")

	require.NoError(t, tx.Commit())

	// Assert
	require.NoError(t, err)
	require.NotEmpty(t, r)
	require.Equal(t, id, r)
}

func Test_Postgresql_pgx_Query_Basic_Type_Pointer(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	_, err := pgxDB.Exec("DELETE FROM test;")
	require.NoError(t, err)

	const insertStmt = `
		INSERT INTO 
		    test (id, nullable) 
		VALUES 
		    ($1, $2);`

	id := uuid.NewString()
	nullable := uuid.NewString()

	_, err = pgxDB.Exec(insertStmt, id, nullable)
	require.NoError(t, err)

	// Act
	r, err := tql.QueryFirst[*string](context.Background(), pgxDB, "SELECT id FROM test;")

	// Assert
	require.NoError(t, err)
	require.NotEmpty(t, r)
	require.Equal(t, id, *r)
}

func Test_Postgresql_pgx_Query_Basic_Type_Pointer_Null(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	// Act
	r, err := tql.QueryFirst[*string](context.Background(), pgxDB, "SELECT NULL;")

	// Assert
	require.NoError(t, err)
	require.Nil(t, r)
}

func Test_Postgresql_pgx_Query_Empty_Result(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	_, err := pgxDB.Exec("INSERT INTO test VALUES ('asdf', 'fdsa');")
	require.NoError(t, err)

	// Act
	r, err := tql.Query[result](context.Background(), pgxDB, "SELECT * FROM test WHERE id = '';")

	// Assert
	require.NoError(t, err)
	require.NotNil(t, r)
}

func Test_Postgresql_pgx_Exec(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	// Act
	const insertStmt = "INSERT INTO test (id, nullable) VALUES (:test, :test2);"
	_, err := tql.Exec(context.Background(), pgxDB, insertStmt, map[string]any{
		"test":  "totally_new_id",
		"test2": "totally_new_id_2",
	})

	// Assert
	require.NoError(t, err)
	r, err := tql.QueryFirst[result](context.Background(), pgxDB, "SELECT * FROM test WHERE id = $1;", "totally_new_id")

	require.NotEmpty(t, r)
	require.Equal(t, "totally_new_id", r.ID)
	require.Equal(t, "totally_new_id_2", *r.Nullable)
	require.NoError(t, err)
	require.NoError(t, err)
}

func Test_Postgresql_pgx_Exec_With_Struct(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

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
	_, err := tql.Exec(context.Background(), pgxDB, insertStmt, params)

	// Assert
	require.NoError(t, err)
	r, err := tql.QueryFirst[result](context.Background(), pgxDB, "SELECT * FROM test WHERE id = $1;", id)

	require.NotEmpty(t, r)
	require.Equal(t, id, r.ID)
	require.Equal(t, userID, *r.Nullable)
	require.NoError(t, err)
	require.NoError(t, err)
}

func Test_Postgresql_pgx_Exec_Not_Named(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	id := uuid.NewString()
	userID := uuid.NewString()
	const insertStmt = "INSERT INTO test (id, nullable) VALUES ($1, $2);"

	// Act
	_, err := tql.Exec(context.Background(), pgxDB, insertStmt, id, userID)

	// Assert
	require.NoError(t, err)
	r, err := tql.QueryFirst[result](context.Background(), pgxDB, "SELECT * FROM test WHERE id = $1;", id)

	require.NotEmpty(t, r)
	require.Equal(t, id, r.ID)
	require.Equal(t, userID, *r.Nullable)
	require.NoError(t, err)
	require.NoError(t, err)
}

func Test_Postgresql_pgx_Exec_Mixed_Named_Positional(t *testing.T) {
	// Arrange
	require.NoError(t, tql.SetActiveDriver("pgx"))

	id := uuid.NewString()
	userID := uuid.NewString()

	// Act
	const insertStmt = "INSERT INTO test (id, nullable) VALUES ($1, :test2);"
	_, err := tql.Exec(context.Background(), pgxDB, insertStmt, id, userID, map[string]any{"test2": "asdf"})

	// Assert
	require.Error(t, err)
	require.Equal(t, "mixed positional and named parameters", err.Error())
	//require.ErrorIs(t, err, fmt.Errorf("mixed positional and named parameters"))

	r, err := tql.QueryFirst[result](context.Background(), pgxDB, "SELECT * FROM test WHERE id = $1;", id)
	require.ErrorIs(t, err, sql.ErrNoRows)
	require.Empty(t, r)
}
