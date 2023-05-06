package tql

import (
	"testing"
)

func Test_Postgres_ParameterizeQuery_Substition(t *testing.T) {
	// Arrange
	n, p, err := parameterIndicators("postgres")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	const query = "SELECT * FROM tablename WHERE id = :id;"
	// Act
	parameterizedQuery, args, err := parameterizeQuery(n, p, query, map[string]any{"id": "123"})

	// Assert
	if err != nil {
		t.Fatalf("unexpected err: %s", err.Error())
	}

	if len(args) != 1 {
		t.Fatalf("expected len %d found %d", 1, len(args))
	}

	if parameterizedQuery == "" {
		t.Fatalf("unexpected empty 'parameterizedQuery'")
	}

	const expectedParameterizedQuery = "SELECT * FROM tablename WHERE id = $1;"
	if parameterizedQuery != expectedParameterizedQuery {
		t.Fatalf("value '%s' does not equal expected '%s'", parameterizedQuery, expectedParameterizedQuery)
	}
}

func Test_Postgres_ParameterizeQuery_Substition_Multiple_Parameters(t *testing.T) {
	// Arrange
	n, p, err := parameterIndicators("postgres")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	const query = "SELECT * FROM tablename WHERE id = :id OR name = :name;"
	// Act
	parameterizedQuery, args, err := parameterizeQuery(n, p, query, map[string]any{"name": "123", "id": "123"})

	// Assert
	if err != nil {
		t.Fatalf("unexpected err: %s", err.Error())
	}

	expectedArgsLen := 2
	if len(args) != expectedArgsLen {
		t.Fatalf("expected len %d found %d", expectedArgsLen, len(args))
	}

	if parameterizedQuery == "" {
		t.Fatalf("unexpected empty 'parameterizedQuery'")
	}

	const expectedParameterizedQuery = "SELECT * FROM tablename WHERE id = $1 OR name = $2;"
	if parameterizedQuery != expectedParameterizedQuery {
		t.Fatalf("value '%s' does not equal expected '%s'", parameterizedQuery, expectedParameterizedQuery)
	}
}
