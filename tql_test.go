package tql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"
)

type dummyDriver struct{}

func (d dummyDriver) Open(string) (driver.Conn, error) {
	return nil, fmt.Errorf("not implemented")
}

func TestMain(m *testing.M) {
	sql.Register("postgres", dummyDriver{})
	m.Run()
}

func Test_Postgres_ParameteriseQuery_Substitution(t *testing.T) {
	// Arrange
	err := SetActiveDriver("postgres")
	if err != nil {
		t.Fatalf("failed to set driver: %s", err.Error())
	}

	n, p, err := parameterIndicators("postgres")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	const query = "SELECT * FROM tablename WHERE id = :id;"
	// Act
	parameterisedQuery, args, err := parameteriseQuery(n, p, query, map[string]any{"id": "123"})

	// Assert
	if err != nil {
		t.Fatalf("unexpected err: %s", err.Error())
	}

	if len(args) != 1 {
		t.Fatalf("expected len %d found %d", 1, len(args))
	}

	if parameterisedQuery == "" {
		t.Fatalf("unexpected empty 'parameterisedQuery'")
	}

	const expectedParameterisedQuery = "SELECT * FROM tablename WHERE id = $1;"
	if parameterisedQuery != expectedParameterisedQuery {
		t.Fatalf("value '%s' does not equal expected '%s'", parameterisedQuery, expectedParameterisedQuery)
	}
}

func Test_Postgres_ParameteriseQuery_Substitution_Multiple_Parameters(t *testing.T) {
	// Arrange
	err := SetActiveDriver("postgres")
	if err != nil {
		t.Fatalf("failed to set driver: %s", err.Error())
	}

	n, p, err := parameterIndicators("postgres")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	const query = "SELECT * FROM tablename WHERE id = :id OR name = :name;"
	// Act
	parameterisedQuery, args, err := parameteriseQuery(n, p, query, map[string]any{"name": "123", "id": "123"})

	// Assert
	if err != nil {
		t.Fatalf("unexpected err: %s", err.Error())
	}

	expectedArgsLen := 2
	if len(args) != expectedArgsLen {
		t.Fatalf("expected len %d found %d", expectedArgsLen, len(args))
	}

	if parameterisedQuery == "" {
		t.Fatalf("unexpected empty 'parameterisedQuery'")
	}

	const expectedParameterisedQuery = "SELECT * FROM tablename WHERE id = $1 OR name = $2;"
	if parameterisedQuery != expectedParameterisedQuery {
		t.Fatalf("value '%s' does not equal expected '%s'", parameterisedQuery, expectedParameterisedQuery)
	}
}
