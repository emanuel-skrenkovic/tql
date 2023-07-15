package main

import (
	"database/sql"
	"log"
	"os"
	"testing"

	"github.com/joho/godotenv"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq"
)

var pgDB *sql.DB
var cockroachDB *sql.DB
var mariaDB *sql.DB
var sqlite3DB *sql.DB

const (
	EnvVarNamePqDbConnectionString        = "PQ_DATABASE_URL"
	EnvVarNameMariaDbConnectionString     = "MARIADB_DATABASE_URL"
	EnvVarNameCockroachDBConnectionString = "COCKROACH_DB_DATABASE_URL"
)

type result struct {
	ID       string  `db:"id"`
	Nullable *string `db:"nullable"`
}

func TestMain(m *testing.M) {
	if err := godotenv.Load("config.env"); err != nil {
		log.Fatal(err)
	}

	dbConnStringPG := os.Getenv(EnvVarNamePqDbConnectionString)
	if dbConnStringPG == "" {
		log.Fatal("empty PQ_DATABASE_URL environment variable")
	}

	dbConnStringMariaDB := os.Getenv(EnvVarNameMariaDbConnectionString)
	if dbConnStringMariaDB == "" {
		log.Fatal("empty MARIADB_DATABASE_URL environment variable")
	}

	dbConnStringCockroachDB := os.Getenv(EnvVarNameCockroachDBConnectionString)
	if dbConnStringCockroachDB == "" {
		log.Fatal("empty COCKROACH_DB_DATABASE_URL environment variable")
	}

	fixture, err := NewLocalTestFixture(
		"./docker-compose.yml",
		WithWaitDBFunc("tql-postgres", dbConnStringPG, "postgres", 5432),
		WithWaitDBFunc("tql-cockroachdb", dbConnStringCockroachDB, "postgres", 26257),
		WithWaitDBFunc("tql-mariadb", dbConnStringMariaDB, "mysql", 3306),
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := fixture.Start(); err != nil {
		fixture.Stop()
		log.Fatal(err)
	}

	defer func() {
		if err := fixture.Stop(); err != nil {
			log.Fatal(err)
		}
	}()

	pgDB, err = sql.Open("postgres", dbConnStringPG)
	if err != nil {
		log.Fatal(err)
	}

	cockroachDB, err = sql.Open("postgres", dbConnStringCockroachDB)
	if err != nil {
		log.Fatal(err)
	}

	mariaDB, err = sql.Open("mysql", dbConnStringMariaDB)
	if err != nil {
		log.Fatal(err)
	}

	sqlite3DB, err = sql.Open("sqlite3", ":memory:")
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := pgDB.Close(); err != nil {
			log.Printf("error closing database: %s", err.Error())
		}

		if err := cockroachDB.Close(); err != nil {
			log.Printf("error closing database: %s", err.Error())
		}

		if err := mariaDB.Close(); err != nil {
			log.Printf("error closing database: %s", err.Error())
		}

		if err := sqlite3DB.Close(); err != nil {
			log.Printf("error closing database: %s", err.Error())
		}
	}()

	if _, err := pgDB.Exec("CREATE TABLE test (id text, nullable text);"); err != nil {
		log.Fatal(err)
	}

	if _, err := cockroachDB.Exec("CREATE TABLE test (id text, nullable text);"); err != nil {
		log.Fatal(err)
	}

	if _, err := mariaDB.Exec("CREATE TABLE test (id text, nullable text);"); err != nil {
		log.Fatal(err)
	}

	if _, err := sqlite3DB.Exec("CREATE TABLE test (id text, nullable text);"); err != nil {
		log.Fatal(err)
	}

	_ = m.Run()

	if err := recover(); err != nil {
		log.Println(err)
	}

	if _, err := pgDB.Exec("DROP TABLE test;"); err != nil {
		log.Println(err)
	}

	if _, err := cockroachDB.Exec("DROP TABLE test;"); err != nil {
		log.Println(err)
	}

	if _, err := mariaDB.Exec("DROP TABLE test;"); err != nil {
		log.Println(err)
	}

	if _, err := sqlite3DB.Exec("DROP TABLE test;"); err != nil {
		log.Println(err)
	}

	if err := fixture.Stop(); err != nil {
		log.Fatal(err)
	}
}
