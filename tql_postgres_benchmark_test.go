package tql

import (
	"arena"
	"testing"
)

func Benchmark_Postgres_ParameteriseQuery(b *testing.B) {
	b.StopTimer()
	activeDriver = "postgres"

	n, p, err := parameterIndicators("postgres")
	if err != nil {
		b.Fatalf("unexpected error: %s", err.Error())
	}

	type t struct {
		Name  string `db:"name"`
		Age   int    `db:"age"`
		First string `db:"first"`
		Last  string `db:"last"`
	}
	am := t{"Emanuel Skrenkovic", 30, "Emanuel", "Skrenkovic"}

	const query = "INSERT INTO foo (a, b, c, d) VALUES (:name, :age, :first, :last)"
	a := arena.NewArena()

	args, _ := bindArgs(a, am)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = parameteriseQuery(a, n, p, query, args)
	}

	b.StopTimer()
	a.Free()
}

func Benchmark_Postgres_bindArgs_Struct(b *testing.B) {
	b.StopTimer()
	activeDriver = "postgres"
	type t struct {
		Name  string `db:"name"`
		Age   int    `db:"age"`
		First string `db:"first"`
		Last  string `db:"last"`
	}
	am := t{"Emanuel Skrenkovic", 30, "Emanuel", "Skrenkovic"}

	a := arena.NewArena()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bindArgs(a, am)
	}
	b.StopTimer()
	a.Free()
}

func Benchmark_Postgres_createDestinations(b *testing.B) {
	b.StopTimer()
	cols := []string{
		"name",
		"age",
		"first",
		"last",
	}
	type t struct {
		Name  string `db:"name"`
		Age   int    `db:"age"`
		First string `db:"first"`
		Last  string `db:"last"`
	}
	am := t{"Emanuel Skrenkovic", 30, "Emanuel", "Skrenkovic"}

	a := arena.NewArena()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _ = createDestinations(a, &am, cols)
	}
	b.StopTimer()
	a.Free()
}

func Benchmark_Postgres_mapParameters_Struct(b *testing.B) {
	b.StopTimer()
	activeDriver = "postgres"
	type t struct {
		Name  string `db:"name"`
		Age   int    `db:"age"`
		First string `db:"first"`
		Last  string `db:"last"`
	}
	am := t{"Emanuel Skrenkovic", 30, "Emanuel", "Skrenkovic"}

	a := arena.NewArena()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _ = mapParameters(a, am)
	}

	a.Free()
}

func Benchmark_Postgres_mapParameters_Map(b *testing.B) {
	b.StopTimer()
	activeDriver = "postgres"
	type t struct {
		Name  string `db:"name"`
		Age   int    `db:"age"`
		First string `db:"first"`
		Last  string `db:"last"`
	}
	am := map[string]any{
		"name":  "Emanuel Skrenkovic",
		"age":   30,
		"first": "Emanuel",
		"last":  "Skrenkovic",
	}

	a := arena.NewArena()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _ = mapParameters(a, am)
	}

	a.Free()
}
