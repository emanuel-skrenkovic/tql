package tql

import "testing"

func Benchmark_Postgres_ParameteriseQuery(b *testing.B) {
	b.StopTimer()
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
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		args, _ := bindArgs(am)
		_, _, _ = parameteriseQuery(n, p, query, args)
	}
}

func Benchmark_Postgres_bindArgs_Struct(b *testing.B) {
	b.StopTimer()
	type t struct {
		Name  string `db:"name"`
		Age   int    `db:"age"`
		First string `db:"first"`
		Last  string `db:"last"`
	}
	am := t{"Emanuel Skrenkovic", 30, "Emanuel", "Skrenkovic"}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bindArgs(am)
	}
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

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _ = createDestinations(&am, cols)
	}
}

func Benchmark_Postgres_mapParameters_Struct(b *testing.B) {
	b.StopTimer()
	type t struct {
		Name  string `db:"name"`
		Age   int    `db:"age"`
		First string `db:"first"`
		Last  string `db:"last"`
	}
	am := t{"Emanuel Skrenkovic", 30, "Emanuel", "Skrenkovic"}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _ = mapParameters(am)
	}
}

func Benchmark_Postgres_mapParameters_Map(b *testing.B) {
	b.StopTimer()
	am := map[string]any{
		"name":  "Emanuel Skrenkovic",
		"age":   30,
		"first": "Emanuel",
		"last":  "Skrenkovic",
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _ = mapParameters(am)
	}
}
