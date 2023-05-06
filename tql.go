package tql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"unicode"
)

type typeMapper struct {
	mappings       map[reflect.Type]func(typeName string, field reflect.Value) reflect.Value
	typeFieldCache map[string]map[string]int
}

var mapper = typeMapper{
	mappings:       make(map[reflect.Type]func(string, reflect.Value) reflect.Value),
	typeFieldCache: make(map[string]map[string]int),
}

type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func QueryOneNamed[T any](ctx context.Context, q Querier, query string, params ...any) (T, error) {
	panic("not implemented")
}

// TODO:
// QuerySingle
// QuerySingleOrDefault
// QueryFirst
// QueryFirstOrDefault

func QueryFirst[T any](ctx context.Context, q Querier, query string, params ...any) (result T, err error) {
	rows, err := q.QueryContext(ctx, query, params...)
	if err != nil {
		return result, err
	}

	if rows == nil {
		return result, sql.ErrNoRows
	}

	defer func() {
		if rows.Err() != nil {
			return
		}

		if err = rows.Close(); err != nil {
			// #horribleways
			err = fmt.Errorf("failed to close rows: %w", err)
		}
	}()

	if !rows.Next() {
		return result, sql.ErrNoRows
	}

	val := reflect.Indirect(reflect.ValueOf(result))

	switch val.Kind() {
	case reflect.Struct:
		var cols []string
		cols, err = rows.Columns()
		if err != nil {
			return result, err
		}

		var dest []any
		dest, err = createDestinations(&result, cols)
		if err != nil {
			return result, err
		}

		if err = rows.Scan(dest...); err != nil {
			return result, err
		}

	case reflect.Slice:
		return result, fmt.Errorf("invalid type: slice")

	case reflect.Pointer:
		underlyingType := reflect.TypeOf(result).Elem()
		zero := reflect.New(underlyingType)

		val.Set(zero)

		if err = rows.Scan(result); err != nil {
			return result, err
		}

	default:
		if err = rows.Scan(&result); err != nil {
			return result, err
		}
	}

	return result, err
}

func QueryNamed[T any](ctx context.Context, q Querier, query string, params ...any) ([]T, error) {
	panic("not implemented")
}

func Query[T any](ctx context.Context, q Querier, query string, params ...any) (result []T, err error) {
	result = make([]T, 0)

	rows, err := q.QueryContext(ctx, query, params...)
	if err != nil {
		return result, err
	}

	if rows == nil {
		return result, nil
	}

	defer func() {
		if rows.Err() != nil {
			return
		}

		if err = rows.Close(); err != nil {
			// #horribleways
			err = fmt.Errorf("failed to close rows: %w", err)
		}
	}()

	for rows.Next() {
		var current T

		val := reflect.Indirect(reflect.ValueOf(current))

		switch val.Kind() {
		case reflect.Struct:
			var cols []string
			cols, err = rows.Columns()
			if err != nil {
				return result, err
			}

			var dest []any
			dest, err = createDestinations(&current, cols)
			if err != nil {
				return result, err
			}

			if err = rows.Scan(dest...); err != nil {
				return result, err
			}

		case reflect.Pointer:
			underlyingType := reflect.TypeOf(current).Elem()
			zero := reflect.New(underlyingType)

			val.Set(zero)

			if err = rows.Scan(current); err != nil {
				return result, err
			}

		default:
			if err = rows.Scan(&current); err != nil {
				return result, err
			}
		}

		result = append(result, current)
	}

	return result, err
}

type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type Preparer interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

func ExecNamed(ctx context.Context, e Executor, query string, params ...any) (sql.Result, error) {
	panic("not implemented")
}

func Exec(ctx context.Context, e Executor, query string, params ...any) (sql.Result, error) {
	parameters := make(map[string]any)
	for _, p := range params {
		val := reflect.ValueOf(p)

		switch val.Kind() {
		case reflect.Map:
			value := reflect.Indirect(val).Interface()
			m := value.(map[string]any)

			for k, v := range m {
				if _, exists := parameters[k]; exists {
					return nil, fmt.Errorf("found parameter with duplicate name: %s", k)
				}

				parameters[k] = v
			}

		case reflect.Struct:
			// TODO: cache per type
			value := reflect.Indirect(val)
			valueType := reflect.TypeOf(p)

			for i := 0; i < value.NumField(); i++ {
				field := valueType.Field(i)
				tag, found := field.Tag.Lookup("db")
				if !found {
					return nil, fmt.Errorf("field %s is not tagged with 'db' tag", field.Name)
				}

				parameters[tag] = value.Field(i).Interface()
			}
		}
	}

	parameterizedQuery, args, err := parameterizeQuery(query, parameters)
	if err != nil {
		return nil, err
	}

	if len(args) < 1 {
		args = params
	}

	//log.Println(parameterizedQuery)
	//log.Printf("ARGS: %+v\n", args)

	return e.ExecContext(ctx, parameterizedQuery, args...)
}

func isNameChar(c rune) bool {
	return unicode.IsLetter(c) || unicode.IsNumber(c) || c == '_'
}

func parameterizeQuery(query string, parameters map[string]any) (string, []any, error) {
	var (
		insideName bool

		hasPositional bool

		result     string
		resultArgs []any

		currentName string
		currentNum  int
	)
	for _, c := range query {
		if !hasPositional && c == '$' {
			hasPositional = true
		}

		if !insideName && c == ':' {
			insideName = true
			//result += string(c)
			continue
		}

		if insideName && !isNameChar(c) {
			arg, found := parameters[currentName]
			if !found {
				return "", []any{}, fmt.Errorf("query parameter '%s' not found in provided parameters", currentName)
			}
			resultArgs = append(resultArgs, arg)

			insideName = false
			currentName = ""
			currentNum++

			result += fmt.Sprintf("$%s%c", strconv.Itoa(currentNum), c)
			continue
		}

		if insideName {
			// #horribleways
			currentName += string(c)
			continue
		}

		result += string(c)
	}

	if hasPositional && len(resultArgs) > 0 {
		return "", []any{}, fmt.Errorf("mixed positional and named parameters")
	}

	return result, resultArgs, nil
}

// TODO: candidate for caching
func createDestinations(source any, columns []string) ([]any, error) {
	value := reflect.ValueOf(source).Elem()
	valueType := value.Type()

	typeName := valueType.Name()
	if indices, found := mapper.typeFieldCache[typeName]; found {
		dest := make([]any, len(columns))
		for i, c := range columns {
			fieldIdx, found := indices[c]
			if !found {
				return nil, fmt.Errorf("no matching field found for column: %s", c)
			}

			field := value.Field(fieldIdx)
			if field.CanAddr() {
				dest[i] = field.Addr().Interface()
			} else {
				dest[i] = field.Interface()
			}
		}
		return dest, nil
	}

	numFields := valueType.NumField()
	indices := make(map[string]int, numFields)
	for i := 0; i < numFields; i++ {
		field := valueType.Field(i)

		tag, found := field.Tag.Lookup("db")
		if !found {
			return nil, fmt.Errorf("field %s is not tagged with 'db' tag", field.Name)
		}

		indices[tag] = i
	}

	dest := make([]any, len(columns))
	for i, c := range columns {
		fieldIdx, found := indices[c]
		if !found {
			return nil, fmt.Errorf("no matching field found for column: %s", c)
		}

		field := value.Field(fieldIdx)
		if field.CanAddr() {
			dest[i] = field.Addr().Interface()
		} else {
			dest[i] = field.Interface()
		}
	}

	mapper.typeFieldCache[typeName] = indices

	return dest, nil
}
