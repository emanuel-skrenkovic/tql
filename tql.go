package tql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode"
)

var ErrMultipleResults = errors.New("sql: found multiple results expected single")

type typeMapper struct {
	typeFieldCache map[string]map[string]int
}

var mapper = typeMapper{
	typeFieldCache: make(map[string]map[string]int),
}

// SetActiveDriver
//
// *Do not use this!*
//
// Sets which driver to use to know which parameter syntax to use.
// Don't use this, it's global state, it's not safe for concurrent use, and it is bad.
// It is just here, so I can choose which driver I want to use in the tests for tql,
// and the tests are in a separate module so this is public.
func SetActiveDriver(driver string) error {
	for _, d := range sql.Drivers() {
		if d == driver {
			activeDriver = driver
			return nil
		}
	}

	return fmt.Errorf("cannot set active driver to %s driver %s is not registered", driver, driver)
}

type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// QuerySingleOrDefault
// A variant of QueryFirstOrDefault that expects only a single result.
//
// If the query returns more than one result, this function returns tql.ErrMultipleResults.
//
// If the query returns no results, this function return the provided default.
//
// If the query returns a single result, this function returns the result.
func QuerySingleOrDefault[T any](ctx context.Context, q Querier, def T, query string, params ...any) (T, error) {
	var result T
	result, err := QuerySingle[T](ctx, q, query, params...)
	switch {
	case err != nil && errors.Is(err, sql.ErrNoRows):
		return def, nil
	case err != nil:
		return result, err
	default:
		return result, nil
	}
}

// QuerySingle
// Queries the table to return one result. A variant of QueryFirst that expects only a single result.
//
// If the query returns more than one result, this function returns tql.ErrMultipleResults.
//
// If the query returns no results, this function return sql.ErrNoRows.
//
// If the query returns a single result, this function returns the result.
func QuerySingle[T any](ctx context.Context, q Querier, query string, params ...any) (T, error) {
	var result T
	results, err := Query[T](ctx, q, query, params...)
	if err != nil {
		return result, err
	}

	resultsLen := len(results)

	if resultsLen < 1 {
		return result, sql.ErrNoRows
	}
	if resultsLen > 1 {
		return result, ErrMultipleResults
	}

	return results[0], err
}

func QueryFirstOrDefault[T any](ctx context.Context, q Querier, def T, query string, params ...any) (T, error) {
	var result T
	result, err := QueryFirst[T](ctx, q, query, params...)
	switch {
	case err != nil && errors.Is(err, sql.ErrNoRows):
		return def, nil
	case err != nil:
		return result, err
	default:
		return result, nil
	}
}

// QueryFirst
// Queries the table and returns the first result. If the query returns no results,
// the function returns sql.ErrNoRows.
func QueryFirst[T any](ctx context.Context, q Querier, query string, params ...any) (T, error) {
	var result T
	parameterisedQuery, args, err := translateParams(query, params...)
	if err != nil {
		return result, err
	}

	rows, err := q.QueryContext(ctx, parameterisedQuery, args...)
	if err != nil {
		return result, err
	}

	if rows == nil {
		err = sql.ErrNoRows
		return result, err
	}

	defer func() {
		if rows.Err() != nil {
			return
		}

		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(closeErr, err)
		}
	}()

	if !rows.Next() {
		err = sql.ErrNoRows
		return result, err
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

// Query
// Queries the database and returns all the results as a slice. If the query returns no results,
// an empty slice of type T is returned. This matches the sql.QueryContext function from database/sql.
func Query[T any](ctx context.Context, q Querier, query string, params ...any) ([]T, error) {
	// TODO: think about returning sql.ErrNoRows if no results are found.
	result := make([]T, 0, 256)

	parameterisedQuery, args, err := translateParams(query, params...)
	if err != nil {
		return result, err
	}

	rows, err := q.QueryContext(ctx, parameterisedQuery, args...)
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

		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(closeErr, err)
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

// Exec
// Executes a statement and returns sql.Result.
//
// The statement can be parameterised using, either, the positional parameters
// (e.g. $1, $2 or ?,  ?, depending on the driver) or using named
// parameters (such as :parameter1, :parameter2).
//
// When using named parameters with structs as params, the names in the query *must* be specified as the
// db tag in the struct name. When using a map, the keys will be the names.
func Exec(ctx context.Context, e Executor, query string, params ...any) (sql.Result, error) {
	parameterisedQuery, args, err := translateParams(query, params...)
	if err != nil {
		return nil, err
	}

	return e.ExecContext(ctx, parameterisedQuery, args...)
}

func mapParameters(params ...any) (map[string]any, error) {
	parameters := make(map[string]any)

ParamLoop:
	for _, p := range params {
		if _, ok := p.(driver.Valuer); ok {
			continue
		}

		val := reflect.ValueOf(p)

		switch val.Kind() {
		case reflect.Map:
			value := reflect.Indirect(val).Interface()
			m, ok := value.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("invalid type %T, expected map[string]any", value)
			}

			for k, v := range m {
				if _, exists := parameters[k]; exists {
					return nil, fmt.Errorf("found parameter with duplicate name: %s", k)
				}

				parameters[k] = v
			}

		case reflect.Struct:
			valueType := reflect.TypeOf(p)
			typeName := typeName(valueType)

			fieldTags, found := typeFieldDBTags[typeName]
			if !found {
				fieldsCount := valueType.NumField()

				fieldTags = make([]string, fieldsCount)
				exportedFieldIndices := make([]int, 0, fieldsCount)

				for i := range fieldsCount {
					field := valueType.Field(i)

					if !field.IsExported() {
						continue ParamLoop
					}

					tag, foundTag := field.Tag.Lookup("db")
					if !foundTag {
						return nil, fmt.Errorf("field %s is not tagged with 'db' tag", field.Name)
					}

					fieldTags[i] = tag
					exportedFieldIndices = append(exportedFieldIndices, i)
				}

				typeExportedFieldIndices[typeName] = exportedFieldIndices
			}

			value := reflect.Indirect(val)
			for _, i := range typeExportedFieldIndices[typeName] {
				field := value.Field(i)
				parameters[fieldTags[i]] = field.Interface()
			}
		default: // no-op
		}
	}

	return parameters, nil
}

var activeDriver string

func getActiveDriver() string {
	if activeDriver == "" {
		activeDriver = sql.Drivers()[0]
	}

	return activeDriver
}

type indicators struct {
	named      rune
	positional rune
}

var driverIndicators = map[string]indicators{
	"postgres":         {named: ':', positional: '$'},
	"pgx":              {named: ':', positional: '$'},
	"pq-timeouts":      {named: ':', positional: '$'},
	"cloudsqlpostgres": {named: ':', positional: '$'},
	"ql":               {named: ':', positional: '$'},
	"nrpostgres":       {named: ':', positional: '$'},
	"cockroach":        {named: ':', positional: '$'},

	"mysql":   {named: ':', positional: '?'},
	"nrmysql": {named: ':', positional: '?'},

	"sqlite3":   {named: ':', positional: '?'},
	"nrsqlite3": {named: ':', positional: '?'},
}

func parameterIndicators(driverName string) (rune, rune, error) {
	i, found := driverIndicators[driverName]
	if !found {
		return 0, 0, fmt.Errorf("failed to find driver parameter indicator mapping")
	}
	return i.named, i.positional, nil
}

func translateParams(query string, params ...any) (string, []any, error) {
	parameters, err := mapParameters(params...)
	if err != nil {
		return "", nil, err
	}

	// #horribleways
	driverName := getActiveDriver()
	pos, nam, err := parameterIndicators(driverName)
	if err != nil {
		return "", nil, err
	}

	parameterisedQuery, args, err := parameteriseQuery(pos, nam, query, parameters)
	if err != nil {
		return "", nil, err
	}

	if len(args) < 1 {
		args = params
	}

	return parameterisedQuery, args, nil
}

func parameteriseQuery(
	namedParamIndicator rune,
	positionalParamIndicator rune,
	query string,
	parameters map[string]any,
) (string, []any, error) {
	var (
		insideName    bool
		hasPositional bool

		result     strings.Builder
		resultArgs = make([]any, 0, len(parameters))

		currentName strings.Builder
		currentNum  int
	)

	result.Grow(len(query))

	// TODO: inside name has to know the connection type to
	// properly decide on which token to use as the namedIndicator
	// of a parameter inside a query.
	// Also, which token to remap to.

	for _, c := range query {
		if !hasPositional && c == positionalParamIndicator {
			hasPositional = true
		}

		if !insideName && c == namedParamIndicator {
			currentName.Reset()
			insideName = true
			continue
		}

		if insideName && !(unicode.IsLetter(c) || unicode.IsNumber(c) || c == '_') {
			arg, found := parameters[currentName.String()]
			if !found {
				return "", []any{}, fmt.Errorf("query parameter '%s' not found in provided parameters", currentName.String())
			}
			resultArgs = append(resultArgs, arg)

			insideName = false
			currentNum++

			// No need for a default case here (famous last words).
			// If there is no driver, then the execution will return
			// an error before this part of code is executed.
			switch getActiveDriver() {
			case "mysql", "sqlite3":
				result.WriteRune(positionalParamIndicator)
			case "postgres", "pgx":
				result.WriteRune(positionalParamIndicator)
				result.WriteString(strconv.FormatInt(int64(currentNum), 10))
			}
			result.WriteRune(c)
			continue
		}

		if insideName {
			currentName.WriteRune(c)
			continue
		}

		result.WriteRune(c)
	}

	if hasPositional && len(resultArgs) > 0 {
		return "", []any{}, fmt.Errorf("mixed positional and named parameters")
	}

	return result.String(), resultArgs, nil
}

// TODO: candidate for caching
func createDestinations(source any, columns []string) ([]any, error) {
	value := reflect.ValueOf(source).Elem()
	valueType := value.Type()
	typeName := typeName(valueType)

	indices, found := mapper.typeFieldCache[typeName]
	if !found {
		numFields := valueType.NumField()
		indices = make(map[string]int, numFields)

		for i := range numFields {
			field := valueType.Field(i)

			tag, foundTag := field.Tag.Lookup("db")
			if !foundTag {
				continue
			}

			indices[tag] = i
		}

		mapper.typeFieldCache[typeName] = indices
	}

	dest := make([]any, len(columns))
	for i, c := range columns {
		fieldIdx, foundField := indices[c]
		if !foundField {
			return nil, fmt.Errorf("no matching field found for column: %s", c)
		}

		field := value.Field(fieldIdx)
		switch field.CanAddr() {
		case true:
			dest[i] = field.Addr().Interface()
		case false:
			dest[i] = field.Interface()
		}
	}

	return dest, nil
}

// typeFieldDBTags
//
// Acts as a cache for struct field 'db' tag names.
// Used as such:
//
// tagName := typeFieldDBTags[typeName][fieldNumber]
var typeFieldDBTags = make(map[string][]string)
var typeExportedFieldIndices = make(map[string][]int)

func bindArgs(params ...any) (map[string]any, error) {
	parameters := make(map[string]any, len(params))

	for _, p := range params {
		val := reflect.ValueOf(p)

		switch val.Kind() {
		case reflect.Map:
			value := reflect.Indirect(val).Interface()
			m, ok := value.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("bindArgs is not of type map[string]any")
			}

			for k, v := range m {
				if _, exists := parameters[k]; exists {
					return nil, fmt.Errorf("found parameter with duplicate name: %s", k)
				}

				parameters[k] = v
			}

		case reflect.Struct:
			value := reflect.Indirect(val)
			valueType := reflect.TypeOf(p)

			// Aggressively pre-cache the struct 'db' tag bindings.
			typeName := typeName(valueType)
			fieldsCount := valueType.NumField()

			fieldTags, found := typeFieldDBTags[typeName]
			if !found {
				fieldTags = make([]string, fieldsCount)
				typeFieldDBTags[typeName] = fieldTags

				for i := range fieldsCount {
					field := valueType.Field(i)
					tag, foundTag := field.Tag.Lookup("db")
					if !foundTag {
						return nil, fmt.Errorf("field %s is not tagged with 'db' tag", field.Name)
					}

					fieldTags[i] = tag
				}
			}

			for i := range fieldsCount {
				parameters[fieldTags[i]] = value.Field(i).Interface()
			}
		default: // no-op
		}
	}

	return parameters, nil
}

func typeName(typ reflect.Type) string {
	return strings.Join([]string{typ.PkgPath(), typ.Name()}, "/")
}
