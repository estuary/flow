package sql

import (
	"fmt"
	"strings"

	"github.com/estuary/protocols/fdb/tuple"
)

// ColumnType represents a minimal set of database-agnostic types that we may try to store and
// query. This set of types is slightly different than the set of JSON types. This has a "binary"
// type for dealing with byte slices, and there is no "null" type, since nullability is modeled
// separately.
type ColumnType string

// ColumnType constants that are used by ColumnTypeMapper
const (
	STRING  ColumnType = "string"
	BOOLEAN ColumnType = "boolean"
	INTEGER ColumnType = "integer"
	NUMBER  ColumnType = "number"
	OBJECT  ColumnType = "object"
	ARRAY   ColumnType = "array"
	BINARY  ColumnType = "binary"
)

// StringTypeInfo holds optional additional type information for string columns
type StringTypeInfo struct {
	Format      string
	ContentType string
	MaxLength   uint32
}

// Column describes a SQL table column that will hold JSON values
type Column struct {
	// The Name of the column, which is used when referencing columns by name when calling SQL
	// generation functions. This value should not include any quotes or escape characters.
	Name string
	// Identifier is the final form of the column name, exactly as it should be represented in SQL
	// statements. If quoting is necessary, then the quotes must be included here.
	Identifier string
	// Comment is optional text that will be used only on CREATE TABLE statements
	Comment string
	// PrimaryKey is true if this column is the primary key, or if it is part of a composite key.
	PrimaryKey bool
	// Type is the application type of the data. This corresponds closely to JSON types, but
	// includes "binary" and excludes "null". Unlike Flow Projections, a Column may only have a
	// single type, and nullability is represented as a separate boolean rather than a type itself.
	Type ColumnType
	// StringType is optional additional type information for strings.
	StringType *StringTypeInfo
	// NotNull is true if the database columns should disallow null values.
	NotNull bool
}

// Table describes a database table, which can be used to generate various types of SQL statements.
type Table struct {
	// The Name of the table before sanitization and quoting.
	Name string
	// Identifier is the final form of the table name, exactly as it should be represented in SQL
	// statements. If quoting is necessary, then the quotes must be included here.
	Identifier string
	// Optional Comment to add to create table statements.
	Comment string
	// The complete list of columns that should be created for the table and used in insert statements. This does
	// not need to include "automatic" columns (e.g. rowid), but only columns that should be
	// explicitly created and inserted into.
	Columns []Column
	// If IfNotExists is true then the create table statement will include an "IF NOT EXISTS" (or
	// equivalent).
	IfNotExists bool

	// Whether this is a temporary table. If true, then this will be created with the "TEMP(ORARY)" keyword.
	Temporary bool
	// If this is a temporary table, then this may optionally specify the ON COMMIT behavior. If
	// left blank, then no "ON COMMIT" clause will be added.
	TempOnCommit string
}

// GetColumn returns the Column with the given Name (not to be confused with Identifier). This can
// be used, for example, to map from the Name to the Identifier for a column.
func (t Table) GetColumn(name string) *Column {
	for _, col := range t.Columns {
		if col.Name == name {
			return &col
		}
	}
	return nil
}

// ParametersConverter is a slice of functions that can be used to convert a Tuple into an
// []interface{} that can be passed to the database driver. This conversion may be different
// depending on the specific driver, so instances of ParametersConverter should be obtained from the
// Generator when generating a sql statement.
type ParametersConverter []func(interface{}) (interface{}, error)

// Convert transforms the given Tuple into an []interface{} that can be used to execute SQL
// statements.
func (c ParametersConverter) Convert(tup tuple.Tuple) ([]interface{}, error) {
	var results = make([]interface{}, len(tup))
	for i, elem := range tup {
		var v, err = (c[i])(elem)
		if err != nil {
			return nil, fmt.Errorf("failed to convert value at index %d: %w", i, err)
		}
		results[i] = v
	}
	return results, nil
}

func newParametersConverter(mapper TypeMapper, table *Table, columns []string) (ParametersConverter, error) {
	var converters = make([]func(interface{}) (interface{}, error), len(columns))
	for i, name := range columns {
		var column = table.GetColumn(name)
		if column == nil {
			return nil, fmt.Errorf("Table '%s' has no such column '%s'", table.Identifier, name)
		}
		var ty, err = mapper.GetColumnType(column)
		if err != nil {
			return nil, err
		}
		converters[i] = ty.ValueConverter
	}
	return ParametersConverter(converters), nil
}

// Identity is an identity function for no-op conversions of tuple elements to `interface{}` values
// that are suitable for use as sql parameters.
func Identity(elem interface{}) (interface{}, error) {
	return elem, nil
}

// ResolvedColumnType represents the result of successfully mapping a Column to SQL DDL and a
// function that can be used to convert a Tuple element into a type that is appropriate for the
// driver.
type ResolvedColumnType struct {
	SQLType        string
	ValueConverter func(interface{}) (interface{}, error)
}

// A TypeMapper resolves a Column to a specific base SQL type. For example, for all "string" type
// Columns, it may return the "TEXT" sql type. We use a decorator pattern to compose TypeMappers.
type TypeMapper interface {
	// GetColumnType resolves a Column to a specific SQL type. For example, for all "string"
	// type Columns, it may return the "TEXT" sql type. An implementation may take into account as
	// much or as little information as it wants to about a particular column, and some may not
	// inspect the column at all.
	GetColumnType(column *Column) (*ResolvedColumnType, error)
}

// ConstColumnType is a ResolvedColumnType that is known statically at compile time.
type ConstColumnType ResolvedColumnType

// RawConstColumnType returns a ConstColumnType that always uses the given sql string as DDL and
// performs a no-op value conversion.
func RawConstColumnType(sql string) ConstColumnType {
	return ConstColumnType{
		SQLType:        sql,
		ValueConverter: Identity,
	}
}

// GetColumnType implements the TypeMapper interface
func (c ConstColumnType) GetColumnType(col *Column) (*ResolvedColumnType, error) {
	var res = ResolvedColumnType(c)
	return &res, nil
}

// TypeLengthPlaceholder is the placeholder string that may appear in the SQL string, which will be
// replaced by the MaxLength of the string.
const TypeLengthPlaceholder = "?"

// LengthConstrainedColumnType is a TypeMapper that must always have a length argument, e.g.
// "VARCHAR(42)"
type LengthConstrainedColumnType ResolvedColumnType

// GetColumnType implements the TypeMapper interface
func (c LengthConstrainedColumnType) GetColumnType(col *Column) (*ResolvedColumnType, error) {
	var resolved = strings.Replace(c.SQLType, TypeLengthPlaceholder, fmt.Sprint(col.StringType.MaxLength), 1)
	return &ResolvedColumnType{
		SQLType:        resolved,
		ValueConverter: c.ValueConverter,
	}, nil
}

// MaxLengthableColumnType is a TypeMapper that supports column types that may have a length
// argument (e.g. "VARCHAR(76)").
type MaxLengthableColumnType struct {
	WithoutLength *ConstColumnType
	WithLength    *LengthConstrainedColumnType
}

// GetColumnType implements the TypeMapper interface
func (c MaxLengthableColumnType) GetColumnType(col *Column) (*ResolvedColumnType, error) {
	if c.WithLength != nil && col.StringType != nil && col.StringType.MaxLength > 0 {
		return c.WithLength.GetColumnType(col)
	} else if c.WithoutLength != nil {
		return c.WithoutLength.GetColumnType(col)
	} else {
		return nil, fmt.Errorf("Column type requires a length argument, but no max length is present in the column description")
	}
}

// NullableTypeMapping wraps a TypeMapper to add "NULL" and/or "NOT NULL" to the generated SQL type
// depending on the nullability of the column. Most databases will assume that a column may contain
// null as long as it isn't declared with a NOT NULL constraint, but some databases (e.g. ms sql
// server) make that behavior configurable, requiring the DDL to explicitly declare a column with
// NULL if it may contain null values. This wrapper will handle either or both cases.
type NullableTypeMapping struct {
	NotNullText  string
	NullableText string
	Inner        TypeMapper
}

// GetColumnType implements the TypeMapper interface
func (mapper NullableTypeMapping) GetColumnType(col *Column) (*ResolvedColumnType, error) {
	var ty, err = mapper.Inner.GetColumnType(col)
	if err != nil {
		return nil, err
	}
	if col.NotNull && len(mapper.NotNullText) > 0 {
		ty.SQLType = fmt.Sprintf("%s %s", ty.SQLType, mapper.NotNullText)
	} else if !col.NotNull && len(mapper.NullableText) > 0 {
		ty.SQLType = fmt.Sprintf("%s %s", ty.SQLType, mapper.NullableText)
	}
	return ty, nil
}

// StringTypeMapping is a special TypeMapper for string type columns, which can take the format
// and/or content type into account when deciding what sql column type to generate.
type StringTypeMapping struct {
	Default       TypeMapper
	ByFormat      map[string]*TypeMapper
	ByContentType map[string]*TypeMapper
}

// GetColumnType implements the TypeMapper interface
func (mapping StringTypeMapping) GetColumnType(col *Column) (*ResolvedColumnType, error) {
	var stringType = col.StringType
	var resolvedMapper *TypeMapper

	if stringType != nil {
		if len(stringType.Format) > 0 {
			resolvedMapper = mapping.ByFormat[stringType.Format]
		}

		if resolvedMapper == nil && len(stringType.ContentType) > 0 {
			resolvedMapper = mapping.ByContentType[stringType.ContentType]
		}
	}

	if resolvedMapper == nil {
		resolvedMapper = &mapping.Default
	}
	return (*resolvedMapper).GetColumnType(col)
}

// ColumnTypeMapper selects a specific TypeMapper based on the type of the data that will be passed
// to as a parameter for inserts or updates to the column.
type ColumnTypeMapper map[ColumnType]TypeMapper

// GetColumnType implements the TypeMapper interface
func (amap ColumnTypeMapper) GetColumnType(col *Column) (*ResolvedColumnType, error) {
	var mapper = amap[col.Type]
	if mapper == nil {
		return nil, fmt.Errorf("unsupported type %s", col.Type)
	}
	return mapper.GetColumnType(col)
}

// CommentConfig determines how SQL comments are rendered.
type CommentConfig struct {
	// Linewise determines whether to render line or block comments. If it is true, then each line
	// of comment text will be wrapped separately. If false, then the entire multi-line block of
	// comment text will be wrapped once.
	Linewise bool
	// Wrap holds the strings that will bound the beginning and end of the comment.
	Wrap TokenPair
}

// LineComment returns a CommentConfig configured for standard sql line comments that begins
// each line with a double dash ("-- ")
func LineComment() CommentConfig {
	return CommentConfig{
		Linewise: true,
		Wrap: TokenPair{
			Left:  "-- ",
			Right: "",
		},
	}
}

// Generator generates SQL for a large variety of SQL dialects using various
// configuration parameters.
type Generator struct {
	Placeholder        func(int) string
	CommentRenderer    *CommentRenderer
	IdentifierRenderer *Renderer
	ValueRenderer      *Renderer
	TypeMappings       TypeMapper
}

// PostgresParameterPlaceholder returns $N style parameters where N is the parameter number
// starting at 1.
func PostgresParameterPlaceholder(parameterIndex int) string {
	// parameterIndex starts at 0, but postgres parameters start at $1
	return fmt.Sprintf("$%d", parameterIndex+1)
}

// QuestionMarkPlaceholder returns the constant string "?".
func QuestionMarkPlaceholder(_ int) string {
	return "?"
}

// SQLiteSQLGenerator returns a SQLGenerator for the sqlite SQL dialect.
func SQLiteSQLGenerator() Generator {
	var typeMappings = ColumnTypeMapper{
		INTEGER: RawConstColumnType("INTEGER"),
		NUMBER:  RawConstColumnType("REAL"),
		BOOLEAN: RawConstColumnType("BOOLEAN"),
		OBJECT:  RawConstColumnType("TEXT"),
		ARRAY:   RawConstColumnType("TEXT"),
		BINARY:  RawConstColumnType("BLOB"),
		STRING: StringTypeMapping{
			Default: RawConstColumnType("TEXT"),
		},
	}
	var nullable TypeMapper = NullableTypeMapping{
		NotNullText: "NOT NULL",
		Inner:       typeMappings,
	}

	return Generator{
		CommentRenderer:    LineCommentRenderer(),
		IdentifierRenderer: NewRenderer(nil, DoubleQuotesWrapper(), DefaultUnwrappedIdentifiers),
		ValueRenderer:      NewRenderer(DefaultQuoteSanitizer, SingleQuotesWrapper(), nil),
		Placeholder:        QuestionMarkPlaceholder,
		TypeMappings:       nullable,
	}
}

// PostgresSQLGenerator returns a SQLGenerator for the postgresql SQL dialect.
func PostgresSQLGenerator() Generator {
	var typeMappings TypeMapper = NullableTypeMapping{
		NotNullText: "NOT NULL",
		Inner: ColumnTypeMapper{
			INTEGER: RawConstColumnType("BIGINT"),
			NUMBER:  RawConstColumnType("DOUBLE PRECISION"),
			BOOLEAN: RawConstColumnType("BOOLEAN"),
			OBJECT:  RawConstColumnType("JSON"),
			ARRAY:   RawConstColumnType("JSON"),
			BINARY:  RawConstColumnType("BYTEA"),
			STRING: StringTypeMapping{
				Default: RawConstColumnType("TEXT"),
			},
		},
	}

	return Generator{
		CommentRenderer:    LineCommentRenderer(),
		IdentifierRenderer: NewRenderer(nil, DoubleQuotesWrapper(), DefaultUnwrappedIdentifiers),
		ValueRenderer:      NewRenderer(DefaultQuoteSanitizer, SingleQuotesWrapper(), nil),
		Placeholder:        PostgresParameterPlaceholder,
		TypeMappings:       typeMappings,
	}
}

// QueryOnPrimaryKey generates a query that has a placeholder parameter for each primary key in
// the order given in the table. Only selectColumns will be selected in the same order as
// provided.
func (gen *Generator) QueryOnPrimaryKey(table *Table, selectColumns ...string) (string, ParametersConverter, error) {
	var builder strings.Builder

	builder.WriteString("SELECT ")
	for i, colName := range selectColumns {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(table.GetColumn(colName).Identifier)
	}
	builder.WriteString(" FROM ")
	builder.WriteString(table.Identifier)
	builder.WriteString(" WHERE ")
	var pkIndex = 0
	var converters []func(interface{}) (interface{}, error)
	for _, col := range table.Columns {
		if col.PrimaryKey {
			if pkIndex > 0 {
				builder.WriteString(" AND ")
			}
			builder.WriteString(col.Identifier)
			builder.WriteString(" = ")
			builder.WriteString(gen.Placeholder(pkIndex))

			// Lookup the type mapping for this column and add the value converter
			var ty, err = gen.TypeMappings.GetColumnType(&col)
			if err != nil {
				return "", nil, err
			}
			converters = append(converters, ty.ValueConverter)
			pkIndex++
		}
	}
	builder.WriteRune(';')
	return builder.String(), ParametersConverter(converters), nil
}

// InsertStatement returns an insert statement for the given table that includes all columns.
// The returned sql will have a parameter placeholder for every column in the order they appear
// in the Table. This should generate a plain insert statement, not an upsert, since we'll know
// in advance whether each document exists or not, and only use the InsertStatement when we know
// the document does not exist.
func (gen *Generator) InsertStatement(table *Table) (string, ParametersConverter, error) {
	return gen.genInsertStatement(table, gen.Placeholder)
}

func (gen Generator) genInsertStatement(table *Table, genParams func(int) string) (string, ParametersConverter, error) {
	var builder strings.Builder
	builder.WriteString("INSERT INTO ")
	builder.WriteString(table.Identifier)
	builder.WriteString(" (")

	var converters []func(interface{}) (interface{}, error)
	for i, col := range table.Columns {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(col.Identifier)
		var ty, err = gen.TypeMappings.GetColumnType(&col)
		if err != nil {
			return "", nil, err
		}
		converters = append(converters, ty.ValueConverter)
	}
	builder.WriteString(") VALUES (")
	for i := range table.Columns {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(genParams(i))
	}
	builder.WriteString(");")
	return builder.String(), ParametersConverter(converters), nil
}

// UpdateStatement returns an update statement for the given table that sets the columns given
// in setColumns and matches based on the columns in whereColumns. The returned statement will
// have a placeholder parameter for each of the setColumns in the order given, followed by a
// parameter for each of the whereColumns in the order given.
func (gen *Generator) UpdateStatement(table *Table, setColumns []string, whereColumns []string) (string, ParametersConverter, error) {
	var builder strings.Builder
	builder.WriteString("UPDATE ")
	builder.WriteString(table.Identifier)
	builder.WriteString(" SET ")
	var parameterIndex = 0
	for i, colName := range setColumns {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(table.GetColumn(colName).Identifier)
		builder.WriteString(" = ")
		builder.WriteString(gen.Placeholder(parameterIndex))
		parameterIndex++
	}
	builder.WriteString(" WHERE ")
	for i, colName := range whereColumns {
		if i > 0 {
			builder.WriteString(" AND ")
		}
		builder.WriteString(table.GetColumn(colName).Identifier)
		builder.WriteString(" = ")
		builder.WriteString(gen.Placeholder(parameterIndex))
		parameterIndex++
	}
	builder.WriteString(";")

	var setConverters, err = newParametersConverter(gen.TypeMappings, table, setColumns)
	if err != nil {
		return "", nil, err
	}
	valConverters, err := newParametersConverter(gen.TypeMappings, table, whereColumns)
	if err != nil {
		return "", nil, err
	}
	var converters = ParametersConverter(append(setConverters, valConverters...))

	return builder.String(), converters, nil
}
