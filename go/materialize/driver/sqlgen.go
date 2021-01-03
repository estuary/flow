package driver

import (
	"bufio"
	"fmt"
	"strings"
)

// ColumnType represents a minimal set of database-agnostic types that we may try to store and
// query. This set of types is slightly different than the set of JSON types. This has a "binary"
// type for dealing with byte slices, and there is no "null" type, since nullability is modeleed
// separately.
type ColumnType string

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
	// The Name of the column
	Name string
	// Comment is optional text that will be used only on CREATE TABLE statements
	Comment string
	// PrimaryKey is true if this column is the primary key, or if it is part of a composite key.
	PrimaryKey bool
	// Type is the application type of the data. This corresponds closely to JSON types. The
	// database-specific column type.
	Type ColumnType
	// StringType is optional additional type information for strings.
	StringType *StringTypeInfo
	// NotNull is true if the database columns should disallow null values.
	NotNull bool
}

// Table describes a database table, which can be used to generate various types of SQL statements.
type Table struct {
	// The Name of the table
	Name string
	// Optional Comment to add to create table statements.
	Comment string
	// The complete list of Columns that are part of this table. More specifically, this is the complete
	// list of columns that should be created for the table and used in insert statements. This does
	// not need to include "automatic" columns
	Columns []Column
	// If IfNotExists is true then the create table statement will include an "IF NOT EXISTS" (or
	// equivalent).
	IfNotExists bool
}

// A SqlGenerator is a type that can generate all the sql required for a Flow materialization using
// the SqlDriver type.
type SqlGenerator interface {
	// Comment returns a new string with a comment containing the given string. The returned string
	// must always end with a newline. The comment can either be a line or a block comment.
	Comment(string) string

	// Generates a CREATE TABLE statement for the given table. The returned statement must not
	// contain any parameter placeholders.
	CreateTable(table *Table) (string, error)

	// QueryOnPrimaryKey generates a query that has a placeholder parameter for each primary key in
	// the order given in the table. Only selectColumns will be selected in the same order as
	// provided. An error should be returned if selectColumns is empty.
	QueryOnPrimaryKey(table *Table, selectColumns ...string) (string, error)

	// InsertStatement returns an insert statement for the given table that includes all columns.
	// The returns sql will have a parameter placeholder for every column. For most systems, this
	// can be a plain insert statement, not an upsert, since we only use the insert statement if a
	// query for this document has already returned no result.
	InsertStatement(table *Table) (string, error)

	// DirectInsertStatement returns an insert statement without any bound parameters. Only string
	// type parameters are accepted, since that is all we require from this interface. The string
	// args will be quoted and escaped as required. The number of provided args must be the same as
	// the number of the columns, and they must also be provided in the same order.
	DirectInsertStatement(table *Table, args ...string) (string, error)

	// UpdateStatement returns an update statement for the given table that sets the columns given
	// in setColumns and matches based on the columns in whereColumns. The returned statement will
	// have a placeholder parameter for each of the setColumns in the order given, followed by a
	// parameter for each of the whereColumns in the order given.
	UpdateStatement(table *Table, setColumns []string, whereColumns []string) (string, error)
}

// TokenPair is a generic way of representing strings that can be used to surround some text for
// quoting and commenting.
type TokenPair struct {
	Left  string
	Right string
}

func (pair *TokenPair) writeWrapped(builder *strings.Builder, text string) {
	builder.WriteString(pair.Left)
	builder.WriteString(text)
	builder.WriteString(pair.Right)
}

// DoubleQuotes returns a TokenPair with a single double quote character on the both the Left and
// the Right.
func DoubleQuotes() TokenPair {
	return TokenPair{
		Left:  "\"",
		Right: "\"",
	}
}

// A TypeMapper resolves a Column to a specific base SQL type. For example, for all "string" type
// Columns, it may return the "TEXT" sql type. We use a decorator pattern to compose TypeMappers.
type TypeMapper interface {
	// GetColumnType resolves a Column to a specific SQL type. For example, for all "string"
	// type Columns, it may return the "TEXT" sql type. An implementation may take into account as
	// much or as little information as it wants to about a particular column, and some may not
	// inspect the column at all.
	GetColumnType(column *Column) (string, error)
}

// ConstColumnType is a TypeMapper that simply returns the raw string value. Most column types can
// just use this.
type ConstColumnType string

func (columnType ConstColumnType) GetColumnType(col *Column) (string, error) {
	return string(columnType), nil
}

const TYPE_LENGTH_PLACEHOLDER = "?"

// LenLengthConstrainedColumnType is a TypeMapper that must always have a length argument, e.g.
// "VARCHAR(42)"
type LengthConstrainedColumnType string

func (columnType LengthConstrainedColumnType) GetColumnType(col *Column) (string, error) {
	return strings.Replace(string(columnType), TYPE_LENGTH_PLACEHOLDER, fmt.Sprint(col.StringType.MaxLength), 1), nil
}

// MaxLengthableColumnType is a TypeMapper that supports column types that may have a length
// argument (e.g. "VARCHAR(76)").
type MaxLengthableColumnType struct {
	WithoutLength *ConstColumnType
	WithLength    *LengthConstrainedColumnType
}

func (columnType MaxLengthableColumnType) GetColumnType(col *Column) (string, error) {
	if columnType.WithLength != nil && col.StringType != nil && col.StringType.MaxLength > 0 {
		return columnType.WithLength.GetColumnType(col)
	} else if columnType.WithoutLength != nil {
		return columnType.WithoutLength.GetColumnType(col)
	} else {
		return "", fmt.Errorf("Column type requires a length argument, but no max length is present in the column description")
	}
}

// NullableTypeMapping wraps a TypeMapper to add "NULL" and/or "NOT NULL" to the generated SQL type
// depending on the nullability of the column. Most databases will assume that a column may contain
// null as long as it isnt' declared with a NOT NULL constraint, but some databases (e.g. ms sql
// server) make that behavior configurable, requiring the DDL to explicitly declare a column with
// NULL if it may contain null values. This wrapper will handle either or both cases.
type NullableTypeMapping struct {
	NotNullText  string
	NullableText string
	Inner        TypeMapper
}

func (mapper NullableTypeMapping) GetColumnType(col *Column) (string, error) {
	var ty, err = mapper.Inner.GetColumnType(col)
	if err != nil {
		return "", err
	}
	if col.NotNull && len(mapper.NotNullText) > 0 {
		return fmt.Sprintf("%s %s", ty, mapper.NotNullText), nil
	} else if !col.NotNull && len(mapper.NullableText) > 0 {
		return fmt.Sprintf("%s %s", ty, mapper.NullableText), nil
	} else {
		return ty, nil
	}
}

// StringTypeMapping is a special TypeMapper for string type columns, which can take the format
// and/or content type into account when deciding what sql column type to generate.
type StringTypeMapping struct {
	Default       TypeMapper
	ByFormat      map[string]*TypeMapper
	ByContentType map[string]*TypeMapper
}

func (mapping StringTypeMapping) GetColumnType(col *Column) (string, error) {
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

type ColumnTypeMapper map[ColumnType]TypeMapper

func (amap ColumnTypeMapper) GetColumnType(col *Column) (string, error) {
	var mapper = amap[col.Type]
	if mapper == nil {
		return "", fmt.Errorf("unsupported type %s", col.Type)
	}
	return mapper.GetColumnType(col)
}

type CommentConfig struct {
	Linewise bool
	Wrap     TokenPair
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

// A GenericSqlGenerator is able to generate SQL for a large variety of SQL dialects using various
// configuration parameters.
type GenericSqlGenerator struct {
	CommentConf             CommentConfig
	IdentifierQuotes        TokenPair
	GetParameterPlaceholder func(int) string
	QuoteStringValue        func(string) string
	TypeMappings            TypeMapper
}

// GetPostgresParameterPlaceholder returns $N style parameters where N is the parameter number
// starting at 1.
func GetPostgresParameterPlaceholder(parameterIndex int) string {
	// parameterIndex starts at 0, but postgres parameters start at $1
	return fmt.Sprintf("$%d", parameterIndex+1)
}

func QuestionMarkPlaceholder(_ int) string {
	return "?"
}

// DefaultQuoteStringValue surrounds the given string with single quotes and escapes any single
// quote characters within the string by doubling them. This works for well enough for most
// databases, since we're not super concerned about sql injection edge cases here (the user already has
// database credentials, after all).
func DefaultQuoteStringValue(value string) string {
	var builder strings.Builder
	builder.WriteRune('\'')
	var val = value
	for {
		var idx = strings.IndexByte(val, byte('\''))
		if idx == -1 {
			builder.WriteString(val)
			break
		} else {
			builder.WriteString(val[0:idx])
			builder.WriteString("''")
			val = val[idx+1:] // safe because we know there's a single quote char there
		}
	}
	builder.WriteRune('\'')
	return builder.String()
}

// SqlitSqliteSqlGenerator returns a SqlGenerator for the sqlite SQL dialect.
func SqliteSqlGenerator() GenericSqlGenerator {
	var typeMappings = ColumnTypeMapper{
		INTEGER: ConstColumnType("INTEGER"),
		NUMBER:  ConstColumnType("REAL"),
		BOOLEAN: ConstColumnType("BOOLEAN"),
		OBJECT:  ConstColumnType("TEXT"),
		ARRAY:   ConstColumnType("TEXT"),
		BINARY:  ConstColumnType("BLOB"),
		STRING: StringTypeMapping{
			Default: ConstColumnType("TEXT"),
		},
	}
	var nullable TypeMapper = NullableTypeMapping{
		NotNullText: "NOT NULL",
		Inner:       typeMappings,
	}

	return GenericSqlGenerator{
		CommentConf:             LineComment(),
		IdentifierQuotes:        DoubleQuotes(),
		GetParameterPlaceholder: QuestionMarkPlaceholder,
		TypeMappings:            nullable,
		QuoteStringValue:        DefaultQuoteStringValue,
	}
}

// PostgresSqlGenerator returns a SqlGenerator for the postgresql SQL dialect.
func PostgresSqlGenerator() GenericSqlGenerator {
	var typeMappings TypeMapper = NullableTypeMapping{
		NotNullText: "NOT NULL",
		Inner: ColumnTypeMapper{
			INTEGER: ConstColumnType("BIGINT"),
			NUMBER:  ConstColumnType("DOUBLE PRECISION"),
			BOOLEAN: ConstColumnType("BOOLEAN"),
			OBJECT:  ConstColumnType("JSON"),
			ARRAY:   ConstColumnType("JSON"),
			BINARY:  ConstColumnType("BYTEA"),
			STRING: StringTypeMapping{
				Default: ConstColumnType("TEXT"),
			},
		},
	}

	return GenericSqlGenerator{
		CommentConf:             LineComment(),
		IdentifierQuotes:        DoubleQuotes(),
		GetParameterPlaceholder: GetPostgresParameterPlaceholder,
		TypeMappings:            typeMappings,
		QuoteStringValue:        DefaultQuoteStringValue,
	}
}

// Comment is part of the SqlGenerator implementation for GenericSqlGenerator
func (gen *GenericSqlGenerator) Comment(text string) string {
	var builder strings.Builder
	gen.writeComment(&builder, text, "")
	return builder.String()
}

// CreateTable is part of the SqlGenerator implementation for GenericSqlGenerator
func (gen *GenericSqlGenerator) CreateTable(table *Table) (string, error) {
	var builder strings.Builder

	if len(table.Comment) > 0 {
		gen.writeComment(&builder, table.Comment, "")
	}

	builder.WriteString("CREATE TABLE ")
	if table.IfNotExists {
		builder.WriteString("IF NOT EXISTS ")
	}
	gen.IdentifierQuotes.writeWrapped(&builder, table.Name)
	builder.WriteString(" (\n\t")

	for i, column := range table.Columns {
		if i > 0 {
			builder.WriteString(",\n\t")
		}
		if len(column.Comment) > 0 {
			gen.writeComment(&builder, column.Comment, "\t")
			// The comment will always end with a newline, but we'll need to add the indentation
			// for the next line. If there's no comment, then the indentation will aready be there.
			builder.WriteRune('\t')
		}
		gen.writeIdent(&builder, column.Name)
		builder.WriteRune(' ')

		var sqlType, err = gen.TypeMappings.GetColumnType(&column)
		if err != nil {
			return "", err
		}
		builder.WriteString(sqlType)
	}
	builder.WriteString(",\n\n\tPRIMARY KEY(")
	var firstPk = true
	for _, column := range table.Columns {
		if column.PrimaryKey {
			if !firstPk {
				builder.WriteString(", ")
			}
			firstPk = false
			gen.writeIdent(&builder, column.Name)
		}
	}
	// Close the primary key paren, then newline and close the create table statement
	builder.WriteString(")\n);\n")
	return builder.String(), nil
}

// QueryOnPrimaryKey is part of the SqlGenerator implementation for GenericSqlGenerator
func (gen *GenericSqlGenerator) QueryOnPrimaryKey(table *Table, selectColumns ...string) (string, error) {
	if len(selectColumns) == 0 {
		return "", fmt.Errorf("missing columns to solect")
	}
	var builder strings.Builder

	builder.WriteString("SELECT ")
	for i, colName := range selectColumns {
		if i > 0 {
			builder.WriteString(", ")
		}
		gen.writeIdent(&builder, colName)
	}
	builder.WriteString(" FROM ")
	gen.writeIdent(&builder, table.Name)
	builder.WriteString(" WHERE ")
	var pkIndex = 0
	for _, col := range table.Columns {
		if col.PrimaryKey {
			if pkIndex > 0 {
				builder.WriteString(" AND ")
			}
			gen.writeIdent(&builder, col.Name)
			builder.WriteString(" = ")
			builder.WriteString(gen.GetParameterPlaceholder(pkIndex))
			pkIndex++
		}
	}
	builder.WriteRune(';')
	return builder.String(), nil
}

// InsertStatement is part of the SqlGenerator implementation for GenericSqlGenerator
func (gen *GenericSqlGenerator) InsertStatement(table *Table) (string, error) {
	return gen.genInsertStatement(table, gen.GetParameterPlaceholder)
}

// DirectInsertStatement is part of the SqlGenerator implementation for GenericSqlGenerator
func (gen *GenericSqlGenerator) DirectInsertStatement(table *Table, args ...string) (string, error) {
	if len(args) != len(table.Columns) {
		return "", fmt.Errorf("The table has %d columns, but only %d arguments were provided", len(table.Columns), len(args))
	}
	var escapedArgs []string
	for _, arg := range args {
		escapedArgs = append(escapedArgs, gen.QuoteStringValue(arg))
	}
	var genParams = func(i int) string {
		return escapedArgs[i]
	}
	return gen.genInsertStatement(table, genParams)
}

func (gen *GenericSqlGenerator) genInsertStatement(table *Table, genParams func(int) string) (string, error) {
	var builder strings.Builder
	builder.WriteString("INSERT INTO ")
	gen.writeIdent(&builder, table.Name)
	builder.WriteString(" (")
	for i, col := range table.Columns {
		if i > 0 {
			builder.WriteString(", ")
		}
		gen.writeIdent(&builder, col.Name)
	}
	builder.WriteString(") VALUES (")
	for i := range table.Columns {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(genParams(i))
	}
	builder.WriteString(");")
	return builder.String(), nil
}

// UpdateStatement is part of the SqlGenerator implementation for GenericSqlGenerator
func (gen *GenericSqlGenerator) UpdateStatement(table *Table, setColumns []string, whereColumns []string) (string, error) {
	var builder strings.Builder
	builder.WriteString("UPDATE ")
	gen.writeIdent(&builder, table.Name)
	builder.WriteString(" SET ")
	var parameterIndex = 0
	for i, colName := range setColumns {
		if i > 0 {
			builder.WriteString(", ")
		}
		gen.writeIdent(&builder, colName)
		builder.WriteString(" = ")
		builder.WriteString(gen.GetParameterPlaceholder(parameterIndex))
		parameterIndex++
	}
	builder.WriteString(" WHERE ")
	for i, colName := range whereColumns {
		if i > 0 {
			builder.WriteString(" AND ")
		}
		gen.writeIdent(&builder, colName)
		builder.WriteString(" = ")
		builder.WriteString(gen.GetParameterPlaceholder(parameterIndex))
		parameterIndex++
	}
	builder.WriteString(";")
	return builder.String(), nil
}

func (gen *GenericSqlGenerator) writeIdent(builder *strings.Builder, ident string) {
	gen.IdentifierQuotes.writeWrapped(builder, ident)
}

func (gen *GenericSqlGenerator) writeComment(builder *strings.Builder, text string, indent string) {
	var comment = gen.CommentConf
	var scanner = bufio.NewScanner(strings.NewReader(text))

	if comment.Linewise {
		var first = true
		for scanner.Scan() {
			if !first {
				builder.WriteRune('\n')
				builder.WriteString(indent)
			}
			first = false
			comment.Wrap.writeWrapped(builder, scanner.Text())
		}
	} else {
		builder.WriteString(gen.CommentConf.Wrap.Left)
		var first = true
		for scanner.Scan() {
			if !first {
				builder.WriteRune('\n')
				builder.WriteString(indent)
			}
			first = false
			builder.WriteString(scanner.Text())
		}
		builder.WriteString(gen.CommentConf.Wrap.Right)
	}
	// Comments always end with a newline
	builder.WriteRune('\n')
}
