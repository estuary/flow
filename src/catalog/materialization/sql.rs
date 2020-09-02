use super::{FieldProjection, MaterializationTarget, ProjectionsError};
use estuary_json::schema::types;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Encapsulates the mapping from a single JSON String type to one or more SQL column types. Currently,
/// this only maps to a single "default" column type, but the intent is to allow for specialization
/// later by using the content-type and/or "format" schema annotations to determine a more specific
/// column type.
/// TODO: allow for specialization based on string 'format' from json schema
#[derive(Debug, Serialize, Deserialize)]
pub struct StringTypeMapping {
    /// The column type to use if there is no other more specific match. Note that the
    /// `SqlColumnType` may further specialize the DDL based on the presence of a "maxLength"
    /// schema validation keyword.
    default_type: SqlColumnType,
}
impl StringTypeMapping {
    fn simple(column_type: impl Into<String>) -> StringTypeMapping {
        StringTypeMapping::new(SqlColumnType::simple(column_type))
    }

    fn new(default_type: SqlColumnType) -> StringTypeMapping {
        StringTypeMapping { default_type }
    }

    /// returns the resolved `SqlColumnType` to use for the given mime type. Currently this always
    /// just returns the `default_type` and ignores the passed mime string, since it's not really
    /// clear how the mechanism for specialization ought to work. But this gives us a placeholder
    /// for whenever we do figure that out, even if the exact arguments to this function would
    /// change.
    fn lookup(&self, _mime_type: Option<&str>) -> &SqlColumnType {
        &self.default_type
    }
}

/// Top-level structure for mapping each JSON data type to a SQL column type. For all types except
/// strings, this mapping is simple and direct. Strings are more complicated because they are often
/// used to represent things like dates and email addresses, and databases often have specialized
/// column types that are more appropriate for these things. Strings may also hold base64 encoded
/// data, which has separate mapping here, since these might map to separate column types. For
/// example, a base64 string might map to a BLOB column, while a plain string maps to TEXT.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectionTypeMappings {
    pub integer: SqlColumnType,
    pub number: SqlColumnType,
    pub boolean: SqlColumnType,
    pub array: SqlColumnType,
    pub object: SqlColumnType,
    pub string: StringTypeMapping,
    pub string_base64: StringTypeMapping,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SqlColumnType {
    pub ddl: SqlColumnTypeDdl,
    pub max_supported_length: Option<u64>,
    pub insert_template: String,
}

impl SqlColumnType {
    fn simple(column_type: impl Into<String>) -> SqlColumnType {
        SqlColumnType {
            ddl: SqlColumnTypeDdl::AlwaysPlain {
                plain: column_type.into(),
            },
            max_supported_length: None,
            insert_template: "?".to_owned(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged, rename_all = "camelCase")]
pub enum SqlColumnTypeDdl {
    AlwaysPlain { plain: String },
    OptionalLength { plain: String, with_length: String },
    // RequiredLength(String) will be needed to support oracle nvarchar columns
}

/// Holds the configuration that's used to generate SQL statements. In order to support a given
/// database as a materialization target, we need an instance of this struct that is specific to
/// both the target database server and the target SQL dialect. For example, many
/// "postgresql-compatible" databases may use the same SQL dialect, but with different column
/// types. Since this encapsulates both the dialect and the mappings from JSON types to SQL column
/// types, a distinct `SqlMaterializationConfig` may be needed.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SqlMaterializationConfig {
    /// The maximum length (in utf-8-encoded bytes) that is allowed for column names. Any field
    /// names longer than this will result in an error when DDL is generated.
    identifier_max_length: Option<u32>,
    /// String to use for quoting identifiers. For most databases, the `left` and `right` will be
    /// the same, and commonly a double-quote character is used for both. Some databases will use
    /// different characters on the left and right (notably MS SqlServer, which can use '[' and
    /// ']'), and so these are represented separately. Beware also of databases that allow
    /// different sets of quote characters, as they often have different semantics.
    identifier_quotes: TokenPair,

    /// How to express comments in the sql dialect. Typically this will be done using a block
    /// comment with `/*` and `*/`, but not all databases support that, so there's also an option
    /// for line comments, or to disable comments entirely. Comments are added to the SQL DDL to
    /// clarify where each field came from.
    comments: CommentStyle,

    /// SQL fragment to add to columns that cannot contain null. Typically this will be "NOT NULL".
    /// This can be an empty string if such a fragment is not needed.
    not_null: String,
    /// SQL fragment to add to columns that may contain null values. Some databases require this in
    /// certain contexts (especially for external tables), but for most this should be an empty
    /// string. I (phil) don't remember if there's cases where we're actually liable to need this
    /// for non-external tables, so we may be able to remove it.
    nullable: String,
    /// Holds mappings from JSON types to SQL column types.
    type_mappings: ProjectionTypeMappings,
}

impl SqlMaterializationConfig {
    /// Returns the hard-coded configuration that's used for generating SQL for PostgreSQL.
    pub fn postgres() -> Self {
        SqlMaterializationConfig {
            identifier_max_length: Some(63),
            identifier_quotes: TokenPair::symetrical("\""),
            comments: CommentStyle::Block(TokenPair::new("/*", "*/")),
            not_null: "NOT NULL".to_owned(),
            nullable: String::new(),
            type_mappings: ProjectionTypeMappings {
                integer: SqlColumnType::simple("BIGINT"),
                number: SqlColumnType::simple("DOUBLE PRECISION"),
                boolean: SqlColumnType::simple("BOOLEAN"),

                // we might end up needing to add configuration for how we insert json values
                array: SqlColumnType::simple("JSON"),
                object: SqlColumnType::simple("JSON"),
                string: StringTypeMapping::new(SqlColumnType {
                    ddl: SqlColumnTypeDdl::OptionalLength {
                        plain: "TEXT".to_owned(),
                        with_length: "VARCHAR(?)".to_owned(),
                    },
                    max_supported_length: None,
                    insert_template: "?".to_owned(),
                }),
                string_base64: StringTypeMapping::new(SqlColumnType {
                    ddl: SqlColumnTypeDdl::AlwaysPlain {
                        plain: "BYTEA".to_owned(),
                    },
                    max_supported_length: None,
                    // TODO: consider alternatives for string -> blob conversions. Maybe we should
                    // just have the application always decode the base64?
                    insert_template: "decode(?, 'base64')".to_owned(),
                }),
            },
        }
    }

    /// Returns the hard-coded configuration that's used for generating SQL for Sqlite3.
    pub fn sqlite() -> Self {
        SqlMaterializationConfig {
            identifier_max_length: None,
            identifier_quotes: TokenPair::symetrical("\""),
            comments: CommentStyle::Block(TokenPair::new("/*", "*/")),
            not_null: "NOT NULL".to_owned(),
            nullable: String::new(),
            type_mappings: ProjectionTypeMappings {
                integer: SqlColumnType::simple("INTEGER"),
                number: SqlColumnType::simple("REAL"),
                boolean: SqlColumnType::simple("BOOLEAN"),
                array: SqlColumnType::simple("TEXT"),
                object: SqlColumnType::simple("TEXT"),
                string: StringTypeMapping::simple("TEXT"),
                string_base64: StringTypeMapping::simple("BLOB"),
            },
        }
    }

    /// Generates the "CREATE TABLE" statement for the given materialization target.
    pub fn generate_ddl(&self, target: MaterializationTarget) -> Result<String, ProjectionsError> {
        use std::fmt::Write;

        // We'll accumulate invalid fields in these vectors so that we can report all of the
        // invalid projections at once instead of forcing users to re-build repeatedly in order to
        // discover one error at a time.
        let mut invalid_types = Vec::new();
        let mut invalid_identifiers = Vec::new();

        let mut buffer = String::with_capacity(1024);

        let table_description = TableDescription(&target);
        write!(
            &mut buffer,
            "{}\nCREATE TABLE {} IF NOT EXISTS (",
            self.comment(&table_description),
            self.quoted(target.table_name)
        )
        .unwrap();

        let mut first = true;
        for field in target.fields.iter() {
            if !self.is_field_name_valid(field) {
                invalid_identifiers.push(field.clone());
            }
            if let Some(column_type) = self.lookup_type(field) {
                let column_ddl_gen = ColumnDdlGen {
                    indent: "\t",
                    conf: self,
                    sql_type: column_type,
                    field,
                };

                if first {
                    first = false;
                } else {
                    buffer.push_str(",\n");
                }
                write!(&mut buffer, "\n{}", column_ddl_gen).unwrap();
            } else {
                invalid_types.push(field.clone());
            }
        }

        let mut primary_keys = target
            .fields
            .iter()
            .filter(|f| f.is_primary_key)
            .map(|f| self.quoted(f.field_name.as_str()))
            .peekable();
        if primary_keys.peek().is_some() {
            // We have at least one primary key defined for the table, so we'll emit that ddl here
            write!(
                &mut buffer,
                ",\n\n\tPRIMARY KEY({})",
                primary_keys.format(", ")
            )
            .unwrap();
        }
        buffer.push_str("\n);");

        let mut error = ProjectionsError::empty(target.target_type);
        if !invalid_types.is_empty() {
            let description = String::from(MIXED_TYPES_ERR_MSG);
            error.naughty_projections.insert(description, invalid_types);
        }
        if !invalid_identifiers.is_empty() {
            let description = format!("Cannot create SQL table columns with names longer than {} bytes because that is the maximum length supported by {}",
                                      self.identifier_max_length.expect("missing identifier_max_length"), target.target_type);
            error
                .naughty_projections
                .insert(description, invalid_identifiers);
        }
        if !error.is_empty() {
            Err(error)
        } else {
            Ok(buffer)
        }
    }

    fn is_field_name_valid(&self, field: &FieldProjection) -> bool {
        if let Some(max) = self.identifier_max_length {
            field.field_name.len() <= max as usize
        } else {
            true
        }
    }

    fn comment<'a, T: fmt::Display>(&'a self, content: &'a T) -> Comment<'a, T> {
        Comment {
            style: &self.comments,
            content,
        }
    }

    fn quoted<'a>(&'a self, field: &'a str) -> Surrounded<'a, str> {
        Surrounded {
            conf: &self.identifier_quotes,
            field,
        }
    }

    fn lookup_type(&self, field: &FieldProjection) -> Option<&SqlColumnType> {
        let mime = field.string_content_type.as_ref().map(String::as_str);
        let non_null = field.types & (!types::NULL);
        match non_null {
            types::STRING if !field.string_content_encoding_is_base64 => {
                Some(&self.type_mappings.string.lookup(mime))
            }
            types::STRING if field.string_content_encoding_is_base64 => {
                Some(&self.type_mappings.string_base64.lookup(mime))
            }
            types::BOOLEAN => Some(&self.type_mappings.boolean),
            types::INTEGER => Some(&self.type_mappings.integer),
            types::NUMBER => Some(&self.type_mappings.number),
            types::OBJECT => Some(&self.type_mappings.object),
            types::ARRAY => Some(&self.type_mappings.array),
            _ => None,
        }
    }
}

const MIXED_TYPES_ERR_MSG: &str = "Cannot create SQL table columns for json fields that hold mixed data types.\
                           Each projected field must hold only one data type (besides null). \
                           Consider either removing these fields from your projections, or updating \
                           your schema so that they will always have a single known type.";

#[derive(Debug, Serialize, Deserialize)]
enum CommentStyle {
    Block(TokenPair),
    Line(String),
    None,
}
impl CommentStyle {
    fn is_none(&self) -> bool {
        match self {
            CommentStyle::None => true,
            _ => false,
        }
    }
}

#[derive(Debug)]
struct Comment<'a, T: fmt::Display> {
    style: &'a CommentStyle,
    content: &'a T,
}

impl<'a, T: fmt::Display> fmt::Display for Comment<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.style {
            CommentStyle::Block(pair) => write!(f, "{} {} {}", pair.left, self.content, pair.right),
            CommentStyle::Line(start) => write!(f, "{} {}", start, self.content),
            CommentStyle::None => Ok(()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct TokenPair {
    left: String,
    right: String,
}
impl TokenPair {
    fn new(l: impl Into<String>, r: impl Into<String>) -> TokenPair {
        TokenPair {
            left: l.into(),
            right: r.into(),
        }
    }

    fn symetrical(c: impl Into<String>) -> TokenPair {
        let left = c.into();
        let right = left.clone();
        TokenPair::new(left, right)
    }
}

/// Helper struct that just wraps the given `field` in the identifier quotes for the particular
/// SQL dialect.
#[derive(Debug)]
struct Surrounded<'a, T: ?Sized> {
    conf: &'a TokenPair,
    field: &'a T,
}
impl<'a, T: fmt::Display + ?Sized> fmt::Display for Surrounded<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}{}{}", self.conf.left, self.field, self.conf.right)
    }
}

/// Helper struct that holds the resolved `SqlColumnType`, the `FieldProjection`, and the
/// `SqlMaterializationConfig`, and implements `Display` to format the actual DDL for a single
/// column.
#[derive(Debug)]
struct ColumnDdlGen<'a> {
    indent: &'a str,
    sql_type: &'a SqlColumnType,
    field: &'a FieldProjection,
    conf: &'a SqlMaterializationConfig,
}

impl<'a> fmt::Display for ColumnDdlGen<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if !self.conf.comments.is_none() {
            // regardless of whether it's a line or block comment, we'll put a newline after it for
            // readability, since the descriptions can get a little long.
            writeln!(
                f,
                "{}{}",
                self.indent,
                self.conf.comment(&ColumnDescription(&self.field))
            )?;
        }
        write!(
            f,
            "{}{} ",
            self.indent,
            self.conf.quoted(self.field.field_name.as_str())
        )?;

        match &self.sql_type.ddl {
            SqlColumnTypeDdl::AlwaysPlain { plain } => f.write_str(plain.as_str())?,
            SqlColumnTypeDdl::OptionalLength { plain, with_length } => {
                if let Some(len) = self.field.string_max_length {
                    write!(f, "{}({})", with_length, len)?;
                } else {
                    f.write_str(plain.as_str())?;
                }
            }
        }

        if !self.field.is_nullable() && !self.conf.not_null.is_empty() {
            write!(f, " {}", self.conf.not_null)?;
        }
        if self.field.is_nullable() && !self.conf.nullable.is_empty() {
            write!(f, " {}", self.conf.nullable)?;
        }
        Ok(())
    }
}

struct ColumnDescription<'a>(&'a FieldProjection);
impl<'a> fmt::Display for ColumnDescription<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let source = if self.0.user_provided {
            "user provided"
        } else {
            "auto-generated"
        };
        let partition = if self.0.is_partition_key {
            "(partition key) "
        } else {
            ""
        };
        write!(
            f,
            "{} projection of JSON at: {} {}with inferred types: [{}]",
            source,
            self.0.location_ptr,
            partition,
            self.0.types.iter().format(", ")
        )
    }
}

struct TableDescription<'a>(&'a MaterializationTarget<'a>);
impl<'a> fmt::Display for TableDescription<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Materialization '{}' for Estuary collection '{}', intended for {}",
            self.0.materialization_name, self.0.collection_name, self.0.target_type
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use insta::assert_snapshot;

    #[test]
    fn ddl_is_generated_without_any_primary_key() {
        let fields = basic_fields();
        let postgres_conf = SqlMaterializationConfig::postgres();
        let target = MaterializationTarget {
            collection_name: "my_test/collection",
            materialization_name: "testMaterialization",
            target_type: "postgres",
            target_uri: "any://test/uri",
            table_name: "test_postgres_table",
            fields: fields.as_slice(),
        };

        let schema = postgres_conf
            .generate_ddl(target)
            .expect("failed to generate sql");
        assert_snapshot!(schema);
    }

    #[test]
    fn ddl_is_generated_for_posgres_with_composit_primary_keys() {
        let mut fields = basic_fields();
        fields[0].is_primary_key = true;
        fields[1].is_primary_key = true;
        let postgres_conf = SqlMaterializationConfig::postgres();
        let target = MaterializationTarget {
            collection_name: "my_test/collection",
            materialization_name: "testMaterialization",
            target_type: "postgres",
            target_uri: "any://test/uri",
            table_name: "test_postgres_table",
            fields: fields.as_slice(),
        };

        let schema = postgres_conf
            .generate_ddl(target)
            .expect("failed to generate sql");
        assert_snapshot!(schema);
    }

    #[test]
    fn ddl_is_generated_for_sqlite_with_composit_primary_keys() {
        let mut fields = basic_fields();
        fields[0].is_primary_key = true;
        fields[1].is_primary_key = true;
        let sqlite_conf = SqlMaterializationConfig::sqlite();
        let target = MaterializationTarget {
            collection_name: "my_test/collection",
            materialization_name: "testMaterialization",
            target_type: "sqlite",
            target_uri: "any://test/uri",
            table_name: "test_sqlite_table",
            fields: fields.as_slice(),
        };

        let schema = sqlite_conf
            .generate_ddl(target)
            .expect("failed to generate sql");
        assert_snapshot!(schema);
    }

    #[test]
    fn invalid_projections_are_returned_in_a_single_error() {
        let mut fields = basic_fields();
        // names are too long for postgres
        fields[0].field_name = std::iter::repeat('f').take(64).collect();
        fields[1].field_name = std::iter::repeat('g').take(64).collect();

        // columns with mixed types don't work for any sql database
        fields[2].types = types::BOOLEAN | types::OBJECT;
        fields[3].types = types::OBJECT | types::INTEGER;

        let target = MaterializationTarget {
            collection_name: "my_test/collection",
            materialization_name: "testMaterialization",
            target_type: "postgres",
            target_uri: "any://test/uri",
            table_name: "test_postgres_table",
            fields: fields.as_slice(),
        };

        let err = SqlMaterializationConfig::postgres()
            .generate_ddl(target)
            .expect_err("expected an error generating ddl");
        assert_snapshot!(err.to_string());
    }

    fn basic_fields() -> Vec<FieldProjection> {
        vec![
            field("intCol", types::INTEGER),
            field("numCol", types::INTEGER),
            field("boolCol", types::BOOLEAN),
            field("objCol", types::OBJECT),
            field("arrayCol", types::ARRAY),
            field("intColNullable", types::INTEGER | types::NULL),
            field("numColNullable", types::NUMBER | types::NULL),
            field("boolColNullable", types::BOOLEAN | types::NULL),
            field("objColNullable", types::OBJECT | types::NULL),
            field("arrayColNullable", types::ARRAY | types::NULL),
            field("basicString", types::STRING),
            field("basicStringNullable", types::STRING | types::NULL),
            FieldProjection {
                string_content_encoding_is_base64: true,
                ..field("base64String", types::STRING)
            },
            FieldProjection {
                string_content_encoding_is_base64: true,
                ..field("base64StringNullable", types::STRING | types::NULL)
            },
        ]
    }

    fn field(name: &str, types: types::Set) -> FieldProjection {
        FieldProjection {
            field_name: name.to_owned(),
            location_ptr: format!("/{}", name),
            user_provided: true,
            types,
            must_exist: true,
            is_primary_key: false,
            is_partition_key: false,
            string_content_type: None,
            string_content_encoding_is_base64: false,
            string_max_length: None,
        }
    }
}
