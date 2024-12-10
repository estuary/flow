use std::fmt::{self, Display};

use itertools::Itertools;

use crate::firebolt::firebolt_types::TableType;

use super::firebolt_types::{identifier_quote, Table};

#[derive(Debug, PartialEq)]
pub struct CreateTable<'a> {
    pub table: &'a Table,
    pub if_not_exists: bool,
    /// Extra SQL string passed on table creation
    pub extra: &'a str,
}

impl<'a> Display for CreateTable<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let if_not_exists = if self.if_not_exists {
            "IF NOT EXISTS"
        } else {
            ""
        };

        // External tables do not have indices
        let indices = if self.table.r#type != TableType::External {
            let keys: Vec<String> = self
                .table
                .schema
                .columns
                .iter()
                .filter(|col| col.is_key)
                .map(|col| col.key.as_str())
                .map(identifier_quote)
                .collect();

            if keys.len() > 0 {
                format!("PRIMARY INDEX {}", keys.join(","))
            } else {
                "".to_string()
            }
        } else {
            "".to_string()
        };

        let table_name = identifier_quote(&self.table.name);

        write!(
            f,
            "CREATE {} TABLE {} {} ({}) {} {};",
            self.table.r#type, if_not_exists, table_name, self.table.schema, indices, self.extra,
        )
    }
}

#[derive(Debug, PartialEq)]
pub struct DropTable<'a> {
    pub table: &'a Table,
}

impl<'a> Display for DropTable<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP TABLE {};", identifier_quote(&self.table.name))
    }
}

/// Query to insert from one table (source) into another (destination)
/// Assumes that all the fields in the destination are available in the source
#[derive(Debug, PartialEq)]
pub struct InsertFromTable<'a> {
    pub destination: &'a Table,
    pub source_name: &'a str,
}

impl<'a> Display for InsertFromTable<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let column_list = self
            .destination
            .schema
            .columns
            .iter()
            .map(|c| c.key.as_str())
            .map(identifier_quote)
            .join(",");

        let source_name = identifier_quote(&self.source_name);
        let destination_name = identifier_quote(&self.destination.name);

        write!(
            f,
            "INSERT INTO {} ({}) SELECT {} FROM {} WHERE $source_file_name IN (?) AND ((SELECT count(*) FROM {} WHERE $source_file_name IN (?)) < 1);",
            destination_name, column_list, column_list, source_name,
            destination_name
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::firebolt::firebolt_types::{Column, FireboltType, TableSchema, TableType};

    use super::*;

    #[test]
    fn test_create_table() {
        assert_eq!(
            CreateTable {
                table: &Table {
                    name: "test-table".to_string(),
                    schema: TableSchema {
                        columns: vec![
                            Column {
                                key: "str".to_string(),
                                r#type: FireboltType::Text,
                                nullable: false,
                                is_key: true,
                            },
                            Column {
                                key: "Int".to_string(),
                                r#type: FireboltType::Int,
                                nullable: true,
                                is_key: false,
                            },
                        ]
                    },
                    r#type: TableType::Fact,
                },
                if_not_exists: true,
                extra: ""
            }
            .to_string(),
            "CREATE FACT TABLE IF NOT EXISTS \"test-table\" (str TEXT,\"Int\" INT NULL) PRIMARY INDEX str ;"
        );

        assert_eq!(
            CreateTable {
                table: &Table {
                    name: "test_table".to_string(),
                    schema: TableSchema {
                        columns: vec![
                            Column {
                                key: "str".to_string(),
                                r#type: FireboltType::Text,
                                nullable: false,
                                is_key: true,
                            },
                            Column {
                                key: "Int".to_string(),
                                r#type: FireboltType::Int,
                                nullable: true,
                                is_key: true,
                            },
                        ]
                    },
                    r#type: TableType::Dimension,
                },
                if_not_exists: false,
                extra: ""
            }
            .to_string(),
            "CREATE DIMENSION TABLE  test_table (str TEXT,\"Int\" INT NULL) PRIMARY INDEX str,\"Int\" ;"
        );

        assert_eq!(
            CreateTable {
                table: &Table {
                    name: "test_table".to_string(),
                    schema: TableSchema {
                        columns: vec![Column {
                            key: "str".to_string(),
                            r#type: FireboltType::Text,
                            nullable: false,
                            is_key: true,
                        }, Column {
                            key: "Int".to_string(),
                            r#type: FireboltType::Int,
                            nullable: false,
                            is_key: true,
                        }]
                    },
                    r#type: TableType::External,
                },
                if_not_exists: false,
                extra: "CREDENTIALS = ( AWS_KEY_ID = '' AWS_SECRET_KEY = '' ) URL = '' OBJECT_PATTERN = ''"
            }
            .to_string(),
            "CREATE EXTERNAL TABLE  test_table (str TEXT,\"Int\" INT)  CREDENTIALS = ( AWS_KEY_ID = '' AWS_SECRET_KEY = '' ) URL = '' OBJECT_PATTERN = '';"
        );
    }

    #[test]
    fn test_drop_table() {
        assert_eq!(
            DropTable {
                table: &Table {
                    name: "test-table".to_string(),
                    schema: TableSchema { columns: vec![] },
                    r#type: TableType::Fact,
                }
            }
            .to_string(),
            "DROP TABLE \"test-table\";"
        );
    }

    #[test]
    fn test_insert_into() {
        assert_eq!(
            InsertFromTable {
                destination: &Table {
                    name: "destination-test".to_string(),
                    schema: TableSchema {
                        columns: vec![
                            Column {
                                key: "str".to_string(),
                                r#type: FireboltType::Text,
                                nullable: false,
                                is_key: true,
                            },
                            Column {
                                key: "Int".to_string(),
                                r#type: FireboltType::Int,
                                nullable: true,
                                is_key: false,
                            },
                        ]
                    },
                    r#type: TableType::Fact,
                },
                source_name: "source-test"
            }
            .to_string(),
            "INSERT INTO \"destination-test\" (str,\"Int\") SELECT str,\"Int\" FROM \"source-test\" WHERE $source_file_name IN (?) AND ((SELECT count(*) FROM \"destination-test\" WHERE $source_file_name IN (?)) < 1);"
        );
    }
}
