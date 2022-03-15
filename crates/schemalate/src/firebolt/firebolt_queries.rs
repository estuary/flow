use std::fmt::{self, Display};

use itertools::Itertools;

use crate::firebolt::firebolt_types::TableType;

use super::firebolt_types::Table;

#[derive(Debug, PartialEq)]
pub struct CreateTable {
    pub table: Table,
    pub if_not_exists: bool,
    /// Extra SQL string passed on table creation
    pub extra: String,
}

impl Display for CreateTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let if_not_exists = if self.if_not_exists {
            "IF NOT EXISTS"
        } else {
            ""
        };

        // External tables do not have indices
        let indices = if self.table.r#type != TableType::External {
            let keys: Vec<&str> = self
                .table
                .schema
                .columns
                .iter()
                .filter(|col| col.is_key)
                .map(|col| col.key.as_str())
                .collect();

            if keys.len() > 0 {
                format!("PRIMARY INDEX {}", keys.join(","))
            } else {
                "".to_string()
            }
        } else {
            "".to_string()
        };

        write!(
            f,
            "CREATE {} TABLE {} {} ({}) {} {};",
            self.table.r#type,
            if_not_exists,
            self.table.name,
            self.table.schema,
            indices,
            self.extra,
        )
    }
}

/// Query to insert from one table (source) into another (destination)
/// Assumes that all the fields in the destination are available in the source
#[derive(Debug, PartialEq)]
pub struct InsertFromTable {
    pub destination: Table,
    pub source_name: String,
}

impl Display for InsertFromTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let column_list = self
            .destination
            .schema
            .columns
            .iter()
            .map(|c| &c.key)
            .join(",");

        write!(
            f,
            "INSERT INTO {} ({}) SELECT {} FROM {} WHERE source_file_name IN (?);",
            self.destination.name, column_list, column_list, self.source_name
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
                table: Table {
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
                                key: "int".to_string(),
                                r#type: FireboltType::Int,
                                nullable: true,
                                is_key: false,
                            },
                        ]
                    },
                    r#type: TableType::Fact,
                },
                if_not_exists: true,
                extra: "".to_string()
            }
            .to_string(),
            "CREATE FACT TABLE IF NOT EXISTS test_table (str TEXT,int INT NULL) PRIMARY INDEX str ;"
        );

        assert_eq!(
            CreateTable {
                table: Table {
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
                                key: "int".to_string(),
                                r#type: FireboltType::Int,
                                nullable: true,
                                is_key: true,
                            },
                        ]
                    },
                    r#type: TableType::Dimension,
                },
                if_not_exists: false,
                extra: "".to_string()
            }
            .to_string(),
            "CREATE DIMENSION TABLE  test_table (str TEXT,int INT NULL) PRIMARY INDEX str,int ;"
        );

        assert_eq!(
            CreateTable {
                table: Table {
                    name: "test_table".to_string(),
                    schema: TableSchema {
                        columns: vec![Column {
                            key: "str".to_string(),
                            r#type: FireboltType::Text,
                            nullable: false,
                            is_key: true,
                        }]
                    },
                    r#type: TableType::External,
                },
                if_not_exists: false,
                extra: "CREDENTIALS = ( AWS_KEY_ID = '' AWS_SECRET_KEY = '' ) URL = '' OBJECT_PATTERN = ''".to_string()
            }
            .to_string(),
            "CREATE EXTERNAL TABLE  test_table (str TEXT)  CREDENTIALS = ( AWS_KEY_ID = '' AWS_SECRET_KEY = '' ) URL = '' OBJECT_PATTERN = '';"
        );
    }

    #[test]
    fn test_insert_into() {
        assert_eq!(
            InsertFromTable {
                destination: Table {
                    name: "destination_test".to_string(),
                    schema: TableSchema {
                        columns: vec![
                            Column {
                                key: "str".to_string(),
                                r#type: FireboltType::Text,
                                nullable: false,
                                is_key: true,
                            },
                            Column {
                                key: "int".to_string(),
                                r#type: FireboltType::Int,
                                nullable: true,
                                is_key: false,
                            },
                        ]
                    },
                    r#type: TableType::Fact,
                },
                source_name: "source_test".to_string()
            }
            .to_string(),
            "INSERT INTO destination_test (str,int) SELECT str,int FROM source_test WHERE source_file_name IN (?);"
        );
    }
}
