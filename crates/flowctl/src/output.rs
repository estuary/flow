use serde::Serialize;
use serde_json::Value;
use std::io::{self, Write};

#[derive(clap::Args, Clone, Debug, Default)]
pub struct Output {
    /// How to format CLI output
    #[clap(global = true, short, long, value_enum)]
    pub output: Option<OutputType>,
}

#[derive(clap::ValueEnum, Debug, Copy, Clone, PartialEq)]
pub enum OutputType {
    /// Format output as compact JSON with items separated by newlines
    Json,
    /// Format output as YAML
    Yaml,
    /// Format the output as a pretty-printed table
    Table,
}

/// A trait for things that can be output from the CLI as either JSON, YAML, or a table.
/// The body of this trait is focused on table output, since JSON and YAML are both handled
/// by `Serialize`.
pub trait CliOutput: Serialize {
    /// Allows threading through an alternate representation of table output. An example is
    /// the `--flows` option of `flowctl catalog list`, which adds additional columns to the table.
    /// `type TableAlt = ();` is used to opt out of having an alternative representation.
    type TableAlt: Copy;
    /// The type output from `into_table_rows`. Common types are `String` and `JsonCell`, or
    /// anything implementing `std::fmt::Display`.
    type CellValue: Into<comfy_table::Cell>;

    /// Returns the column headers of the table.
    fn table_headers(alt: Self::TableAlt) -> Vec<&'static str>;

    /// Converts this item into a tablular representation. The returned cells must be in the
    /// same order as the `table_headers`.
    fn into_table_row(self, alt: Self::TableAlt) -> Vec<Self::CellValue>;
}

pub fn print_yaml(items: impl IntoIterator<Item = impl CliOutput>) -> anyhow::Result<()> {
    let mut stdout = io::stdout().lock();
    for item in items {
        serde_yaml::to_writer(&mut stdout, &item)?;
        stdout.write_all(b"\n")?;
    }
    Ok(())
}

pub fn print_json(items: impl IntoIterator<Item = impl CliOutput>) -> anyhow::Result<()> {
    let mut stdout = io::stdout().lock();
    for item in items {
        serde_json::to_writer(&mut stdout, &item)?;
        stdout.write_all(b"\n")?;
    }
    Ok(())
}

pub fn print_table<T: CliOutput>(
    alt: T::TableAlt,
    items: impl IntoIterator<Item = T>,
) -> anyhow::Result<()> {
    let mut stdout = io::stdout().lock();
    let headers = T::table_headers(alt);

    let mut table = crate::new_table(headers);

    for item in items {
        table.add_row(item.into_table_row(alt));
    }

    for line in table.lines() {
        stdout.write_all(line.as_bytes())?;
        stdout.write_all(b"\n")?;
    }
    Ok(())
}

/// Converts an item implementing `Serialize` into a table row by extracting values
/// using the given list of JSON `pointers`. This function is often used to implement
/// `CliOutput::into_table_row`.
pub fn to_table_row<T: Serialize>(value: T, pointers: &[&str]) -> Vec<JsonCell> {
    let mut json = serde_json::to_value(value).expect("failed to serialize json");

    let mut row = Vec::with_capacity(pointers.len());
    for column in pointers {
        let val = json.pointer_mut(column).map(Value::take);
        row.push(JsonCell(val));
    }
    row
}

/// A wrapper around an `Option<Value>` to allow it to be converted into a table cell.
pub struct JsonCell(pub Option<Value>);

impl Into<comfy_table::Cell> for JsonCell {
    fn into(self) -> comfy_table::Cell {
        match self.0 {
            None => comfy_table::Cell::new(String::new()),
            Some(Value::String(s)) => comfy_table::Cell::new(s),
            Some(other) => comfy_table::Cell::new(other),
        }
    }
}
