use serde::Serialize;
use serde_json::Value;
use std::io::{self, Write};

#[derive(clap::Args, Clone, Debug, Default)]
pub struct Output {
    #[clap(global = true, short, long, value_enum)]
    pub output: Option<OutputType>,
}

#[derive(clap::ValueEnum, Debug, Copy, Clone, PartialEq)]
pub enum OutputType {
    /// Format output as compact JSON with items separated by newlines
    Json,
    /// Format output as YAML
    Yaml,
    /// Format the output as a prett-printed table
    Table,
}

pub trait CliOutput: Serialize {
    type TableAlt: Copy;
    type CellValue: Into<comfy_table::Cell>;

    fn table_headers(alt: Self::TableAlt) -> Vec<&'static str>;

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

pub fn to_table_row<T: Serialize>(value: T, columns: &[&str]) -> Vec<JsonCell> {
    let mut json = serde_json::to_value(value).expect("failed to serialize json");

    let mut row = Vec::with_capacity(columns.len());
    for column in columns {
        let val = json.pointer_mut(column).map(Value::take);
        row.push(JsonCell(val));
    }
    row
}

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
