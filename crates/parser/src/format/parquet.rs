//! Parser for the parquet format, limited to 1GB per row group.
//!
//! Files are decoded through parquet's Arrow reader ([`ParquetRecordBatchReader`]),
//! converting each [`arrow_array::RecordBatch`] to one JSON object per row. Output
//! is byte-for-byte identical to parquet's record API (decimals and timestamps as
//! strings, binary as base64, non-finite floats as null); [`leaf_to_value`]
//! reproduces that formatting and the tests below assert the equality.
//!
//! Legacy INT96 timestamps and INT64 nanosecond timestamps both decode to Arrow's
//! `Timestamp(Nanosecond)`, but the record API renders INT96 as a millisecond
//! string ([`int96_nanos_to_millis`]) and INT64 nanoseconds as a raw integer.
//! Since the Arrow type cannot distinguish them, [`build_column_plans`] consults
//! the parquet leaf schema to build a per-column [`ColPlan`].
//!
//! Arrow types not on the [`datatype_supported`] allow-list fall back to the
//! record API. No type the parquet reader produces today hits this path; it is a
//! defensive backstop.
//!
//! The one intentional departure from the record API is the VARIANT logical type
//! ([`variant_to_value`]): the record API emits base64 of the raw variant binary,
//! while this decodes it to the JSON value it encodes.
use super::{Input, Output, ParseError, Parser};
use arrow_array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Decimal128Array,
    Decimal256Array, FixedSizeBinaryArray, Float16Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeListArray, LargeStringArray,
    ListArray, MapArray, RecordBatch, StringArray, StringViewArray, StructArray,
    Time32MillisecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, TimeUnit};
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use chrono::{TimeZone, Utc};
use num_bigint::{BigInt, Sign};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::basic::{LogicalType, Type as PhysicalType};
use parquet::file::reader::SerializedFileReader;
use parquet::record::reader::RowIter;
use parquet::schema::types::{SchemaDescriptor, Type as SchemaType};
use parquet_variant::Variant;
use parquet_variant_json::VariantToJson;
use serde_json::Value;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::sync::Arc;

// Included as a module rather than an integration test so the tests below can
// exercise the private decode path directly.
#[cfg(test)]
#[path = "../../tests/parquet_gen.rs"]
mod parquet_gen;

struct ParquetParser;

pub fn new_parser() -> Box<dyn Parser> {
    Box::new(ParquetParser)
}

const MAX_RG_SIZE: i64 = 1024 * 1024 * 1024;

// Bounds peak memory independent of file size.
const BATCH_SIZE: usize = 8192;

impl Parser for ParquetParser {
    fn parse(&self, content: Input) -> Result<Output, ParseError> {
        let file = content.into_file()?;

        // Cloned so `file` stays available for the record-API fallback below.
        let builder = ParquetRecordBatchReaderBuilder::try_new(file.try_clone()?)?;

        for rg in builder.metadata().row_groups() {
            if rg.total_byte_size() > MAX_RG_SIZE {
                return Err(ParseError::RowGroupTooLarge);
            }
        }

        let arrow_supported = builder
            .schema()
            .fields()
            .iter()
            .all(|f| datatype_supported(f.data_type()));

        if arrow_supported {
            let plans = build_column_plans(
                builder.schema(),
                builder.metadata().file_metadata().schema_descr(),
            );
            let reader = builder.with_batch_size(BATCH_SIZE).build()?;
            drop(file);
            Ok(Box::new(ArrowRowIter::new(reader, Arc::new(plans))))
        } else {
            drop(builder);
            let file_reader = SerializedFileReader::try_from(file)?;
            let iter = file_reader.into_iter();
            Ok(Box::new(RecordRowIter {
                inner: Box::new(iter),
            }))
        }
    }
}

/// Record-API fallback, matching the parser's prior output exactly.
struct RecordRowIter {
    inner: Box<RowIter<'static>>,
}

impl Iterator for RecordRowIter {
    type Item = Result<Value, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next()? {
            Ok(row) => Some(Ok(row.to_json_value())),
            Err(e) => Some(Err(e.into())),
        }
    }
}

/// Yields rows by converting a whole record batch, then draining it.
struct ArrowRowIter {
    reader: ParquetRecordBatchReader,
    plans: Arc<Vec<ColPlan>>,
    buffered: std::vec::IntoIter<Result<Value, ParseError>>,
}

impl ArrowRowIter {
    fn new(reader: ParquetRecordBatchReader, plans: Arc<Vec<ColPlan>>) -> Self {
        ArrowRowIter {
            reader,
            plans,
            buffered: Vec::new().into_iter(),
        }
    }
}

impl Iterator for ArrowRowIter {
    type Item = Result<Value, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(result) = self.buffered.next() {
                return Some(result);
            }
            match self.reader.next()? {
                Ok(batch) => self.buffered = convert_batch(&batch, &self.plans).into_iter(),
                Err(e) => return Some(Err(e.into())),
            }
        }
    }
}

/// Carries the one distinction the Arrow schema loses: whether a
/// `Timestamp(Nanosecond)` column came from a legacy INT96 (record API renders a
/// millisecond string) or an INT64 nanosecond column (record API renders a raw
/// integer).
enum ColPlan {
    Leaf,
    Int96Millis,
    /// An (unshredded) VARIANT group: decode `{metadata, value}` binary into the
    /// JSON value it encodes. Unlike every other plan, this intentionally departs
    /// from the record API, which cannot decode variant and emits base64 blobs.
    Variant,
    Struct(Vec<ColPlan>),
    List(Box<ColPlan>),
    Map(Box<ColPlan>, Box<ColPlan>),
}

/// Walks the Arrow fields in lock-step with the parquet leaf columns (same
/// depth-first order), and scans the parquet schema for VARIANT-annotated groups,
/// which the Arrow schema shows as a plain struct.
fn build_column_plans(schema: &arrow_schema::Schema, parquet: &SchemaDescriptor) -> Vec<ColPlan> {
    let mut variant_paths = HashSet::new();
    collect_variant_paths(parquet.root_schema(), "", &mut variant_paths);

    let mut leaves = parquet.columns().iter().map(|c| c.physical_type());
    schema
        .fields()
        .iter()
        .map(|f| build_plan(f.data_type(), f.name(), &variant_paths, &mut leaves))
        .collect()
}

/// Records the dotted paths of every group annotated with the VARIANT logical
/// type. Variant internals are not descended into.
fn collect_variant_paths(group: &SchemaType, prefix: &str, out: &mut HashSet<String>) {
    for field in group.get_fields() {
        let path = if prefix.is_empty() {
            field.name().to_string()
        } else {
            format!("{prefix}.{}", field.name())
        };
        if !field.is_group() {
            continue;
        }
        if matches!(field.get_basic_info().logical_type(), Some(LogicalType::Variant)) {
            out.insert(path);
        } else {
            collect_variant_paths(field, &path, out);
        }
    }
}

fn build_plan(
    data_type: &DataType,
    path: &str,
    variant_paths: &HashSet<String>,
    leaves: &mut impl Iterator<Item = PhysicalType>,
) -> ColPlan {
    match data_type {
        // A VARIANT group reads as a `Struct{metadata, value}`. Detect it by the
        // parquet annotation plus that exact unshredded shape; consume its two
        // leaf columns to keep the leaf iterator aligned. Shredded variants (with
        // a `typed_value`) are not yet handled and fall through to a plain struct.
        DataType::Struct(fields)
            if variant_paths.contains(path) && is_unshredded_variant(fields) =>
        {
            leaves.next();
            leaves.next();
            ColPlan::Variant
        }
        DataType::Struct(fields) => ColPlan::Struct(
            fields
                .iter()
                .map(|f| build_plan(f.data_type(), &child_path(path, f.name()), variant_paths, leaves))
                .collect(),
        ),
        DataType::List(field) | DataType::LargeList(field) => ColPlan::List(Box::new(build_plan(
            field.data_type(),
            &child_path(path, field.name()),
            variant_paths,
            leaves,
        ))),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(kv) if kv.len() == 2 => {
                let key = build_plan(kv[0].data_type(), path, variant_paths, leaves);
                let value = build_plan(kv[1].data_type(), path, variant_paths, leaves);
                ColPlan::Map(Box::new(key), Box::new(value))
            }
            _ => {
                leaves.next();
                ColPlan::Leaf
            }
        },
        // Only a nanosecond timestamp's rendering depends on the physical type.
        DataType::Timestamp(TimeUnit::Nanosecond, _) => match leaves.next() {
            Some(PhysicalType::INT96) => ColPlan::Int96Millis,
            _ => ColPlan::Leaf,
        },
        _ => {
            leaves.next();
            ColPlan::Leaf
        }
    }
}

fn child_path(prefix: &str, name: &str) -> String {
    format!("{prefix}.{name}")
}

/// The canonical unshredded VARIANT layout: exactly a `metadata` and a `value`
/// binary field.
fn is_unshredded_variant(fields: &arrow_schema::Fields) -> bool {
    fields.len() == 2
        && fields.iter().any(|f| f.name() == "metadata" && is_binary(f.data_type()))
        && fields.iter().any(|f| f.name() == "value" && is_binary(f.data_type()))
}

fn is_binary(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Binary | DataType::LargeBinary | DataType::BinaryView)
}

/// Column order does not affect output: `serde_json` serializes object keys
/// sorted. A row failing to convert (only VARIANT can) becomes an `Err` in its
/// slot, matching how the record path surfaces per-row errors.
fn convert_batch(batch: &RecordBatch, plans: &[ColPlan]) -> Vec<Result<Value, ParseError>> {
    let schema = batch.schema();
    let columns = batch.columns();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut object = serde_json::Map::with_capacity(columns.len());
        let mut error = None;
        for ((name, column), plan) in names.iter().zip(columns.iter()).zip(plans.iter()) {
            match array_to_value(column.as_ref(), row, plan) {
                Ok(value) => {
                    object.insert((*name).to_string(), value);
                }
                Err(e) => {
                    error = Some(e);
                    break;
                }
            }
        }
        rows.push(error.map_or_else(|| Ok(Value::Object(object)), Err));
    }
    rows
}

fn array_to_value(array: &dyn Array, row: usize, plan: &ColPlan) -> Result<Value, ParseError> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }

    match plan {
        ColPlan::Leaf => Ok(leaf_to_value(array, row)),

        ColPlan::Int96Millis => {
            let nanos = downcast::<TimestampNanosecondArray>(array).value(row);
            Ok(Value::String(convert_timestamp_millis_to_string(int96_nanos_to_millis(nanos))))
        }

        ColPlan::Variant => variant_to_value(array, row),

        ColPlan::Struct(child_plans) => {
            let structs = downcast::<StructArray>(array);
            let fields = match array.data_type() {
                DataType::Struct(fields) => fields,
                _ => unreachable!("Struct plan on a non-struct array"),
            };
            let mut object = serde_json::Map::with_capacity(fields.len());
            for ((field, column), child) in fields
                .iter()
                .zip(structs.columns().iter())
                .zip(child_plans.iter())
            {
                object.insert(field.name().clone(), array_to_value(column.as_ref(), row, child)?);
            }
            Ok(Value::Object(object))
        }

        ColPlan::List(child) => {
            let elements = match array.data_type() {
                DataType::List(_) => downcast::<ListArray>(array).value(row),
                DataType::LargeList(_) => downcast::<LargeListArray>(array).value(row),
                other => unreachable!("List plan on a {:?} array", other),
            };
            list_to_value(elements.as_ref(), child)
        }

        ColPlan::Map(key_plan, value_plan) => {
            let map = downcast::<MapArray>(array);
            let offsets = map.value_offsets();
            let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
            let keys = map.keys();
            let values = map.values();
            let mut object = serde_json::Map::with_capacity(end - start);
            for i in start..end {
                let key = array_to_value(keys.as_ref(), i, key_plan)?;
                // Match the record API: string keys are used verbatim, other
                // key types fall back to their JSON string representation.
                let key = key
                    .as_str()
                    .map(|s| s.to_owned())
                    .unwrap_or_else(|| key.to_string());
                object.insert(key, array_to_value(values.as_ref(), i, value_plan)?);
            }
            Ok(Value::Object(object))
        }
    }
}

/// Decodes an unshredded VARIANT cell into the JSON value it encodes. This is
/// the one conversion with no record-API equivalent: the record path emits
/// base64 of the raw variant binary.
fn variant_to_value(array: &dyn Array, row: usize) -> Result<Value, ParseError> {
    let structs = downcast::<StructArray>(array);
    let metadata = binary_field(structs, "metadata", row);
    let value = binary_field(structs, "value", row);

    let variant = Variant::try_new(metadata, value)?;
    let mut buffer = Vec::new();
    variant.to_json(&mut buffer)?;
    Ok(serde_json::from_slice(&buffer)?)
}

/// The variant metadata/value fields are required, so a null is read as empty.
fn binary_field<'a>(structs: &'a StructArray, name: &str, row: usize) -> &'a [u8] {
    let column = structs
        .column_by_name(name)
        .expect("variant struct must have the named binary field");
    if column.is_null(row) {
        return &[];
    }
    let column = column.as_ref();
    match column.data_type() {
        DataType::Binary => downcast::<BinaryArray>(column).value(row),
        DataType::LargeBinary => downcast::<LargeBinaryArray>(column).value(row),
        DataType::BinaryView => downcast::<BinaryViewArray>(column).value(row),
        other => unreachable!("variant {name} field is {other:?}, expected binary"),
    }
}

/// Converts a non-null scalar cell. Panics on types not cleared by
/// [`datatype_supported`], which `parse` guarantees cannot reach here.
fn leaf_to_value(array: &dyn Array, row: usize) -> Value {
    match array.data_type() {
        DataType::Null => Value::Null,
        DataType::Boolean => Value::Bool(downcast::<BooleanArray>(array).value(row)),

        DataType::Int8 => Value::Number(downcast::<Int8Array>(array).value(row).into()),
        DataType::Int16 => Value::Number(downcast::<Int16Array>(array).value(row).into()),
        DataType::Int32 => Value::Number(downcast::<Int32Array>(array).value(row).into()),
        DataType::Int64 => Value::Number(downcast::<Int64Array>(array).value(row).into()),
        DataType::UInt8 => Value::Number(downcast::<UInt8Array>(array).value(row).into()),
        DataType::UInt16 => Value::Number(downcast::<UInt16Array>(array).value(row).into()),
        DataType::UInt32 => Value::Number(downcast::<UInt32Array>(array).value(row).into()),
        DataType::UInt64 => Value::Number(downcast::<UInt64Array>(array).value(row).into()),

        DataType::Float16 => {
            number_from_f64(f64::from(downcast::<Float16Array>(array).value(row)))
        }
        DataType::Float32 => {
            number_from_f64(f64::from(downcast::<Float32Array>(array).value(row)))
        }
        DataType::Float64 => number_from_f64(downcast::<Float64Array>(array).value(row)),

        DataType::Utf8 => Value::String(downcast::<StringArray>(array).value(row).to_owned()),
        DataType::LargeUtf8 => {
            Value::String(downcast::<LargeStringArray>(array).value(row).to_owned())
        }
        DataType::Utf8View => {
            Value::String(downcast::<StringViewArray>(array).value(row).to_owned())
        }

        DataType::Binary => {
            Value::String(BASE64_STANDARD.encode(downcast::<BinaryArray>(array).value(row)))
        }
        DataType::LargeBinary => {
            Value::String(BASE64_STANDARD.encode(downcast::<LargeBinaryArray>(array).value(row)))
        }
        DataType::BinaryView => {
            Value::String(BASE64_STANDARD.encode(downcast::<BinaryViewArray>(array).value(row)))
        }
        DataType::FixedSizeBinary(_) => Value::String(
            BASE64_STANDARD.encode(downcast::<FixedSizeBinaryArray>(array).value(row)),
        ),

        DataType::Date32 => {
            Value::String(convert_date_to_string(downcast::<Date32Array>(array).value(row)))
        }
        DataType::Time32(TimeUnit::Millisecond) => Value::String(convert_time_millis_to_string(
            downcast::<Time32MillisecondArray>(array).value(row),
        )),
        DataType::Time64(TimeUnit::Microsecond) => Value::String(convert_time_micros_to_string(
            downcast::<Time64MicrosecondArray>(array).value(row),
        )),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Value::String(
            convert_timestamp_millis_to_string(
                downcast::<TimestampMillisecondArray>(array).value(row),
            ),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Value::String(
            convert_timestamp_micros_to_string(
                downcast::<TimestampMicrosecondArray>(array).value(row),
            ),
        ),

        // INT64 nanosecond time/timestamp columns have no legacy converted type,
        // so the record API surfaces them as raw integers (INT96 is handled by
        // ColPlan::Int96Millis before reaching here).
        DataType::Time64(TimeUnit::Nanosecond) => {
            Value::Number(downcast::<Time64NanosecondArray>(array).value(row).into())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Value::Number(downcast::<TimestampNanosecondArray>(array).value(row).into())
        }

        DataType::Decimal128(_, scale) => {
            let value = downcast::<Decimal128Array>(array).value(row);
            Value::String(decimal_to_string(&value.to_be_bytes(), *scale as i32))
        }
        DataType::Decimal256(_, scale) => {
            let value = downcast::<Decimal256Array>(array).value(row);
            Value::String(decimal_to_string(&value.to_be_bytes(), *scale as i32))
        }

        other => unreachable!(
            "leaf_to_value reached an unsupported data type {:?}; the schema check in parse \
             should have routed this file to the record API",
            other
        ),
    }
}

fn list_to_value(elements: &dyn Array, plan: &ColPlan) -> Result<Value, ParseError> {
    let values = (0..elements.len())
        .map(|i| array_to_value(elements, i, plan))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Value::Array(values))
}

const NANOS_IN_DAY: i64 = 24 * 60 * 60 * 1_000_000_000;
const MILLIS_IN_DAY: i64 = 24 * 60 * 60 * 1_000;

/// Reproduces `Int96::to_millis()` from the nanoseconds Arrow computed via
/// `Int96::to_nanos()`. `to_millis` floors the day component and adds a
/// non-negative intra-day remainder, so a plain `nanos / 1_000_000` diverges for
/// pre-epoch values (Rust integer division truncates toward zero). Euclidean
/// division recovers the exact split, matching `to_millis()` across the full
/// i64-nanosecond range.
fn int96_nanos_to_millis(nanos: i64) -> i64 {
    let days = nanos.div_euclid(NANOS_IN_DAY);
    let nanos_of_day = nanos.rem_euclid(NANOS_IN_DAY);
    days * MILLIS_IN_DAY + nanos_of_day / 1_000_000
}

fn downcast<T: 'static>(array: &dyn Array) -> &T {
    array
        .as_any()
        .downcast_ref::<T>()
        .expect("arrow array downcast matched its DataType")
}

/// Non-finite floats (NaN, infinities) have no JSON number form; the record API
/// maps them to null.
fn number_from_f64(value: f64) -> Value {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

/// Allow-list of Arrow types whose conversion is verified equal to the record
/// API; anything else falls back to the record API in `parse`.
fn datatype_supported(data_type: &DataType) -> bool {
    match data_type {
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_)
        | DataType::Date32
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => true,

        // Millisecond/microsecond units render as strings via legacy converted
        // types. Nanosecond units are also supported: an INT64 nanosecond column
        // renders as a raw integer (via `leaf_to_value`), and an INT96 column is
        // handled through `ColPlan::Int96Millis`. Second units cannot originate
        // from a parquet file, so they are left out.
        DataType::Time32(TimeUnit::Millisecond)
        | DataType::Time64(TimeUnit::Microsecond)
        | DataType::Time64(TimeUnit::Nanosecond)
        | DataType::Timestamp(TimeUnit::Millisecond, _)
        | DataType::Timestamp(TimeUnit::Microsecond, _)
        | DataType::Timestamp(TimeUnit::Nanosecond, _) => true,

        DataType::Struct(fields) => fields.iter().all(|f| datatype_supported(f.data_type())),
        DataType::List(field) | DataType::LargeList(field) => {
            datatype_supported(field.data_type())
        }
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) => fields.iter().all(|f| datatype_supported(f.data_type())),
            _ => false,
        },

        _ => false,
    }
}

// The functions below are exact reimplementations of the private
// `convert_*_to_string` helpers in `parquet::record::api`. They share the same
// chrono version as `parquet`, so identical calls yield identical output. Any
// change here must be checked against the differential test.

fn convert_date_to_string(value: i32) -> String {
    const NUM_SECONDS_IN_DAY: i64 = 60 * 60 * 24;
    let dt = Utc
        .timestamp_opt(value as i64 * NUM_SECONDS_IN_DAY, 0)
        .unwrap();
    format!("{}", dt.format("%Y-%m-%d"))
}

fn convert_timestamp_millis_to_string(value: i64) -> String {
    let dt = Utc.timestamp_millis_opt(value).unwrap();
    format!("{}", dt.format("%Y-%m-%d %H:%M:%S%.3f %:z"))
}

fn convert_timestamp_micros_to_string(value: i64) -> String {
    let dt = Utc.timestamp_micros(value).unwrap();
    format!("{}", dt.format("%Y-%m-%d %H:%M:%S%.6f %:z"))
}

fn convert_time_millis_to_string(value: i32) -> String {
    let total_ms = value as u64;
    let hours = total_ms / (60 * 60 * 1000);
    let minutes = (total_ms % (60 * 60 * 1000)) / (60 * 1000);
    let seconds = (total_ms % (60 * 1000)) / 1000;
    let millis = total_ms % 1000;
    format!("{hours:02}:{minutes:02}:{seconds:02}.{millis:03}")
}

fn convert_time_micros_to_string(value: i64) -> String {
    let total_us = value as u64;
    let hours = total_us / (60 * 60 * 1000 * 1000);
    let minutes = (total_us % (60 * 60 * 1000 * 1000)) / (60 * 1000 * 1000);
    let seconds = (total_us % (60 * 1000 * 1000)) / (1000 * 1000);
    let micros = total_us % (1000 * 1000);
    format!("{hours:02}:{minutes:02}:{seconds:02}.{micros:06}")
}

/// Reimplementation of `convert_decimal_to_string`. `data` is the two's-complement
/// big-endian value; leading sign-extension bytes do not change the result, so a
/// wider representation (e.g. i128 bytes for a value stored in fewer parquet
/// bytes) produces the identical string.
fn decimal_to_string(data: &[u8], scale: i32) -> String {
    let num = BigInt::from_signed_bytes_be(data);

    let negative = i32::from(num.sign() == Sign::Minus);
    let mut num_str = num.to_string();
    let mut point = num_str.len() as i32 - scale - negative;

    if point <= 0 {
        while point < 0 {
            num_str.insert(negative as usize, '0');
            point += 1;
        }
        num_str.insert_str(negative as usize, "0.");
    } else {
        num_str.insert((point + negative) as usize, '.');
    }

    num_str
}

#[cfg(test)]
mod test {
    use std::fs::File;

    use super::*;
    use serde_json::json;

    use super::parquet_gen::{
        Codec, VARIANT_JSON_SAMPLES, write_flat_parquet, write_int96_timestamp_parquet,
        write_rich_parquet, write_timestamp_nanos_parquet, write_variant_parquet,
    };

    fn input_for_file(rel_path: impl AsRef<std::path::Path>) -> Input {
        let file = File::open(rel_path).expect("failed to open file");
        Input::File(file)
    }

    /// Reference output: the record-API path (`RowIter` + `to_json_value`) that
    /// the parser used before the Arrow fast path existed.
    fn record_reference(path: &std::path::Path) -> Vec<Value> {
        let file = File::open(path).unwrap();
        let reader = SerializedFileReader::try_from(file).unwrap();
        reader
            .into_iter()
            .map(|r| r.expect("record row").to_json_value())
            .collect()
    }

    /// Actual output: the full `ParquetParser`, which selects the Arrow path for
    /// schemas cleared by `datatype_supported`.
    fn parser_output(path: &std::path::Path) -> Vec<Value> {
        let input = Input::File(File::open(path).unwrap());
        ParquetParser
            .parse(input)
            .expect("parser output")
            .map(|r| r.expect("parsed row"))
            .collect()
    }

    /// Nanosecond (INT64) timestamps now decode via the Arrow fast path and must
    /// match the record API, which — lacking a legacy converted type — renders
    /// them as raw integers.
    #[test]
    fn nanosecond_timestamps_match_record_api() {
        let dir = tempdir::TempDir::new("pq-nanos").unwrap();
        let path = dir.path().join("nanos.parquet");
        write_timestamp_nanos_parquet(&path, 1_000);

        assert_uses_arrow_path(&path);
        assert_eq!(
            record_reference(&path),
            parser_output(&path),
            "nanosecond output must match the record API"
        );

        // The record API renders nanosecond timestamps as raw integers; confirm
        // the Arrow path preserves that rather than formatting a string.
        let ts = parser_output(&path)
            .iter()
            .find_map(|row| row.get("ts_nanos").filter(|v| !v.is_null()).cloned())
            .expect("expected a non-null timestamp");
        assert!(
            ts.is_i64() || ts.is_u64(),
            "nanosecond timestamp should be a raw integer, got {ts}"
        );
    }

    /// Legacy INT96 timestamps now decode via the Arrow fast path. The record API
    /// renders them as millisecond strings via `Int96::to_millis()`; the Arrow
    /// path must reproduce that exactly, including for the pre-epoch sample.
    #[test]
    fn int96_timestamps_match_record_api() {
        let dir = tempdir::TempDir::new("pq-int96").unwrap();
        let path = dir.path().join("int96.parquet");
        write_int96_timestamp_parquet(&path, 1_000);

        assert_uses_arrow_path(&path);

        let reference = record_reference(&path);
        let actual = parser_output(&path);
        assert_eq!(reference.len(), actual.len());
        for (i, (r, a)) in reference.iter().zip(actual.iter()).enumerate() {
            assert_eq!(r, a, "INT96 mismatch at row {i}\n  record: {r}\n  arrow:  {a}");
        }

        // Confirm we actually rendered a timestamp string (not an integer), and
        // that the pre-epoch sample is present and correctly formatted.
        let strings: Vec<&str> = actual
            .iter()
            .filter_map(|row| row.get("ts").and_then(|v| v.as_str()))
            .collect();
        assert!(
            strings.iter().any(|s| s.starts_with("1969-12-31")),
            "expected the pre-epoch INT96 sample to render as a 1969 timestamp"
        );
    }

    /// VARIANT columns decode to the JSON they encode (a deliberate departure
    /// from the record API, which emits base64 of the raw variant binary). This
    /// is a golden round-trip: JSON -> variant -> parquet -> parser -> JSON.
    #[test]
    fn variant_decodes_to_json() {
        let dir = tempdir::TempDir::new("pq-variant").unwrap();
        let path = dir.path().join("variant.parquet");
        let rows = 600;
        write_variant_parquet(&path, rows);

        // The variant group reads as Struct{metadata,value} of binary, which is
        // Arrow-eligible, so this exercises the fast path.
        assert_uses_arrow_path(&path);

        let actual = parser_output(&path);
        assert_eq!(actual.len(), rows);
        for (i, row) in actual.iter().enumerate() {
            let expected: Value =
                serde_json::from_str(VARIANT_JSON_SAMPLES[i % VARIANT_JSON_SAMPLES.len()]).unwrap();
            assert_eq!(
                row.get("v"),
                Some(&expected),
                "variant row {i} should decode to its source JSON, got {row}"
            );
            // The id column still decodes normally alongside the variant.
            assert_eq!(row.get("id"), Some(&Value::Number((i as i64).into())));
        }
    }

    /// The Arrow allow-list must still reject genuinely unsupported Arrow types,
    /// routing them to the record fallback. Durations are not producible by the
    /// parquet reader, but guard the decision directly.
    #[test]
    fn unsupported_datatype_is_rejected() {
        assert!(!datatype_supported(&DataType::Duration(TimeUnit::Second)));
        assert!(!datatype_supported(&DataType::Timestamp(TimeUnit::Second, None)));
        assert!(!datatype_supported(&DataType::Struct(
            vec![arrow_schema::Field::new(
                "d",
                DataType::Duration(TimeUnit::Second),
                true,
            )]
            .into()
        )));
    }

    fn assert_uses_arrow_path(path: &std::path::Path) {
        let file = File::open(path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        assert!(
            builder
                .schema()
                .fields()
                .iter()
                .all(|f| datatype_supported(f.data_type())),
            "fixture schema must exercise the Arrow path, not the record fallback"
        );
    }

    /// For every supported schema and compression codec, the Arrow path emits the
    /// same JSON documents and the same serialized bytes as the record API.
    #[test]
    fn arrow_path_matches_record_api() {
        let dir = tempdir::TempDir::new("pq-diff").unwrap();
        let codecs = [Codec::Uncompressed, Codec::Snappy, Codec::Gzip, Codec::Zstd];

        for codec in codecs {
            for (name, writer) in [
                (
                    "rich",
                    &write_rich_parquet as &dyn Fn(&std::path::Path, usize, usize, Codec),
                ),
                ("flat", &write_flat_parquet),
            ] {
                let path = dir.path().join(format!("{name}-{codec:?}.parquet"));
                // Multiple row groups (rows_per_group < total) to exercise batch
                // boundaries crossing row groups.
                writer(&path, 5_000, 900, codec);

                assert_uses_arrow_path(&path);

                let reference = record_reference(&path);
                let actual = parser_output(&path);

                assert_eq!(
                    reference.len(),
                    actual.len(),
                    "row count mismatch for {name}/{codec:?}"
                );
                for (i, (r, a)) in reference.iter().zip(actual.iter()).enumerate() {
                    assert_eq!(
                        r, a,
                        "value mismatch at row {i} for {name}/{codec:?}\n  record: {r}\n  arrow:  {a}"
                    );
                    assert_eq!(
                        serde_json::to_string(r).unwrap(),
                        serde_json::to_string(a).unwrap(),
                        "serialized mismatch at row {i} for {name}/{codec:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn parse_sample_file_iris() {
        let input = input_for_file("tests/examples/iris.parquet");
        let mut output = ParquetParser
            .parse(input)
            .expect("must return output iterator");

        let first = output
            .next()
            .expect("expected a result")
            .expect("must parse object Ok");
        assert_eq!(
            json!({
                "petal.length": 1.4,
                "petal.width": 0.2,
                "sepal.length": 5.1,
                "sepal.width": 3.5,
                "variety": "Setosa"
            }),
            first
        );
        let second = output
            .next()
            .expect("expected a result")
            .expect("must parse object Ok");
        assert_eq!(
            json!({
                "petal.length": 1.4,
                "petal.width": 0.2,
                "sepal.length": 4.9,
                "sepal.width": 3.0,
                "variety": "Setosa"
            }),
            second
        );

        // 50 total items
        assert_eq!(output.count(), 148);
    }
}
