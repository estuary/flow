//! Generators for parquet test fixtures, written to caller-owned paths so
//! nothing is checked into git.

// Compiled both standalone and via `#[path]` include; not every generator is
// used from every inclusion.
#![allow(dead_code)]

use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, Int64Builder,
    ListBuilder, StringBuilder, StructBuilder, TimestampMicrosecondBuilder,
    TimestampNanosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, LogicalType, Repetition, Type as PhysicalType};
use parquet::data_type::{ByteArray, ByteArrayType, Int64Type, Int96, Int96Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::Type as SchemaType;
use parquet_variant::VariantBuilder;
use parquet_variant_json::JsonToVariant;

/// Compression codec to apply to written row groups.
#[derive(Debug, Clone, Copy)]
pub enum Codec {
    Uncompressed,
    Snappy,
    Gzip,
    Zstd,
}

impl Codec {
    fn to_parquet(self) -> Compression {
        match self {
            Codec::Uncompressed => Compression::UNCOMPRESSED,
            Codec::Snappy => Compression::SNAPPY,
            Codec::Gzip => Compression::GZIP(Default::default()),
            Codec::Zstd => Compression::ZSTD(Default::default()),
        }
    }
}

/// Schema covering the edge cases the parser must handle identically:
/// nullable primitives, logical types (timestamp/decimal/date), binary-free
/// nesting (struct), and repeated fields (list).
fn rich_schema() -> Arc<Schema> {
    let nested_fields = Fields::from(vec![
        Field::new("inner_int", DataType::Int64, true),
        Field::new("inner_str", DataType::Utf8, true),
    ]);
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("maybe_int", DataType::Int64, true),
        Field::new("ratio", DataType::Float64, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("flag", DataType::Boolean, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            true,
        ),
        Field::new("day", DataType::Date32, true),
        Field::new("amount", DataType::Decimal128(20, 4), true),
        Field::new("nested", DataType::Struct(nested_fields.clone()), true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
    ]))
}

/// Builds a single record batch of `rows` rows following [`rich_schema`], with
/// deterministic values and interspersed nulls so that repeated invocations
/// produce identical bytes.
fn rich_batch(schema: &Arc<Schema>, rows: usize, row_offset: usize) -> RecordBatch {
    let mut id = Int64Builder::new();
    let mut maybe_int = Int64Builder::new();
    let mut ratio = Float64Builder::new();
    let mut label = StringBuilder::new();
    let mut flag = BooleanBuilder::new();
    let mut ts = TimestampMicrosecondBuilder::new().with_timezone("+00:00");
    let mut day = Date32Builder::new();
    let mut amount = Decimal128Builder::new()
        .with_precision_and_scale(20, 4)
        .unwrap();

    let nested_fields = match schema.field_with_name("nested").unwrap().data_type() {
        DataType::Struct(f) => f.clone(),
        _ => unreachable!(),
    };
    let mut nested = StructBuilder::new(
        nested_fields,
        vec![Box::new(Int64Builder::new()), Box::new(StringBuilder::new())],
    );
    let mut tags = ListBuilder::new(Int64Builder::new());

    for i in row_offset..(row_offset + rows) {
        id.append_value(i as i64);

        // Every 7th row is null, to exercise definition levels.
        if i % 7 == 0 {
            maybe_int.append_null();
        } else {
            maybe_int.append_value((i as i64) * 3 - 1);
        }

        if i % 5 == 0 {
            ratio.append_null();
        } else {
            ratio.append_value((i as f64) / 4.0);
        }

        if i % 3 == 0 {
            label.append_null();
        } else {
            label.append_value(format!("row-{}", i));
        }

        flag.append_value(i % 2 == 0);

        // 1_700_000_000 seconds is 2023-11-14T22:13:20Z; add per-row micros.
        ts.append_value(1_700_000_000_000_000 + (i as i64) * 1_000_000);

        // Days since epoch; ~2021-05 onward.
        day.append_value(18_800 + (i as i32 % 900));

        if i % 11 == 0 {
            amount.append_null();
        } else {
            // Scale 4: store value * 10_000.
            amount.append_value((i as i128) * 12_345 + 6_789);
        }

        let nested_int = nested
            .field_builder::<Int64Builder>(0)
            .unwrap();
        if i % 4 == 0 {
            nested_int.append_null();
        } else {
            nested_int.append_value((i as i64) + 100);
        }
        let nested_str = nested.field_builder::<StringBuilder>(1).unwrap();
        nested_str.append_value(format!("n{}", i % 13));
        // Whole struct is null every 9th row.
        nested.append(i % 9 != 0);

        if i % 6 == 0 {
            tags.append_null();
        } else {
            let values = tags.values();
            for k in 0..(i % 4) {
                values.append_value((i * 10 + k) as i64);
            }
            tags.append(true);
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(id.finish()),
        Arc::new(maybe_int.finish()),
        Arc::new(ratio.finish()),
        Arc::new(label.finish()),
        Arc::new(flag.finish()),
        Arc::new(ts.finish()),
        Arc::new(day.finish()),
        Arc::new(amount.finish()),
        Arc::new(nested.finish()),
        Arc::new(tags.finish()),
    ];
    RecordBatch::try_new(schema.clone(), arrays).unwrap()
}

/// Writes a parquet file with the rich edge-case schema to `path`, split into
/// row groups of `rows_per_group` and compressed with `codec`.
pub fn write_rich_parquet(
    path: &std::path::Path,
    total_rows: usize,
    rows_per_group: usize,
    codec: Codec,
) {
    let schema = rich_schema();
    let props = WriterProperties::builder()
        .set_compression(codec.to_parquet())
        .set_max_row_group_size(rows_per_group)
        .build();

    let file = std::fs::File::create(path).expect("create fixture file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    let mut written = 0;
    while written < total_rows {
        let n = rows_per_group.min(total_rows - written);
        let batch = rich_batch(&schema, n, written);
        writer.write(&batch).unwrap();
        written += n;
    }
    writer.close().unwrap();
}

/// Writes a nanosecond-precision timestamp column. These have no legacy
/// converted type, so the record API renders them as raw integers.
pub fn write_timestamp_nanos_parquet(path: &std::path::Path, total_rows: usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "ts_nanos",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            true,
        ),
    ]));
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let file = std::fs::File::create(path).expect("create fixture file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    let mut id = Int64Builder::new();
    let mut ts = TimestampNanosecondBuilder::new().with_timezone("+00:00");
    for i in 0..total_rows {
        id.append_value(i as i64);
        if i % 4 == 0 {
            ts.append_null();
        } else {
            ts.append_value(1_700_000_000_000_000_000 + (i as i64) * 1_000_000_000);
        }
    }
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id.finish()), Arc::new(ts.finish())],
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// JSON documents encoded, one per row (cycled), into the VARIANT fixture.
/// Covers objects, arrays, nesting, and scalars so the decode is exercised
/// across variant value kinds.
pub const VARIANT_JSON_SAMPLES: &[&str] = &[
    r#"{"name":"acme","active":true,"count":3}"#,
    r#"[1,2,3,"four",null]"#,
    r#"{"nested":{"a":1,"b":[true,false]},"tags":["x","y"]}"#,
    r#"42"#,
    r#""just a string""#,
    r#"{"mixed":[{"k":1},{"k":2}],"f":1.5,"n":null}"#,
];

/// Writes a parquet file whose `v` column is an unshredded VARIANT group
/// (`required group v (VARIANT) { required binary metadata; required binary
/// value }`), with values encoding [`VARIANT_JSON_SAMPLES`] in row order. Uses
/// the low-level writer because arrow cannot emit VARIANT groups.
pub fn write_variant_parquet(path: &std::path::Path, total_rows: usize) {
    let metadata_field = SchemaType::primitive_type_builder("metadata", PhysicalType::BYTE_ARRAY)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();
    let value_field = SchemaType::primitive_type_builder("value", PhysicalType::BYTE_ARRAY)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();
    let variant_group = SchemaType::group_type_builder("v")
        .with_repetition(Repetition::REQUIRED)
        .with_logical_type(Some(LogicalType::Variant))
        .with_fields(vec![Arc::new(metadata_field), Arc::new(value_field)])
        .build()
        .unwrap();
    let id_field = SchemaType::primitive_type_builder("id", PhysicalType::INT64)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();
    let schema = Arc::new(
        SchemaType::group_type_builder("schema")
            .with_fields(vec![Arc::new(id_field), Arc::new(variant_group)])
            .build()
            .unwrap(),
    );

    // Encode each row's JSON sample into (metadata, value) variant buffers.
    let mut metadatas = Vec::with_capacity(total_rows);
    let mut values = Vec::with_capacity(total_rows);
    for i in 0..total_rows {
        let mut builder = VariantBuilder::new();
        builder
            .append_json(VARIANT_JSON_SAMPLES[i % VARIANT_JSON_SAMPLES.len()])
            .unwrap();
        let (metadata, value) = builder.finish();
        metadatas.push(ByteArray::from(metadata));
        values.push(ByteArray::from(value));
    }
    let ids: Vec<i64> = (0..total_rows as i64).collect();

    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
    );
    let file = std::fs::File::create(path).expect("create fixture file");
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    let mut rg = writer.next_row_group().unwrap();

    // Leaf columns are written in order: id, v.metadata, v.value.
    let mut col = rg.next_column().unwrap().unwrap();
    col.typed::<Int64Type>().write_batch(&ids, None, None).unwrap();
    col.close().unwrap();
    let mut col = rg.next_column().unwrap().unwrap();
    col.typed::<ByteArrayType>().write_batch(&metadatas, None, None).unwrap();
    col.close().unwrap();
    let mut col = rg.next_column().unwrap().unwrap();
    col.typed::<ByteArrayType>().write_batch(&values, None, None).unwrap();
    col.close().unwrap();

    rg.close().unwrap();
    writer.close().unwrap();
}

/// Matches `parquet::data_type`'s JULIAN_DAY_OF_EPOCH.
const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;

fn int96_from(days_since_epoch: i64, nanos_of_day: i64) -> Int96 {
    let julian = (days_since_epoch + JULIAN_DAY_OF_EPOCH) as u64;
    let nanos = nanos_of_day as u64;
    let mut v = Int96::new();
    // value = [nanos_of_day low 32, nanos_of_day high 32, julian day].
    v.set_data(
        (nanos & 0xFFFF_FFFF) as u32,
        (nanos >> 32) as u32,
        julian as u32,
    );
    v
}

/// Writes a parquet file with a legacy INT96 timestamp column using the
/// low-level writer (arrow cannot emit INT96). Values deliberately include a
/// pre-epoch date and sub-millisecond components to exercise the negative-day
/// and truncation edges of the INT96 -> millisecond reconstruction.
pub fn write_int96_timestamp_parquet(path: &std::path::Path, total_rows: usize) {
    let message_type = "
        message schema {
            REQUIRED INT64 id;
            OPTIONAL INT96 ts;
        }
    ";
    let schema = Arc::new(parse_message_type(message_type).unwrap());
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
    );
    let file = std::fs::File::create(path).expect("create fixture file");
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();

    // (days since epoch, nanoseconds within the day). Covers epoch, pre-epoch,
    // and assorted sub-second values.
    let samples: [(i64, i64); 5] = [
        (19_675, 12 * 3_600 * 1_000_000_000), // 2023-11-14 12:00:00
        (0, 0),                               // 1970-01-01 00:00:00
        (-1, 500_000),                        // 1969-12-31 00:00:00.0005ms
        (-365, 3_600 * 1_000_000_000),        // ~1969-01-01 01:00:00
        (100, 123_456_789),                   // 1970-04-11 00:00:00.123456789
    ];

    let mut rg = writer.next_row_group().unwrap();

    // Column 0: id (REQUIRED INT64).
    let mut col = rg.next_column().unwrap().unwrap();
    let ids: Vec<i64> = (0..total_rows as i64).collect();
    col.typed::<Int64Type>().write_batch(&ids, None, None).unwrap();
    col.close().unwrap();

    // Column 1: ts (OPTIONAL INT96) with every 5th value null.
    let mut col = rg.next_column().unwrap().unwrap();
    let mut values = Vec::new();
    let mut def_levels = Vec::with_capacity(total_rows);
    for i in 0..total_rows {
        if i % 5 == 0 {
            def_levels.push(0);
        } else {
            def_levels.push(1);
            let (days, nanos) = samples[i % samples.len()];
            values.push(int96_from(days, nanos));
        }
    }
    col.typed::<Int96Type>()
        .write_batch(&values, Some(&def_levels), None)
        .unwrap();
    col.close().unwrap();

    rg.close().unwrap();
    writer.close().unwrap();
}

/// Writes a simple wide-ish numeric/string parquet suitable for throughput
/// benchmarking: no nested types, so per-row conversion cost dominates.
pub fn write_flat_parquet(
    path: &std::path::Path,
    total_rows: usize,
    rows_per_group: usize,
    codec: Codec,
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Float64, true),
        Field::new("d", DataType::Utf8, true),
        Field::new("e", DataType::Boolean, false),
        Field::new(
            "f",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            true,
        ),
    ]));
    let props = WriterProperties::builder()
        .set_compression(codec.to_parquet())
        .set_max_row_group_size(rows_per_group)
        .build();

    let file = std::fs::File::create(path).expect("create fixture file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    let mut written = 0;
    while written < total_rows {
        let n = rows_per_group.min(total_rows - written);

        let mut a = Int64Builder::new();
        let mut b = Int64Builder::new();
        let mut c = Float64Builder::new();
        let mut d = StringBuilder::new();
        let mut e = BooleanBuilder::new();
        let mut f = TimestampMicrosecondBuilder::new().with_timezone("+00:00");
        for i in written..(written + n) {
            a.append_value(i as i64);
            if i % 8 == 0 {
                b.append_null();
            } else {
                b.append_value((i as i64) << 1);
            }
            if i % 5 == 0 {
                c.append_null();
            } else {
                c.append_value((i as f64) * 1.5);
            }
            if i % 3 == 0 {
                d.append_null();
            } else {
                d.append_value(format!("val-{}", i));
            }
            e.append_value(i % 2 == 0);
            f.append_value(1_700_000_000_000_000 + (i as i64) * 250_000);
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(a.finish()),
                Arc::new(b.finish()),
                Arc::new(c.finish()),
                Arc::new(d.finish()),
                Arc::new(e.finish()),
                Arc::new(f.finish()),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        written += n;
    }
    writer.close().unwrap();
}
