//! Parser for the parquet format. This will accept any stream of parquet files, limited up to 1GB
//! in size.
use super::{Input, Output, ParseError, Parser};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::reader::RowIter;
use std::convert::TryFrom;
use serde_json::Value;

struct ParquetParser;

pub fn new_parser() -> Box<dyn Parser> {
    Box::new(ParquetParser)
}

// Maximum size for row groups (1GB)
const MAX_RG_SIZE: i64 = 1024 * 1024 * 1024;

impl Parser for ParquetParser {
    fn parse(&self, content: Input) -> Result<Output, ParseError> {
        let file = content.into_file()?;
        let file_reader = SerializedFileReader::try_from(file)?;
    
        for rg in file_reader.metadata().row_groups() {
            if rg.total_byte_size() > MAX_RG_SIZE {
                return Err(ParseError::RowGroupTooLarge)
            }
        }

        let iter = file_reader.into_iter();

        let wrapped = ParquetIter {
            inner: Box::new(iter),
        };
        Ok(Box::new(wrapped))
    }
}

struct ParquetIter<'a> {
    inner: Box<RowIter<'a>>,
}

impl Iterator for ParquetIter<'_> {
    type Item = Result<Value, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next_elem = self.inner.next()?;
        match next_elem {
            Ok(row) => Some(Ok(row.to_json_value())),
            Err(e) => Some(Err(e.into())),
        }
    }
}


#[cfg(test)]
mod test {
    use std::fs::File;

    use super::*;
    use serde_json::json;

    fn input_for_file(rel_path: impl AsRef<std::path::Path>) -> Input {
        let file = File::open(rel_path).expect("failed to open file");
        Input::File(file)
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
        assert_eq!(json!({
            "petal.length": 1.4,
            "petal.width": 0.2,
            "sepal.length": 5.1,
            "sepal.width": 3.5,
            "variety": "Setosa"
        }), first);
        let second = output
            .next()
            .expect("expected a result")
            .expect("must parse object Ok");
        assert_eq!(json!({
            "petal.length": 1.4,
            "petal.width": 0.2,
            "sepal.length": 4.9,
            "sepal.width": 3.0,
            "variety": "Setosa"
        }), second);

        // 50 total items
        assert_eq!(output.count(), 148);
    }

    /* The tests below have been run on TLC Trip Record Data, January 2024
       Yellow Taxi Trip Records and For-Hire Vehicle Trip Records
       They have been commented due to the file sizes of these datasets
       It is recommended to run these tests against these files when changing the behavior
       of the parser
       Specifically download the two files for January 2024 and uncomment these tests:
       https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

    #[test]
    fn parse_sample_file_fhv() {
        let input = input_for_file("tests/examples/fhv_tripdata.parquet");
        let mut output = ParquetParser
            .parse(input)
            .expect("must return output iterator");

        let first = output
            .next()
            .expect("expected a result")
            .expect("must parse object Ok");
        assert_eq!(json!({
            "Affiliated_base_number": "B00014",
            "DOlocationID": null,
            "PUlocationID": null,
            "SR_Flag": null,
            "dispatching_base_num": "B00053",
            "dropOff_datetime": "2024-01-01 02:13:00 +00:00",
            "pickup_datetime": "2024-01-01 00:15:00 +00:00",
        }), first);

        // 50 total items
        assert_eq!(output.count(), 1290115);
    }

    #[test]
    fn parse_sample_file_yellow() {
        let input = input_for_file("tests/examples/yellow_tripdata.parquet");
        let mut output = ParquetParser
            .parse(input)
            .expect("must return output iterator");

        let first = output
            .next()
            .expect("expected a result")
            .expect("must parse object Ok");
        assert_eq!(json!({
            "Airport_fee": 0.0,
            "DOLocationID": 79,
            "PULocationID": 186,
            "RatecodeID": 1,
            "VendorID": 2,
            "congestion_surcharge": 2.5,
            "extra": 1.0,
            "fare_amount": 17.7,
            "improvement_surcharge": 1.0,
            "mta_tax": 0.5,
            "passenger_count": 1,
            "payment_type": 2,
            "store_and_fwd_flag": "N",
            "tip_amount": 0.0,
            "tolls_amount": 0.0,
            "total_amount": 22.7,
            "tpep_dropoff_datetime": "2024-01-01 01:17:43 +00:00",
            "tpep_pickup_datetime": "2024-01-01 00:57:55 +00:00",
            "trip_distance": 1.72
        }), first);

        // 50 total items
        assert_eq!(output.count(), 2964623);
    }*/
}
