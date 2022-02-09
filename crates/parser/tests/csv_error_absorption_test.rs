mod testutil;

use std::io::{Cursor, Seek, Write};

use parser::{csv, ErrorThreshold, Input, ParseConfig};
use testutil::run_test;

#[test]
fn no_tolerance_for_errors_test() {
    let mut csv = EphemeralCsv::default();
    let config = csv.parse_config(None);

    csv.write_rows(1, &|_i| "n,n_squared".to_owned());
    csv.write_rows(500, &|i| format!("{},{}", i, i * i));
    run_test(&config, csv.as_input()).assert_success(500);

    csv.write_rows(1, &|i| format!("{},{},unexpected-column!", i, i * i));
    // Still outputs 500 rows and fails on 501st, because no buffering is done when the error
    // threshold is unset.
    run_test(&config, csv.as_input()).assert_failure(500);
}

#[test]
fn smaller_than_window_test() {
    let mut csv = EphemeralCsv::default();
    let config = csv.parse_config(ErrorThreshold::new(10).unwrap().into());

    csv.write_rows(1, &|_i| "n,n_squared".to_owned());
    csv.write_rows(500, &|i| format!("{},{}", i, i * i));
    csv.write_rows(55, &|i| format!("{},{},unexpected-column!", i, i * i));
    // 555 total rows seen, 55 / 555 = 9.9% => still okay
    run_test(&config, csv.as_input()).assert_success(500);

    csv.write_rows(1, &|i| format!("{},{},unexpected-column!", i, i * i));
    // 556 total rows seen, 56 / 556 = 10.0% => too high!
    run_test(&config, csv.as_input()).assert_failure(0);
}

#[test]
fn larger_than_window_test() {
    let mut csv = EphemeralCsv::default();
    let config = csv.parse_config(ErrorThreshold::new(10).unwrap().into());

    csv.write_rows(1, &|_i| "n,n_squared".to_owned());
    csv.write_rows(1000, &|i| format!("{},{}", i, i * i));
    csv.write_rows(99, &|i| format!("{},{},unexpected-column!", i, i * i));
    // More than 1000 records seen, so only consider the last 1000.
    // 99 / 1000 = 9.9% => still okay
    run_test(&config, csv.as_input()).assert_success(1000);

    csv.write_rows(1, &|i| format!("{},{},unexpected-column!", i, i * i));
    // 100 / 1000 = 10.0% => too high, but the first 100 good rows have already been output
    run_test(&config, csv.as_input()).assert_failure(100);
}

#[test]
fn occasional_bad_data_test() {
    let mut csv = EphemeralCsv::default();
    let config = csv.parse_config(ErrorThreshold::new(25).unwrap().into());

    csv.write_rows(1, &|_i| "n,n_squared".to_owned());

    // We'll write every 5th row as bad data. This is a 20% error rate, under
    // our 25% threshold, so we'll succeed and output 800 good rows.
    csv.write_rows(1000, &|i| {
        if i % 5 == 0 {
            format!("{},{},unexpected-column!", i, i * i)
        } else {
            format!("{},{}", i, i * i)
        }
    });
    run_test(&config, csv.as_input()).assert_success(800);

    // Increase the error rate to 100%. 200 errors are in the buffer. 250 will
    // cross the threshold. As we write these Bad rows, Good rows from the first
    // 1000 will fall out of the other side of the buffer, but so will some of
    // the old Bad rows. If we want to end up with 25% of 1000 inside the
    // window, we'll need to account for 1/5 of the old errors dropping out.
    // 200 + 60 - 12 = 248 + 3 = 251. 63 new Bad rows put us over the threshold.
    csv.write_rows(63, &|i| format!("{},{},unexpected-column!", i, i * i));

    // However, the front of the queue is still filled with 80% Good rows to
    // output. As we slide the buffer window, 4/5 rows are Good and will be
    // output despite reading only Bad rows. 1063 total rows were processed
    // before the threshold is exceeded. Of the first 63 that might have been
    // output, 63 * 80% = 50 would be Good rows that were output.
    run_test(&config, csv.as_input()).assert_failure(50);
}

#[derive(Debug)]
struct EphemeralCsv {
    data: Cursor<Vec<u8>>,
}

impl Default for EphemeralCsv {
    fn default() -> Self {
        Self {
            data: Cursor::new(Vec::default()),
        }
    }
}

impl EphemeralCsv {
    fn write_rows<'c>(&mut self, n: usize, row_generator: &'c dyn Fn(usize) -> String) {
        for i in 0..n {
            let content = row_generator(i);
            self.data
                .write(content.as_bytes())
                .expect("to write to buffer");
            self.data.write(b"\n").expect("to write newline to buffer");
        }
    }

    fn parse_config(&self, error_threshold: Option<ErrorThreshold>) -> ParseConfig {
        ParseConfig {
            filename: Some("data.csv".to_owned()),
            csv: Some(csv::CharacterSeparatedConfig {
                error_threshold,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn as_input(&self) -> Input {
        let mut data = self.data.clone();
        data.rewind().expect("to replay from the beginning");
        Input::Stream(Box::new(data))
    }
}
