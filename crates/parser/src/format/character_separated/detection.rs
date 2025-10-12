use bytes::Bytes;
use std::cmp::Ordering;

use super::new_read_builder;
use crate::character_separated::{Delimiter, Escape, LineEnding, Quote};

/// Represents the relative score given to a specific dialect for a given sequence of bytes.
#[derive(Debug, Copy, Clone, Default)]
pub struct DetectionScore {
    /// The number of rows having > 0 delimiters appearing in them
    row_count: usize,
    /// The average number of delimiters appearing in _all_ rows (including those with 0 delimiters)
    mean_row_score: f64,
    /// The standard deviation of all the row scores (including those with 0 delimiters)
    row_score_stddev: f64,
}

impl DetectionScore {
    /// https://en.wikipedia.org/wiki/Coefficient_of_variation
    fn coefficient_of_variation(&self) -> f64 {
        if self.mean_row_score > 0.0 {
            self.row_score_stddev / self.mean_row_score
        } else {
            panic!("cannot compute coefficient_of_variation with 0 mean_column_count");
        }
    }
}

impl PartialEq for DetectionScore {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(std::cmp::Ordering::Equal)
    }
}

impl PartialOrd for DetectionScore {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Order first based on row count
        let row_cmp = self.row_count.cmp(&other.row_count);
        if !row_cmp.is_eq() {
            return Some(row_cmp);
        }

        if self.row_count == 0 {
            // if both have 0 row counts, then then other fields are irrelevant
            Some(Ordering::Equal)
        } else if self.mean_row_score == 0.0 || other.mean_row_score == 0.0 {
            // if either has a 0 mean row score, then the larger wins, or else they are equal
            self.mean_row_score.partial_cmp(&other.mean_row_score)
        } else {
            // Finally, use the reverse order comparison of coefficient of variation
            // (lower coefficient means it's a higher score)
            self.coefficient_of_variation()
                .partial_cmp(&other.coefficient_of_variation())
                .map(std::cmp::Ordering::reverse)
        }
    }
}

/// The detected dialect of a CSV file
#[derive(Debug)]
pub struct Dialect {
    pub quote: Quote,
    pub delimiter: Delimiter,
    /// The score that was computed for this dialect
    pub score: DetectionScore,
}

/// Tries to detect the dialect of a CSV file, based on a prefix of the file provided in `peeked`.
/// Only the delimiter and quote are currently detected, though we may wish to support detecting the
/// escape character in the future. The values from the configuration are passed in, and
/// `line_separator` and `escape` are required. If `config_quote` or `config_delimiter` are Some,
/// then the search space will be limited to only those values, and they will be returned in the
/// detected dialect.
/// The `header_count` parameter must be the number of header columns that was passed in the config,
/// and must be > 0 if it is `Some`. More typically, it will be `None`, and will be inferred from the
/// first row. This is needed because we consider it an error for any row to have more values than there
/// are headers.
/// A dialect is always detected and returned, even though it may not be a very good fit. This reflects
/// the reality that even an incorrect dialect can usually at least result in a single column per line.
pub fn detect_dialect(
    header_count: Option<usize>,
    line_separator: LineEnding,
    escape: Escape,
    peeked: Bytes,

    config_quote: Option<Quote>,
    config_delimiter: Option<Delimiter>,
) -> Dialect {
    let permutations = get_dialect_candidates(config_quote, config_delimiter);
    let mut dialects = permutations
        .iter()
        .copied()
        .map(|(quote, delimiter)| {
            let score = compute_score(
                header_count,
                peeked.clone(),
                quote,
                delimiter,
                line_separator,
                escape,
            );
            tracing::debug!(?quote, ?delimiter, ?score, "computed score for dialect");
            Dialect {
                quote,
                delimiter,
                score,
            }
        })
        .collect::<Vec<Dialect>>();

    dialects.sort_by(|l, r| {
        l.score
            .partial_cmp(&r.score)
            .expect("invalid dialect score")
            .then_with(|| sort_order(l.quote).cmp(&sort_order(r.quote)))
    });

    let winning_dialect = dialects
        .pop()
        .expect("must have at least one candidate dialect");
    // Log the top few candidates, as it's helpful to see the runner up when detection doesn't go as we expected
    let runners_up = &dialects[dialects.len().saturating_sub(3)..];

    tracing::debug!(
        ?winning_dialect,
        ?runners_up,
        total_checked_dialects = permutations.len(),
        "detected CSV dialect"
    );
    winning_dialect
}

/// When comparing dialects, we use the quote character to break ties in the scores.
/// This is because we detect the dialect based on a prefix of the CSV input, which
/// might not contain any quoted fields. (It's common for CSV writers to only quote
/// fields that need it.) So if we haven't seen any difference between dialect scores,
/// then we always want to prefer double quotes, as they're the most common. And disabled
/// quoting should always be last, since we might still see quote characters used later on.
fn sort_order(q: Quote) -> usize {
    match q {
        Quote::DoubleQuote => 2,
        Quote::SingleQuote => 1,
        Quote::None => 0,
    }
}

/// Returns a set of dialect options to use as candidates.
/// If either or both of `config_quote` or `config_delimiter` are Some,
/// then the returned candidates will all have that value. Otherwise, a
/// default set is used.
fn get_dialect_candidates(
    config_quote: Option<Quote>,
    config_delimiter: Option<Delimiter>,
) -> Vec<(Quote, Delimiter)> {
    use itertools::Itertools;

    let all_quotes: Vec<Quote> = if let Some(q) = config_quote {
        vec![q]
    } else {
        vec![Quote::DoubleQuote, Quote::SingleQuote, Quote::None]
    };
    let all_delims: Vec<Delimiter> = if let Some(d) = config_delimiter {
        vec![d]
    } else {
        vec![
            Delimiter::Comma,
            Delimiter::Pipe,
            Delimiter::Tab,
            Delimiter::Semicolon,
        ]
    };

    all_quotes
        .into_iter()
        .cartesian_product(all_delims)
        .collect()
}

fn compute_score(
    mut header_count: Option<usize>,
    peeked: Bytes,
    quote: Quote,
    delimiter: Delimiter,
    line_ending: LineEnding,
    escape: Escape,
) -> DetectionScore {
    use bytes::Buf;

    let mut builder = new_read_builder(line_ending, quote, delimiter, escape);
    builder.has_headers(false);
    let mut reader = builder.from_reader(peeked.reader());

    // Build a vec containing a count of the delimiters in each row. It's critical that we count
    // delimiter here instead of cells. Consider that _any_ candidate delimiter will work to parse
    // rows into a single column each. If we counted cells, we could easily end up with an obviously
    // incorrect (to humans) candidate delimiter that has a "perfect" score due to having a mean cell
    // count of 1 and a standard deviation of 0. Counting delimiters means that we give a 0 score to
    // each row that doesn't contain the delimiter.
    let mut row_scores = Vec::new();
    let mut record = csv::ByteRecord::new();
    // A count of rows having more than one column. Note that we still add a 0 to `row_scores` when
    // such a row is encountered, but we do not count them here. This count is used as the first-order
    // comparison of scores, and we want to give preference to dialects that result in a greater number
    // of rows that can actually be parsed correctly.
    let mut row_count = 0;
    while let Ok(more) = reader.read_byte_record(&mut record) {
        if !more {
            break;
        }
        let mut score = record.len().saturating_sub(1);
        if let Some(n_headers) = header_count {
            // It is an error for a CSV row to have more values than there are headers, so count this row
            // as an error in that case. Note that it's permissible for a row to have _fewer_ values.
            // This behavior matches that of `CsvOutput`, which returns an error if a row has too many values.
            if record.len() > n_headers {
                score = 0;
            }
        } else {
            // Consider the first row to be headers, and note the number so we can properly score subsequent rows.
            header_count = Some(record.len());
        }
        if score > 0 {
            row_count += 1;
        }
        row_scores.push(score);
    }

    let (mean_row_score, row_score_stddev) = if row_count > 0 {
        let n_rows = row_scores.len() as f64;
        let sum = row_scores.iter().copied().sum::<usize>();
        let mean = (sum as f64) / n_rows;
        let variance_sum = row_scores
            .iter()
            .map(|score| {
                let diff = mean - (*score as f64);
                diff * diff
            })
            .sum::<f64>();

        let stddev = (variance_sum / n_rows).sqrt();
        (mean, stddev)
    } else {
        (0.0, 0.0)
    };
    DetectionScore {
        row_count,
        mean_row_score,
        row_score_stddev,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn detection_score_comparison() {
        let test_cases = &[
            // Ordering is done first based on row count
            ((4, 30.5, 3.5), (5, 30.5, 3.5), Some(Ordering::Less)),
            ((99, 25.0, 12.0), (1, 1.0, 0.0), Some(Ordering::Greater)),
            ((2, 0.0, 0.0), (1, 0.0, 0.0), Some(Ordering::Greater)),
            ((0, 0.0, 0.0), (1, 1.0, 0.0), Some(Ordering::Less)),
            // If row counts are equal, reverse order based on ratio of stddev/mean
            ((4, 30.0, 3.0), (4, 30.0, 3.0), Some(Ordering::Equal)),
            ((4, 30.0, 3.0), (4, 30.0, 3.0), Some(Ordering::Equal)),
            ((4, 3.0, 0.0), (4, 90.0, 14.5), Some(Ordering::Greater)),
            // Except for special cases when either mean row score is 0
            ((0, 0.0, 0.0), (0, 0.0, 0.0), Some(Ordering::Equal)),
            ((2, 0.0, 0.0), (2, 0.0, 0.0), Some(Ordering::Equal)),
            ((2, 0.0, 0.0), (2, 0.01, 1.0), Some(Ordering::Less)),
        ];

        for (a_tuple, b_tuple, expected_ord) in test_cases.into_iter() {
            let a = DetectionScore {
                row_count: a_tuple.0,
                mean_row_score: a_tuple.1,
                row_score_stddev: a_tuple.2,
            };
            let b = DetectionScore {
                row_count: b_tuple.0,
                mean_row_score: b_tuple.1,
                row_score_stddev: b_tuple.2,
            };
            let actual = a.partial_cmp(&b);
            assert_eq!(
                *expected_ord, actual,
                "a: {a:?}, b: {b:?}, expected: {expected_ord:?}, actual: {actual:?}"
            );
            let expected_rev = expected_ord.map(std::cmp::Ordering::reverse);
            let actual = b.partial_cmp(&a);
            assert_eq!(
                expected_rev, actual,
                "reflexive case: a: {a:?}, b: {b:?}, expected: {expected_rev:?}, actual: {actual:?}"
            );
        }
    }

    #[test]
    fn account_for_header_count_when_scoring() {
        let input = Bytes::from_static(b"a;b;c;d\n'a;b;c';d;'e;f;g';h");
        let sq_score = compute_score(
            None,
            input.clone(),
            Quote::SingleQuote,
            Delimiter::Semicolon,
            LineEnding::CRLF,
            Escape::None,
        );
        let dq_score = compute_score(
            None,
            input.clone(),
            Quote::DoubleQuote,
            Delimiter::Semicolon,
            LineEnding::CRLF,
            Escape::None,
        );

        assert!(sq_score > dq_score);
        assert_eq!(2, sq_score.row_count);
        assert_eq!(1, dq_score.row_count);

        let sq_score = compute_score(
            Some(9),
            input.clone(),
            Quote::SingleQuote,
            Delimiter::Semicolon,
            LineEnding::CRLF,
            Escape::None,
        );
        let dq_score = compute_score(
            Some(9),
            input.clone(),
            Quote::DoubleQuote,
            Delimiter::Semicolon,
            LineEnding::CRLF,
            Escape::None,
        );
        // With 9 headers, the double quote dialect should result in 2 rows
        assert_eq!(2, dq_score.row_count);
        // But the single quote dialect should still have a higher score due to greater consistency
        assert!(sq_score > dq_score);
    }

    #[test]
    fn dialect_detection() {
        #[derive(Debug, PartialEq, serde::Deserialize)]
        struct DetectionResult {
            quote: Quote,
            delimiter: Delimiter,
        }

        for result in std::fs::read_dir(crate::test::path(
            "src/format/character_separated/detection_cases",
        ))
        .unwrap()
        {
            let entry = result.unwrap();

            let filename = entry.file_name();
            let path = entry.path();
            let content = bytes::Bytes::from(std::fs::read(path).unwrap());

            // The first line of each file must be a json object with the expected detection results
            let newline_idx = content.iter().position(|b| *b == b'\n').unwrap();
            let expect_json = content.slice(0..newline_idx);
            let csv = content.slice((newline_idx + 1)..);

            let expected: DetectionResult = serde_json::from_slice(&expect_json)
                .expect("failed to deserialize expected detection result");

            let dialect = detect_dialect(None, LineEnding::CRLF, Escape::None, csv, None, None);
            let actual = DetectionResult {
                quote: dialect.quote,
                delimiter: dialect.delimiter,
            };
            let score = dialect.score;

            assert_eq!(
                expected, actual,
                "detection failed for '{filename:?}', actual score was {score:?}"
            );
        }
    }
}
