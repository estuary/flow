use super::{ParseError, ParseResult};
use crate::ErrorThreshold;
use std::collections::VecDeque;

// How many of the recent records to consider when trying to decide if
// we've entered a new region of bad data of the file.
const ERROR_BUFFER_WINDOW_SIZE: usize = 1000;

/// A decorating iterator that tracks parsing errors and absorbs a specified
/// rate of errors. If that rate is exceeded, then all the errors encountered
/// are returned. Should not be polled again once an error is returned.
#[derive(Debug)]
pub struct ParseErrorBuffer<I> {
    /// The iterator we're wrapping.
    inner: I,
    /// The amount of errors we can absorb before halting parsing.
    threshold: ErrorThreshold,
    /// The number of records we've seen.
    total_records: usize,
    /// TODO
    errors_in_buffer: usize,
    /// The most recent rows.
    buffer: VecDeque<ParseResult>,
}

impl<I: Iterator<Item = ParseResult>> ParseErrorBuffer<I> {
    pub fn new(inner: I, threshold: ErrorThreshold) -> Self {
        Self {
            inner,
            threshold,
            total_records: 0,
            errors_in_buffer: 0,
            buffer: VecDeque::with_capacity(ERROR_BUFFER_WINDOW_SIZE),
        }
    }

    /// Consumes the next item out of the inner iterator and pops the next item
    /// out of the internal buffer.
    pub fn advance(&mut self) -> Option<I::Item> {
        let popped = self.buffer.pop_front();
        self.buffer_next();
        if let Some(Err(_)) = popped {
            self.errors_in_buffer -= 1;
        }
        popped
    }

    /// Returns true when the error buffer contains too many errors.
    pub fn exceeded(&self) -> bool {
        if self.total_records == 0 || self.errors_in_buffer == 0 {
            return false;
        }

        // If the whole file is smaller than the window size, we only want to
        // consider the records we have when determining the file's error rate.
        // Otherwise, we'll use the window size for this calculation.
        let window_size = usize::min(self.total_records, ERROR_BUFFER_WINDOW_SIZE);
        let error_rate = self.errors_in_buffer as f64 / window_size as f64;

        self.threshold.exceeded((100.0 * error_rate) as u8)
    }

    /// Fill up the internal buffer with as many items as we can.
    pub fn prefill_buffer(&mut self) {
        while self.buffer.len() < ERROR_BUFFER_WINDOW_SIZE && self.buffer_next() {
            // Continue buffering
        }
    }

    /// Returns true if we successfully added another item to the buffer.
    fn buffer_next(&mut self) -> bool {
        if let Some(item) = self.inner.next() {
            if item.is_err() {
                self.errors_in_buffer += 1;
            }
            self.total_records += 1;
            self.buffer.push_back(item);
            true
        } else {
            false
        }
    }
}

impl<I: Iterator<Item = ParseResult>> Iterator for ParseErrorBuffer<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.prefill_buffer();

        loop {
            if self.exceeded() {
                return Some(Err(ParseError::ErrorLimitExceeded(self.threshold)));
            } else {
                let item = self.advance()?;
                if item.is_ok() {
                    return Some(item);
                } else {
                    tracing::warn!(error=?item.unwrap_err(), "failed to parse row");
                    continue;
                }
            }
        }
    }
}
