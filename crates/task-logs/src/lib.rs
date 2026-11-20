//! Server-side scans of a task's ops logs journal.
//!
//! A scan reads the journal in one direction, running a compiled [`Filter`]
//! over each batch of lines in a tight loop. It stops upon matching a number
//! of lines, upon reading a budget of bytes or time, or upon passing a time
//! bound of its Filter, and returns an offset from which a later scan continues.
//!
//! A scan plans its reads from a listing of the journal's fragments.
//! Persisted fragments are gzip and must be decompressed from their
//! beginning, so a scan reads each one in a single pass, and reads several
//! of them at once. Fragments which aren't yet persisted are served by
//! brokers from local files, which a Backward scan reads in windows.

mod matcher;
mod scan;

pub use matcher::{Filter, Level, Line};
pub use scan::{Direction, Limits, Page, Stats, Stop, scan};
