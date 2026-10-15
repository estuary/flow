//! Parser for XML documents. Elements are converted into JSON using the following rules:
//!
//! - An element with only text content (or no content at all) becomes a JSON string.
//! - An element with attributes or child elements becomes a JSON object. Attributes are added
//!   as `@name` properties, child elements as properties named after the element, and any text
//!   content as a `#text` property.
//! - A child element that occurs more than once becomes a JSON array. Elements named in
//!   `arrayElements` are always arrays, so that documents keep a consistent shape regardless of
//!   how many occurrences a particular file happens to contain. An element that occurs once in
//!   some places and repeatedly in others, and isn't named in `arrayElements`, is logged as a
//!   warning so the user can add it.
//! - All values are strings. Types are never inferred, since doing so would corrupt values such
//!   as postal codes and identifiers with leading zeros.
//! - Namespace prefixes are removed from element and attribute names, and namespace
//!   declarations are dropped.
//! - Leading and trailing whitespace is trimmed from text, except for whitespace within CDATA
//!   sections or produced by character references. When text is interleaved with child elements
//!   (mixed content), the text segments are joined with a single space.
//! - Comments, processing instructions, and DOCTYPE declarations are ignored. Only the predefined
//!   XML entities and character references are expanded. Any other entity reference is an error,
//!   which means that external entities are never resolved and entity expansion cannot be abused.
//!
//! By default, the root element of each file is emitted as a single document. If a record path is
//! configured, then each element matching that path is emitted as a separate document, and only
//! one record is held in memory at a time. Either way, each document is an object with a single
//! property named after the emitted element, such as `{"Order": {...}}`, so that the same
//! element has the same shape whether it's a whole file or one record of a batch.
use super::{Input, Output, ParseError, Parser};
use crate::config::EncodingRef;
use crate::config::xml::XmlConfig;
use crate::input::detect_encoding;
use quick_xml::escape::resolve_predefined_entity;
use quick_xml::events::{BytesRef, BytesStart, Event};
use quick_xml::{Reader, XmlVersion};
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::io;

/// Maximum depth of nested elements. Deeper documents are rejected rather than risking excessive
/// resource usage on adversarial input.
const MAX_DEPTH: usize = 256;

/// Number of bytes to inspect when looking for a byte order mark or an encoding declaration.
const PROLOG_PEEK_BYTES: usize = 1024;

const BUFFER_SIZE: usize = 64 * 1024;

/// Property name used for the text content of elements that also have attributes or children.
const TEXT_KEY: &str = "#text";

/// Prefix of property names used for attributes.
const ATTRIBUTE_PREFIX: char = '@';

pub fn new_parser(config: XmlConfig) -> Box<dyn Parser> {
    Box::new(XmlParser { config })
}

struct XmlParser {
    config: XmlConfig,
}

#[derive(Debug, thiserror::Error)]
pub enum XmlError {
    #[error(
        "invalid recordPath '{0}': must be a path of element names separated by '/', such as '/Records/Record'"
    )]
    InvalidRecordPath(String),

    #[error("unsupported encoding '{0}' declared by the XML document")]
    UnsupportedEncoding(String),

    #[error("malformed XML at byte {position} (within '{path}'): {message}")]
    Malformed {
        position: u64,
        path: String,
        message: String,
    },

    #[error(
        "undefined entity reference '&{name};' at byte {position} (within '{path}'): only the predefined XML entities and character references are supported"
    )]
    UndefinedEntity {
        name: String,
        position: u64,
        path: String,
    },

    #[error(
        "elements are nested more than {MAX_DEPTH} levels deep at byte {position} (within '{path}')"
    )]
    TooDeep { position: u64, path: String },

    #[error("unexpected end of file: element '{path}' was never closed")]
    UnexpectedEof { path: String },

    #[error(
        "text outside of the root element at byte {position}: the file doesn't appear to be XML"
    )]
    TextOutsideRoot { position: u64 },
}

impl From<XmlError> for ParseError {
    fn from(err: XmlError) -> ParseError {
        ParseError::Parse(Box::new(err))
    }
}

impl Parser for XmlParser {
    fn parse(&self, content: Input) -> Result<Output, ParseError> {
        Ok(Box::new(self.iter(content)?))
    }
}

impl XmlParser {
    fn iter(
        &self,
        content: Input,
    ) -> Result<XmlIter<io::BufReader<Box<dyn io::Read>>>, ParseError> {
        let record_path = match self.config.record_path.as_deref() {
            Some(path) => Some(parse_record_path(path)?),
            None => None,
        };
        let input = transcode_to_utf8(content)?;

        let mut reader = Reader::from_reader(input.into_buffered_stream(BUFFER_SIZE));
        let reader_config = reader.config_mut();
        // Whitespace is handled by `TextBuilder`, which needs to distinguish it from CDATA.
        reader_config.trim_text(false);
        reader_config.expand_empty_elements = false;
        reader_config.check_end_names = true;

        Ok(XmlIter {
            reader,
            buf: Vec::new(),
            skip_buf: Vec::new(),
            version: XmlVersion::Implicit1_0,
            record_path,
            array_elements: self.config.array_elements.iter().cloned().collect(),
            outer: Vec::new(),
            stack: Vec::new(),
            record_count: 0,
            done: false,
            single_names: HashSet::new(),
            repeated_names: HashSet::new(),
            warned_names: HashSet::new(),
        })
    }
}

/// Parses a record path like `/Records/Record` into its element names. The leading slash is
/// optional.
fn parse_record_path(path: &str) -> Result<Vec<String>, XmlError> {
    let trimmed = path.trim();
    let names: Vec<String> = trimmed
        .strip_prefix('/')
        .unwrap_or(trimmed)
        .split('/')
        .map(str::to_string)
        .collect();

    if names
        .iter()
        .any(|name| name.is_empty() || name.contains(char::is_whitespace))
    {
        return Err(XmlError::InvalidRecordPath(path.to_string()));
    }
    Ok(names)
}

/// Returns the input transcoded to UTF-8. The source encoding is determined by a byte order mark
/// if present, then by the encoding declared in the XML prolog, and finally by detection.
fn transcode_to_utf8(content: Input) -> Result<Input, ParseError> {
    let (peek, input) = content.peek(PROLOG_PEEK_BYTES)?;

    let encoding = if has_bom(&peek) {
        detect_encoding(&peek)
    } else if let Some(label) = declared_encoding(&peek) {
        match encoding_rs::Encoding::for_label_no_replacement(label.as_bytes()) {
            // The declaration was only readable because it's written one byte per character,
            // which UTF-16 never is: a genuine UTF-16 document is recognized by its byte order
            // mark or by detection instead, and never reaches this point. So a UTF-16 label
            // here is wrong, as tools sometimes write onto UTF-8 files, and trusting it would
            // decode the whole document into garbage. Other encodings that can label
            // themselves this way (including ISO-2022-JP, which isn't ASCII-compatible but
            // writes its declaration in ASCII) are trusted.
            Some(encoding)
                if encoding == encoding_rs::UTF_16LE || encoding == encoding_rs::UTF_16BE =>
            {
                detect_encoding(&peek)
            }
            Some(encoding) => EncodingRef::from(encoding),
            None => return Err(XmlError::UnsupportedEncoding(label).into()),
        }
    } else {
        detect_encoding(&peek)
    };

    if encoding.is_utf8() {
        Ok(input)
    } else {
        Ok(input.transcode_to_utf8(encoding))
    }
}

fn has_bom(peek: &[u8]) -> bool {
    peek.starts_with(&[0xEF, 0xBB, 0xBF])
        || peek.starts_with(&[0xFE, 0xFF])
        || peek.starts_with(&[0xFF, 0xFE])
}

/// Returns the value of the `encoding` pseudo-attribute of the XML declaration, if there is one.
/// The declaration must be at the very start of the document, and it's only ever composed of
/// ASCII characters, so it can be inspected before the encoding is known.
fn declared_encoding(peek: &[u8]) -> Option<String> {
    let rest = peek.strip_prefix(b"<?xml")?;
    let end = rest.windows(2).position(|w| w == b"?>")?;
    let decl = std::str::from_utf8(&rest[..end]).ok()?;

    let after_key = &decl[decl.find("encoding")? + "encoding".len()..];
    let after_eq = after_key.trim_start().strip_prefix('=')?.trim_start();
    let quote = after_eq
        .chars()
        .next()
        .filter(|c| *c == '"' || *c == '\'')?;
    let value = &after_eq[1..];
    Some(value[..value.find(quote)?].to_string())
}

fn is_xml_whitespace(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r')
}

/// Accumulates the text content of an element.
#[derive(Default)]
struct TextBuilder {
    /// Completed runs of text, each of which was terminated by a child element.
    runs: Vec<String>,
    /// The run of text that's currently being accumulated.
    current: String,
    /// Length of the prefix of `current` that must not be trimmed, because it ends with content
    /// from a CDATA section or a reference.
    protected_len: usize,
}

impl TextBuilder {
    /// Adds ordinary text content, which is subject to trimming.
    fn push_text(&mut self, text: &str) {
        if self.current.is_empty() {
            self.current
                .push_str(text.trim_start_matches(is_xml_whitespace));
        } else {
            self.current.push_str(text);
        }
    }

    /// Adds content from a CDATA section or reference, which is never trimmed.
    fn push_literal(&mut self, text: &str) {
        self.current.push_str(text);
        self.protected_len = self.current.len();
    }

    /// Completes the current run of text. Called whenever a child element begins.
    fn end_run(&mut self) {
        let mut run = std::mem::take(&mut self.current);
        let trimmed_len = run.trim_end_matches(is_xml_whitespace).len();
        run.truncate(trimmed_len.max(self.protected_len));
        self.protected_len = 0;

        if !run.is_empty() {
            self.runs.push(run);
        }
    }

    fn finish(mut self) -> Option<String> {
        self.end_run();
        if self.runs.is_empty() {
            None
        } else {
            Some(self.runs.join(" "))
        }
    }
}

/// Accumulates the content of an element until its end tag is reached.
struct ElementBuilder {
    name: String,
    fields: Map<String, Value>,
    text: TextBuilder,
}

impl ElementBuilder {
    fn add_child(&mut self, name: String, value: Value, always_array: bool) {
        self.text.end_run();

        match self.fields.get_mut(&name) {
            // Element values are only ever strings or objects, so an existing array must be one
            // that holds previous occurrences of this element.
            Some(Value::Array(items)) => items.push(value),
            Some(existing) => {
                let first = existing.take();
                *existing = Value::Array(vec![first, value]);
            }
            None if always_array => {
                self.fields.insert(name, Value::Array(vec![value]));
            }
            None => {
                self.fields.insert(name, value);
            }
        }
    }

    fn finish(self) -> Value {
        let ElementBuilder {
            mut fields, text, ..
        } = self;
        let text = text.finish();

        if fields.is_empty() {
            return Value::String(text.unwrap_or_default());
        }
        if let Some(text) = text {
            fields.insert(TEXT_KEY.to_string(), Value::String(text));
        }
        Value::Object(fields)
    }

    /// Finishes an element that's being emitted as a document, which is an object holding the
    /// element under its own name.
    fn finish_record(self) -> Value {
        let mut record = Map::new();
        let name = self.name.clone();
        record.insert(name, self.finish());
        Value::Object(record)
    }
}

struct XmlIter<R: io::BufRead> {
    reader: Reader<R>,
    buf: Vec<u8>,
    /// Separate buffer used when skipping over elements that are outside of any record.
    skip_buf: Vec<u8>,
    version: XmlVersion,
    /// Path of the elements to emit as documents, or `None` to emit the root element.
    record_path: Option<Vec<String>>,
    array_elements: HashSet<String>,
    /// Names of the currently open elements that enclose the current record.
    outer: Vec<String>,
    /// The currently open elements of the record that's being built. The first element is the
    /// record itself, and this is empty when we're not within a record.
    stack: Vec<ElementBuilder>,
    record_count: u64,
    done: bool,
    /// Names of child elements that occurred only once within some parent element, and names
    /// of those that were repeated within some parent element, across the whole file. Names
    /// listed in `array_elements` are excluded, since they're always arrays.
    single_names: HashSet<String>,
    repeated_names: HashSet<String>,
    /// Names that occurred both once and repeatedly, which have already been logged.
    warned_names: HashSet<String>,
}

impl<R: io::BufRead> Iterator for XmlIter<R> {
    type Item = Result<Value, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.next_record() {
            Ok(Some(record)) => {
                self.record_count += 1;
                Some(Ok(record))
            }
            Ok(None) => {
                self.done = true;
                None
            }
            Err(err) => {
                self.done = true;
                Some(Err(err.into()))
            }
        }
    }
}

impl<R: io::BufRead> XmlIter<R> {
    /// Reads events until the next record is complete, returning `None` at the end of the input.
    fn next_record(&mut self) -> Result<Option<Value>, XmlError> {
        let mut buf = std::mem::take(&mut self.buf);
        let result = self.read_record(&mut buf);
        self.buf = buf;
        result
    }

    fn read_record(&mut self, buf: &mut Vec<u8>) -> Result<Option<Value>, XmlError> {
        loop {
            buf.clear();
            let event = match self.reader.read_event_into(buf) {
                Ok(event) => event,
                Err(err) => return Err(self.malformed(self.reader.error_position(), err)),
            };

            match event {
                Event::Start(start) => {
                    self.start_element(&start, false)?;
                }
                Event::Empty(start) => {
                    if self.start_element(&start, true)?
                        && let Some(record) = self.end_element()
                    {
                        return Ok(Some(record));
                    }
                }
                Event::End(_) => {
                    if let Some(record) = self.end_element() {
                        return Ok(Some(record));
                    }
                }
                Event::Text(text) => {
                    let text = text.xml_content(self.version);
                    match self.stack.last_mut() {
                        Some(top) => top.text.push_text(&text),
                        None => self.check_outside_text(&text)?,
                    }
                }
                Event::CData(cdata) => {
                    let text = cdata.xml_content(self.version);
                    match self.stack.last_mut() {
                        Some(top) => top.text.push_literal(&text),
                        None => self.check_outside_text(&text)?,
                    }
                }
                Event::GeneralRef(reference) => {
                    let resolved = self.resolve_reference(&reference)?;
                    match self.stack.last_mut() {
                        Some(top) => top.text.push_literal(&resolved),
                        None => self.check_outside_text(&resolved)?,
                    }
                }
                Event::Decl(decl) => {
                    self.version = decl.xml_version().unwrap_or(XmlVersion::Implicit1_0);
                }
                Event::Comment(_) | Event::PI(_) | Event::DocType(_) => {}
                Event::Eof => {
                    if !self.outer.is_empty() || !self.stack.is_empty() {
                        return Err(XmlError::UnexpectedEof {
                            path: self.current_path(),
                        });
                    }
                    if self.record_count == 0
                        && let Some(path) = &self.record_path
                    {
                        tracing::warn!(
                            record_path = %format!("/{}", path.join("/")),
                            "no XML elements matched the configured record path"
                        );
                    }
                    return Ok(None);
                }
            }
        }
    }

    /// Text outside of any record is expected within an enclosing element that's being skipped
    /// over (in `recordPath` mode). Outside of the root element, XML allows only whitespace, so
    /// anything else means the file isn't XML: failing beats silently emitting no documents.
    fn check_outside_text(&self, text: &str) -> Result<(), XmlError> {
        if self.outer.is_empty() && !text.chars().all(is_xml_whitespace) {
            return Err(XmlError::TextOutsideRoot {
                position: self.reader.buffer_position(),
            });
        }
        Ok(())
    }

    /// Handles the start of an element. Returns false if the element was skipped entirely
    /// because it can't contain any records, in which case there will be no matching end event.
    fn start_element(&mut self, start: &BytesStart, is_empty: bool) -> Result<bool, XmlError> {
        let name = start.local_name().as_ref().to_string();

        if self.outer.len() + self.stack.len() >= MAX_DEPTH {
            return Err(XmlError::TooDeep {
                position: self.reader.buffer_position(),
                path: format!("{}/{}", self.current_path(), name),
            });
        }

        if let Some(parent) = self.stack.last_mut() {
            parent.text.end_run();
            let element = self.new_element(name, start)?;
            self.stack.push(element);
            return Ok(true);
        }

        self.outer.push(name);
        let is_record = match &self.record_path {
            Some(path) => &self.outer == path,
            None => self.outer.len() == 1,
        };
        let may_contain_record = match &self.record_path {
            Some(path) => path.starts_with(&self.outer),
            None => true,
        };

        if is_record {
            let name = self.outer.pop().unwrap();
            let element = self.new_element(name, start)?;
            self.stack.push(element);
        } else if !may_contain_record {
            self.outer.pop();
            // An empty element has no content or end tag to skip over.
            if !is_empty {
                self.skip_buf.clear();
                if let Err(err) = self
                    .reader
                    .read_to_end_into(start.name(), &mut self.skip_buf)
                {
                    return Err(self.malformed(self.reader.error_position(), err));
                }
            }
            return Ok(false);
        }
        Ok(true)
    }

    /// Handles the end of an element, returning the completed record if this element was one.
    fn end_element(&mut self) -> Option<Value> {
        let Some(element) = self.stack.pop() else {
            self.outer.pop();
            return None;
        };
        self.note_child_shapes(&element);

        match self.stack.last_mut() {
            Some(parent) => {
                let always_array = self.array_elements.contains(&element.name);
                let name = element.name.clone();
                parent.add_child(name, element.finish(), always_array);
                None
            }
            None => {
                self.warn_inconsistent_arrays();
                Some(element.finish_record())
            }
        }
    }

    /// Records which children of a completed element occurred once and which were repeated.
    fn note_child_shapes(&mut self, element: &ElementBuilder) {
        for (name, value) in &element.fields {
            if name.starts_with(ATTRIBUTE_PREFIX) || self.array_elements.contains(name) {
                continue;
            }
            // Element values are only ever strings or objects, so an array holds repetitions.
            if value.is_array() {
                self.repeated_names.insert(name.clone());
            } else {
                self.single_names.insert(name.clone());
            }
        }
    }

    /// Logs elements that have so far occurred both once and repeatedly, and so are represented
    /// as an array in some places but not in others. Each name is logged once per file.
    fn warn_inconsistent_arrays(&mut self) {
        let mut names: Vec<String> = self
            .single_names
            .intersection(&self.repeated_names)
            .filter(|name| !self.warned_names.contains(*name))
            .cloned()
            .collect();
        if names.is_empty() {
            return;
        }
        names.sort();

        tracing::warn!(
            elements = ?names,
            "some XML elements occur once in some places and are repeated in others, so they're represented as an array in some places but not others; add them to arrayElements so they're always arrays"
        );
        self.warned_names.extend(names);
    }

    fn new_element(&self, name: String, start: &BytesStart) -> Result<ElementBuilder, XmlError> {
        let mut fields = Map::new();

        for attribute in start.attributes() {
            let attribute =
                attribute.map_err(|err| self.malformed(self.reader.buffer_position(), err))?;
            // Namespace declarations aren't data, so they're omitted.
            if attribute.key.as_namespace_binding().is_some() {
                continue;
            }
            let value = attribute
                .normalized_value(self.version)
                .map_err(|err| self.malformed(self.reader.buffer_position(), err))?;
            fields.insert(
                format!("{ATTRIBUTE_PREFIX}{}", attribute.key.local_name().as_ref()),
                Value::String(value.into_owned()),
            );
        }

        Ok(ElementBuilder {
            name,
            fields,
            text: TextBuilder::default(),
        })
    }

    fn resolve_reference(&self, reference: &BytesRef) -> Result<String, XmlError> {
        match reference.resolve_char_ref() {
            Ok(Some(c)) => return Ok(c.to_string()),
            Ok(None) => {}
            Err(err) => return Err(self.malformed(self.reader.buffer_position(), err)),
        }

        let name = reference.xml_content(self.version);
        match resolve_predefined_entity(&name) {
            Some(resolved) => Ok(resolved.to_string()),
            None => Err(XmlError::UndefinedEntity {
                name: name.into_owned(),
                position: self.reader.buffer_position(),
                path: self.current_path(),
            }),
        }
    }

    /// Returns the path of the currently open element, for use in error messages.
    fn current_path(&self) -> String {
        let names = self
            .outer
            .iter()
            .chain(self.stack.iter().map(|element| &element.name));
        let mut path = String::new();
        for name in names {
            path.push('/');
            path.push_str(name);
        }
        if path.is_empty() {
            path.push('/');
        }
        path
    }

    fn malformed(&self, position: u64, err: impl std::fmt::Display) -> XmlError {
        XmlError::Malformed {
            position,
            path: self.current_path(),
            message: err.to_string(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    fn parse_with(config: XmlConfig, content: impl Into<Vec<u8>>) -> Result<Vec<Value>, String> {
        let input = Input::Stream(Box::new(std::io::Cursor::new(content.into())));
        let output = new_parser(config).parse(input).map_err(|e| e.to_string())?;
        output
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())
    }

    fn parse(content: impl Into<Vec<u8>>) -> Vec<Value> {
        parse_with(XmlConfig::default(), content).expect("parse failed")
    }

    fn parse_one(content: &str) -> Value {
        let mut docs = parse(content);
        assert_eq!(1, docs.len(), "expected one document, got: {docs:?}");
        docs.pop().unwrap()
    }

    fn with_arrays(names: &[&str]) -> XmlConfig {
        XmlConfig {
            array_elements: names.iter().map(|n| n.to_string()).collect(),
            ..Default::default()
        }
    }

    fn with_record_path(path: &str) -> XmlConfig {
        XmlConfig {
            record_path: Some(path.to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn root_element_is_one_document_under_its_own_name() {
        let doc = parse_one(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <Order>
              <Number>B-100</Number>
              <Party><Name>Acme</Name></Party>
            </Order>"#,
        );
        assert_eq!(
            json!({"Order": {"Number": "B-100", "Party": {"Name": "Acme"}}}),
            doc
        );
    }

    #[test]
    fn attributes_and_text() {
        let doc = parse_one(
            r#"<Root id="7">
                 <Weight uom="kg">12.5</Weight>
                 <Dims uom="cm"><Length>40</Length></Dims>
                 <Flag set="true"/>
               </Root>"#,
        );
        assert_eq!(
            json!({"Root": {
                "@id": "7",
                "Weight": {"@uom": "kg", "#text": "12.5"},
                "Dims": {"@uom": "cm", "Length": "40"},
                "Flag": {"@set": "true"},
            }}),
            doc
        );
    }

    #[test]
    fn repeated_elements_become_arrays() {
        let doc = parse_one("<R><Ref>a</Ref><Ref>b</Ref><Ref>c</Ref><One>x</One></R>");
        assert_eq!(json!({"R": {"Ref": ["a", "b", "c"], "One": "x"}}), doc);
    }

    #[test]
    fn non_adjacent_repeated_elements_are_grouped() {
        let doc = parse_one("<R><A>1</A><B>2</B><A>3</A></R>");
        assert_eq!(json!({"R": {"A": ["1", "3"], "B": "2"}}), doc);
    }

    #[test]
    fn array_elements_are_always_arrays() {
        let content = r#"<R>
            <Refs><Ref><Type>A1</Type></Ref></Refs>
            <Notes><Note>one</Note><Note>two</Note></Notes>
            <Other>x</Other>
        </R>"#;
        let docs = parse_with(with_arrays(&["Ref", "Note"]), content).unwrap();
        assert_eq!(
            vec![json!({"R": {
                "Refs": {"Ref": [{"Type": "A1"}]},
                "Notes": {"Note": ["one", "two"]},
                "Other": "x",
            }})],
            docs
        );
    }

    #[test]
    fn values_are_never_typed() {
        let doc =
            parse_one("<R><Zip>012345</Zip><Flag>false</Flag><N>1.50</N><Count>3</Count></R>");
        assert_eq!(
            json!({"R": {"Zip": "012345", "Flag": "false", "N": "1.50", "Count": "3"}}),
            doc
        );
    }

    #[test]
    fn empty_elements_are_empty_strings() {
        let doc = parse_one(r#"<R><A/><B></B><C>   </C><D x="1"/></R>"#);
        assert_eq!(
            json!({"R": {"A": "", "B": "", "C": "", "D": {"@x": "1"}}}),
            doc
        );
    }

    #[test]
    fn text_is_trimmed_but_internal_whitespace_is_kept() {
        let doc = parse_one("<R><Note>\n   line one\n\n   line two  \n</Note></R>");
        assert_eq!(json!({"R": {"Note": "line one\n\n   line two"}}), doc);
    }

    #[test]
    fn cdata_and_references_are_not_trimmed() {
        let doc = parse_one(
            "<R><A><![CDATA[  keep me  ]]></A><B>&#32;x&#32;</B><C> a &amp; b </C><D>  <![CDATA[ x ]]>  </D></R>",
        );
        assert_eq!(
            json!({"R": {"A": "  keep me  ", "B": " x ", "C": "a & b", "D": " x "}}),
            doc
        );
    }

    #[test]
    fn predefined_entities_and_char_refs_are_expanded() {
        let doc = parse_one(
            r#"<R a="&lt;&quot;&#65;&quot;&gt;">&lt;&gt;&amp;&apos;&quot;&#x41;&#66;</R>"#,
        );
        assert_eq!(json!({"R": {"@a": "<\"A\">", "#text": "<>&'\"AB"}}), doc);
    }

    #[test]
    fn mixed_content_text_is_joined() {
        let doc = parse_one("<R><P>hello <b>big</b> world</P></R>");
        assert_eq!(
            json!({"R": {"P": {"b": "big", "#text": "hello world"}}}),
            doc
        );
    }

    #[test]
    fn text_only_and_empty_roots() {
        assert_eq!(json!({"R": "hello"}), parse_one("<R>hello</R>"));
        assert_eq!(json!({"R": ""}), parse_one("<R/>"));
    }

    #[test]
    fn namespaces_are_stripped() {
        let doc = parse_one(
            r#"<ns:Root xmlns:ns="urn:a" xmlns="urn:b" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                 <ns:Item xsi:type="Thing">x</ns:Item>
               </ns:Root>"#,
        );
        assert_eq!(
            json!({"Root": {"Item": {"@type": "Thing", "#text": "x"}}}),
            doc
        );
    }

    #[test]
    fn comments_pis_and_doctype_are_ignored() {
        let doc = parse_one(
            r#"<?xml version="1.0"?>
            <!DOCTYPE R>
            <!-- a comment -->
            <?app instruction?>
            <R><!-- inner --><A>1</A><?pi?></R>"#,
        );
        assert_eq!(json!({"R": {"A": "1"}}), doc);
    }

    #[test]
    fn external_entities_are_never_resolved() {
        let err = parse_with(
            XmlConfig::default(),
            r#"<?xml version="1.0"?>
            <!DOCTYPE R [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
            <R>&xxe;</R>"#,
        )
        .unwrap_err();
        assert!(err.contains("undefined entity reference '&xxe;'"), "{err}");
    }

    #[test]
    fn internal_entities_are_not_expanded() {
        // A "billion laughs" document must fail on the first custom entity reference,
        // without attempting any expansion.
        let err = parse_with(
            XmlConfig::default(),
            r#"<?xml version="1.0"?>
            <!DOCTYPE lolz [
              <!ENTITY lol "lol">
              <!ENTITY lol1 "&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;">
              <!ENTITY lol2 "&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;">
            ]>
            <lolz>&lol2;</lolz>"#,
        )
        .unwrap_err();
        assert!(err.contains("undefined entity reference '&lol2;'"), "{err}");
    }

    #[test]
    fn custom_entity_in_attribute_is_an_error() {
        let err = parse_with(XmlConfig::default(), r#"<R a="&custom;"/>"#).unwrap_err();
        assert!(err.contains("malformed XML"), "{err}");
    }

    #[test]
    fn excessive_nesting_is_rejected() {
        let depth = MAX_DEPTH + 1;
        let content = format!("{}{}", "<a>".repeat(depth), "</a>".repeat(depth));
        let err = parse_with(XmlConfig::default(), content).unwrap_err();
        assert!(err.contains("nested more than 256 levels"), "{err}");

        let depth = MAX_DEPTH;
        let content = format!("{}x{}", "<a>".repeat(depth), "</a>".repeat(depth));
        parse_with(XmlConfig::default(), content).expect("max depth should be allowed");
    }

    #[test]
    fn mismatched_end_tag_is_an_error() {
        let err = parse_with(XmlConfig::default(), "<R><A>1</B></R>").unwrap_err();
        assert!(err.contains("malformed XML"), "{err}");
    }

    #[test]
    fn truncated_file_is_an_error() {
        let err = parse_with(XmlConfig::default(), "<R><A>1</A><B>").unwrap_err();
        assert!(err.contains("'/R/B' was never closed"), "{err}");
    }

    #[test]
    fn empty_input_produces_no_documents() {
        assert!(parse("").is_empty());
        assert!(parse("<?xml version=\"1.0\"?>\n").is_empty());
    }

    #[test]
    fn record_path_emits_each_match() {
        let content = r#"<Batch>
            <Header><Count>2</Count></Header>
            <Orders>
              <Order id="1"><N>a</N></Order>
              <Order id="2"><N>b</N></Order>
              <Other><Order id="nested-elsewhere"/></Other>
            </Orders>
            <Order id="wrong-level"/>
        </Batch>"#;
        for path in ["/Batch/Orders/Order", "Batch/Orders/Order"] {
            let docs = parse_with(with_record_path(path), content).unwrap();
            assert_eq!(
                vec![
                    json!({"Order": {"@id": "1", "N": "a"}}),
                    json!({"Order": {"@id": "2", "N": "b"}}),
                ],
                docs
            );
        }
    }

    #[test]
    fn record_path_matching_root() {
        let docs = parse_with(with_record_path("/R"), "<R><A>1</A></R>").unwrap();
        assert_eq!(vec![json!({"R": {"A": "1"}})], docs);
    }

    #[test]
    fn record_path_matching_empty_elements() {
        let docs = parse_with(
            with_record_path("/R/Item"),
            r#"<R><Item a="1"/><Item a="2"/></R>"#,
        )
        .unwrap();
        assert_eq!(
            vec![json!({"Item": {"@a": "1"}}), json!({"Item": {"@a": "2"}})],
            docs
        );
    }

    #[test]
    fn record_path_with_no_matches_produces_no_documents() {
        let docs = parse_with(with_record_path("/Batch/Order"), "<Batch/>").unwrap();
        assert!(docs.is_empty());
    }

    #[test]
    fn record_path_errors_are_reported_after_preceding_records() {
        let input = Input::Stream(Box::new(std::io::Cursor::new(
            "<R><I>1</I><I>2</I><I>3</J></R>",
        )));
        let results: Vec<_> = new_parser(with_record_path("/R/I"))
            .parse(input)
            .unwrap()
            .collect();
        assert_eq!(3, results.len());
        assert_eq!(json!({"I": "1"}), *results[0].as_ref().unwrap());
        assert_eq!(json!({"I": "2"}), *results[1].as_ref().unwrap());
        assert!(results[2].is_err());
    }

    /// Parses all of `content` and returns the element names that were logged as inconsistent.
    fn warned_names(config: XmlConfig, content: &str) -> Vec<String> {
        let input = Input::Stream(Box::new(std::io::Cursor::new(content.to_string())));
        let parser = XmlParser { config };
        let mut iter = parser.iter(input).unwrap();
        for result in iter.by_ref() {
            result.unwrap();
        }
        let mut names: Vec<String> = iter.warned_names.into_iter().collect();
        names.sort();
        names
    }

    #[test]
    fn inconsistent_arrays_are_reported() {
        let content = r#"<R>
            <A><Ref>1</Ref><Ref>2</Ref><Name>x</Name></A>
            <B><Ref>3</Ref><Name>y</Name><Tag>t</Tag><Tag>u</Tag></B>
        </R>"#;
        // `Ref` is repeated within A but single within B. `Tag` is only ever repeated, and
        // `Name` is only ever single, so neither changes shape.
        assert_eq!(vec!["Ref"], warned_names(XmlConfig::default(), content));
        // Listing it fixes the shape, so it's no longer reported.
        assert!(warned_names(with_arrays(&["Ref"]), content).is_empty());
    }

    #[test]
    fn inconsistent_arrays_are_reported_across_records() {
        let content = r#"<Batch>
            <Order><Line>1</Line></Order>
            <Order><Line>2</Line><Line>3</Line></Order>
        </Batch>"#;
        assert_eq!(
            vec!["Line"],
            warned_names(with_record_path("/Batch/Order"), content)
        );
    }

    #[test]
    fn text_outside_root_is_an_error() {
        for content in [
            r#"{"order": 1, "note": "this is JSON, not XML"}"#,
            "hello <R><A>1</A></R>",
            "<R><A>1</A></R> trailing",
            "<R/>&amp;",
            "<R/><![CDATA[x]]>",
        ] {
            let err = parse_with(XmlConfig::default(), content).unwrap_err();
            assert!(
                err.contains("text outside of the root element"),
                "{content}: {err}"
            );
        }
        // Whitespace, comments and processing instructions around the root are fine, in both
        // modes, as is text within wrapper elements that a record path skips over.
        let content = "\n <?pi?> <!-- c --> <R>wrapper text<I>1</I></R> \n";
        assert_eq!(1, parse(content).len());
        assert_eq!(
            vec![json!({"I": "1"})],
            parse_with(with_record_path("/R/I"), content).unwrap()
        );
    }

    #[test]
    fn utf16_label_on_single_byte_document_is_ignored() {
        let content = "<?xml version=\"1.0\" encoding=\"UTF-16\"?><R><City>München</City></R>";
        assert_eq!(
            vec![json!({"R": {"City": "München"}})],
            parse_with(XmlConfig::default(), content).unwrap()
        );
    }

    #[test]
    fn utf16_without_bom_is_not_silently_empty() {
        let text = "<?xml version=\"1.0\" encoding=\"UTF-16\"?><R><A>hi</A></R>";
        let mut content = Vec::new();
        for unit in text.encode_utf16() {
            content.extend_from_slice(&unit.to_le_bytes());
        }
        // Without a byte order mark, detection may or may not recognize UTF-16, but the
        // result must never be a successful parse that produced nothing.
        if let Ok(docs) = parse_with(XmlConfig::default(), content) {
            assert_eq!(vec![json!({"R": {"A": "hi"}})], docs);
        }
    }

    #[test]
    fn invalid_record_paths_are_rejected() {
        for path in ["", "/", "/a//b", "/a/b c", "a/"] {
            let err = parse_with(with_record_path(path), "<a/>").unwrap_err();
            assert!(err.contains("invalid recordPath"), "{path}: {err}");
        }
    }

    #[test]
    fn declared_encoding_is_used() {
        let mut content = b"<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><R><City>".to_vec();
        content.extend_from_slice(&[0x4D, 0xFC, 0x6E, 0x63, 0x68, 0x65, 0x6E]); // "München"
        content.extend_from_slice(b"</City></R>");
        let docs = parse_with(XmlConfig::default(), content).unwrap();
        assert_eq!(vec![json!({"R": {"City": "München"}})], docs);
    }

    #[test]
    fn unknown_declared_encoding_is_an_error() {
        let err = parse_with(
            XmlConfig::default(),
            r#"<?xml version="1.0" encoding="not-real"?><R/>"#,
        )
        .unwrap_err();
        assert!(err.contains("unsupported encoding 'not-real'"), "{err}");
    }

    #[test]
    fn utf16_with_bom_is_transcoded() {
        let text = "<?xml version=\"1.0\" encoding=\"UTF-16\"?><R><Name>متجر الكتب</Name></R>";
        let mut content = vec![0xFF, 0xFE];
        for unit in text.encode_utf16() {
            content.extend_from_slice(&unit.to_le_bytes());
        }
        let docs = parse_with(XmlConfig::default(), content).unwrap();
        assert_eq!(vec![json!({"R": {"Name": "متجر الكتب"}})], docs);
    }

    #[test]
    fn utf8_bom_is_skipped() {
        let mut content = vec![0xEF, 0xBB, 0xBF];
        content.extend_from_slice(b"<?xml version=\"1.0\"?><R><A>1</A></R>");
        let docs = parse_with(XmlConfig::default(), content).unwrap();
        assert_eq!(vec![json!({"R": {"A": "1"}})], docs);
    }

    #[test]
    fn declared_encoding_is_extracted() {
        assert_eq!(
            Some("ISO-8859-1".to_string()),
            declared_encoding(br#"<?xml version="1.0" encoding = 'ISO-8859-1' ?><R/>"#)
        );
        assert_eq!(None, declared_encoding(br#"<?xml version="1.0"?><R/>"#));
        assert_eq!(None, declared_encoding(br#"<R encoding="x"/>"#));
    }
}
