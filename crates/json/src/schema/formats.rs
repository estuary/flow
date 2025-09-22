use addr::{parse_domain_name, parse_email_address};
use bigdecimal::BigDecimal;
use iri_string::spec::{IriSpec, UriSpec};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{net::IpAddr, str::FromStr};
use time::macros::format_description;
use uuid::Uuid;

use crate::validator::ValidationResult;

#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Format {
    Date,
    #[serde(rename = "date-time", alias = "datetime")]
    DateTime,
    Time,
    Email,
    Hostname,
    /// IdnHostname is parsed but is not supported (validation always fails).
    #[serde(rename = "idn-hostname")]
    IdnHostname,
    /// IdnEmail is parsed but is not supported (validation always fails).
    #[serde(rename = "idn-email")]
    IdnEmail,
    Ipv4,
    Ipv6,
    Macaddr,
    Macaddr8,
    Uuid,
    Duration,
    Iri,
    Uri,
    #[serde(rename = "uri-reference")]
    UriReference,
    #[serde(rename = "iri-reference")]
    IriReference,
    #[serde(rename = "uri-template")]
    UriTemplate,
    #[serde(rename = "json-pointer")]
    JsonPointer,
    #[serde(rename = "regex")]
    Regex,
    #[serde(rename = "relative-json-pointer")]
    RelativeJsonPointer,
    #[serde(alias = "uint32", alias = "uint64")]
    Integer,
    Number,
    #[serde(rename = "sha256")]
    Sha256,
}

// Some are from https://github.com/JamesNK/Newtonsoft.Json.Schema/blob/master/Src/Newtonsoft.Json.Schema/Infrastructure/FormatHelpers.cs
// Some are artisinally crafted
lazy_static::lazy_static! {
    static ref DATE_RE: Regex =
        Regex::new(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}\z").expect("Is a valid regex");
    static ref RELATIVE_JSON_POINTER_RE: Regex =
        Regex::new(r"^(?:0|[1-9][0-9]*)(?:#|(?:/(?:[^~/]|~0|~1)*)*)\z").expect("Is a valid regex");
    static ref URI_TEMPLATE_RE: Regex = Regex::new(
        r#"^(?:(?:[^\x00-\x20""'<>%\\^`{|}]|%[0-9a-f]{2})|\{[+#.\/;?&=,!@|]?(?:[a-z0-9_]|%[0-9a-f]{2})+(?:\:[1-9][0-9]{0,3}|\*)?(?:,(?:[a-z0-9_]|%[0-9a-f]{2})+(?:\:[1-9][0-9]{0,3}|\*)?)*\})*$"#
    )
    .expect("Is a valid regex");
    static ref ISO_8601_DURATION_RE: Regex = Regex::new(r"^P(?:\d+W|(?:(?:\d+Y(?:\d+M)?(?:\d+D)?|\d+M(?:\d+D)?|\d+D)(?:T(?:\d+H(?:\d+M)?(?:\d+S)?|\d+M(?:\d+S)?|\d+S))?)|T(?:\d+H(?:\d+M)?(?:\d+S)?|\d+M(?:\d+S)?|\d+S))$").expect("Is a valid regex");
    static ref JSON_POINTER_RE: Regex = Regex::new(r"^(\/([^~]|(~[01]))*)*$").expect("Is a valid regex");
    static ref MACADDR: Regex = Regex::new(r"^([0-9A-Fa-f]{2}[:-]?){5}[0-9A-Fa-f]{2}$").expect("Is a valid regex");
    static ref MACADDR8: Regex = Regex::new(r"^([0-9A-Fa-f]{2}[:-]?){7}[0-9A-Fa-f]{2}$").expect("Is a valid regex");
}

impl ToString for Format {
    fn to_string(&self) -> String {
        if let serde_json::Value::String(s) = serde_json::json!(self) {
            s
        } else {
            panic!("Format must serialize as JSON string")
        }
    }
}

impl Format {
    pub fn validate(&self, val: &str) -> ValidationResult {
        match self {
            Self::Date => {
                // Padding with zeroes is ignored by the underlying parser. The most efficient
                // way to check it will be to use a custom parser that won't ignore zeroes,
                // but this regex will do the trick and costs ~20% extra time in this validator.
                if !DATE_RE.is_match(val) {
                    return ValidationResult::Invalid(None);
                }
                ValidationResult::from(time::Date::parse(
                    val,
                    &format_description!("[year]-[month]-[day]"),
                ))
            }
            Self::DateTime => ValidationResult::from(time::OffsetDateTime::parse(
                val,
                &time::format_description::well_known::Rfc3339,
            )),
            Self::Time => {
                // [first] will choose the first matching format to parse the value
                // see https://time-rs.github.io/book/api/format-description.html for more info
                let full_format = format_description!(
                    version = 2,
                    "[first
                    [[hour]:[minute]:[second][optional [.[subsecond]]]Z]
                    [[hour]:[minute]:[second][optional [.[subsecond]]]z]
                    [[hour]:[minute]:[second][optional [.[subsecond]]][offset_hour]:[offset_minute]]
                    ]"
                );

                ValidationResult::from(time::Time::parse(
                    val,
                    &time::format_description::FormatItem::First(full_format),
                ))
            }
            Self::Email => ValidationResult::from(parse_email_address(val)),
            Self::Hostname => ValidationResult::from(parse_domain_name(val)),
            // The rules/test cases for these are absolutely bonkers
            // If we end up needing this let's revisit (jshearer)
            Self::IdnHostname | Self::IdnEmail => {
                ValidationResult::Invalid(Some(format!("{self:?} is not supported")))
            }
            Self::Ipv4 => {
                if val.starts_with('0') {
                    return ValidationResult::Invalid(None);
                }
                match IpAddr::from_str(val) {
                    Ok(i) => ValidationResult::from(i.is_ipv4()),
                    Err(e) => ValidationResult::Invalid(Some(e.to_string())),
                }
            }
            Self::Ipv6 => ValidationResult::from(match IpAddr::from_str(val) {
                Ok(i) => ValidationResult::from(i.is_ipv6()),
                Err(e) => ValidationResult::Invalid(Some(e.to_string())),
            }),
            Self::Macaddr => ValidationResult::from(MACADDR.is_match(val)),
            Self::Macaddr8 => ValidationResult::from(MACADDR8.is_match(val)),
            // uuid crate supports non-hyphenated inputs, jsonschema does not
            Self::Uuid if val.len() == 36 => ValidationResult::from(Uuid::parse_str(val)),
            Self::Uuid => ValidationResult::Invalid(Some(format!(
                "{val} is the wrong length (missing hyphens?)"
            ))),

            Self::Duration => ValidationResult::from(ISO_8601_DURATION_RE.is_match(val)),
            Self::Iri => ValidationResult::from(iri_string::validate::iri::<IriSpec>(val)),
            Self::Uri => ValidationResult::from(iri_string::validate::iri::<UriSpec>(val)),
            Self::UriReference => {
                ValidationResult::from(iri_string::validate::iri_reference::<UriSpec>(val))
            }
            Self::IriReference => {
                ValidationResult::from(iri_string::validate::iri_reference::<IriSpec>(val))
            }
            Self::UriTemplate => ValidationResult::from(URI_TEMPLATE_RE.is_match(val)),
            Self::JsonPointer => ValidationResult::from(JSON_POINTER_RE.is_match(val)),
            Self::Regex => ValidationResult::from(Regex::new(val)),
            Self::RelativeJsonPointer => {
                ValidationResult::from(RELATIVE_JSON_POINTER_RE.is_match(val))
            }
            Self::Integer => ValidationResult::from(
                BigDecimal::from_str(val)
                    .map(|d| d.is_integer())
                    .unwrap_or(false)
                    && !val.contains("_"),
            ),
            Self::Number => ValidationResult::from(
                BigDecimal::from_str(val).is_ok() && !val.contains("_")
                    || ["NaN", "Infinity", "-Infinity"].contains(&val),
            ),
            Self::Sha256 => ValidationResult::from(
                // See also doc::redact::Strategy::apply() for Sha256.
                val.len() == 71
                    && &val.as_bytes()[0..7] == b"sha256:"
                    && val[7..].bytes().all(|b| b.is_ascii_hexdigit()),
            ),
        }
    }

    // Detect the Format matched by a given, arbitrary string (if any).
    pub fn detect(val: &str) -> Option<Self> {
        match val {
            _ if Format::Integer.validate(val).is_ok() => Some(Format::Integer),
            _ if Format::Number.validate(val).is_ok() => Some(Format::Number),
            _ if Format::DateTime.validate(val).is_ok() => Some(Format::DateTime),
            _ if Format::Date.validate(val).is_ok() => Some(Format::Date),
            _ if Format::Uuid.validate(val).is_ok() => Some(Format::Uuid),
            _ if Format::Sha256.validate(val).is_ok() => Some(Format::Sha256),
            _ => None,
        }
    }
}

#[cfg(test)]
mod test {
    use super::Format;
    use crate::validator::ValidationResult;

    #[test]
    fn test_format_cases() {
        // Missing format cases:
        //  * idn-hostname
        //  * idn-email
        //  * iri
        //  * iri-reference
        for (format, value, expect) in [
            ("date", "2022-09-11", true),
            ("date", "2022-09-11T10:31:25.123Z", false),
            ("date-time", "2022-09-11T10:31:25.123Z", true),
            ("date-time", "2022-09-11T10:31:25Z", true),
            ("date-time", "2022-09-11T10:31:25z", true),
            ("date-time", "2022-09-11T10:31:25+00:00", true),
            ("date-time", "2022-09-11T10:31:25-00:00", true),
            ("datetime", "2022-09-11T10:31:25.123Z", true), // Accepted alias.
            ("date-time", "10:31:25.123Z", false),
            ("time", "10:31:25.123Z", true),
            ("time", "10:31:25.123z", true),
            ("time", "10:31:25z", true),
            ("time", "10:31:25.123+00:00", true),
            ("time", "10:31:25.123-10:00", true),
            ("time", "10:31:25.123-00:10", true),
            ("time", "10:31:25-00:10", true),
            ("time", "10:31:25+00:10", true),
            ("email", "john@doe.com", true),
            ("email", "john at doe.com", false),
            ("hostname", "hostname.com", true),
            ("hostname", "hostname dot com", false),
            ("ipv4", "123.45.6.78", true),
            ("ipv4", "123.45.6.78.9", false),
            ("ipv4", "0.1.2.3", false),
            ("ipv6", "2001:0db8:0000:0000:0000:ff00:0042:8329", true),
            ("ipv6", "2001:db8::ff00:42:8329", true),
            ("ipv6", "2001 db8  ff00:42:8329", false),
            ("macaddr", "001B638445E6", true),
            ("macaddr", "00:1b-63:84-45:e6", true),
            ("macaddr", "00!1b!63:84!45:e6", false),
            ("macaddr", "00:1b:63:84:45:e6", true),
            ("macaddr8", "00:1b:63:84:45:e6", false),
            ("macaddr8", "00:1b:63:84:45:e6:aa:bb", true),
            ("macaddr8", "00-1b-638445e6aa:bb", true),
            ("uuid", "df518555-34f0-446a-8788-7b36f607bbea", true),
            ("uuid", "DF518555-34F0-446A-8788-7B36F607BBEA", true),
            ("uuid", "not-a-UUID-7B36F607BBEA", false),
            ("duration", "P1M3DT30H4S", true),
            ("duration", "P1W", true),
            ("duration", "P2W", true),
            ("duration", "P3Y6M4DT12H30M5S", true),
            ("duration", "P0.5Y", false),
            ("duration", "P3DT12H", true),
            ("duration", "PT6H", true),
            ("duration", "PT1H30M", true),
            ("duration", "PT45S", true),
            ("duration", "PT0.75H", false),
            ("duration", "P3DT4H30M5.75S", false), // Fractional seconds not allowed.
            ("duration", "PT1.5H2.25M", false),
            ("duration", "PT0.5S", false),
            ("duration", "PT1H30.5S", false),
            ("duration", "PT2M3.75S", false),
            ("duration", "PT0S", true), // zero‑length duration is allowed
            ("duration", "PT1M60S", true), // still matches even if semantically > 59 s
            ("duration", "PT1000000000000H", true),
            ("duration", "P1W3D", false), // mixes weeks with other calendar units
            ("duration", "P", false),     // lone designator
            ("duration", "PT", false),    // lone time designator
            ("duration", "P1W2D", false), // weeks + days
            ("duration", "P1D2Y", false), // wrong unit order
            ("duration", "P1M2H", false), // missing 'T' before time units
            ("duration", "PT4H1Y", false), // date unit after 'T'
            ("duration", "1Y", false),    // missing leading 'P'
            ("duration", "p1D", false),   // lowercase 'p'
            ("duration", "-P1D", false),  // negative value
            ("duration", "P1.5.3Y", false), // multiple decimals
            ("duration", "P1YT", false),  // trailing 'T' with no time fields
            ("duration", "P1M3DT30H4S", true),
            ("duration", "P1W", true),
            ("duration", "P1W3D", false), // Mixes weeks and days (disallowed).
            ("duration", "roundtuit", false),
            ("uri", "http://www.example.org/foo/bar", true),
            ("uri", "../path/to/bar", false),
            ("uri-reference", "../path/to/bar", true),
            ("uri", "http://example.com/~{username}/", false),
            ("uri-template", "http://example.com/~{username}/", true),
            ("json-pointer", "/valid/json pointer", true),
            ("json-pointer", "/invalid/es~cape", false),
            ("relative-json-pointer", "0/objects", true),
            ("regex", "^hello$", true),
            ("regex", "[hello", false),
            ("integer", "1234", true),
            ("integer", "1234.00", true),
            ("integer", "-1234", true),
            ("integer", "1_234", false),
            ("integer", "1.234", false),
            ("integer", " 1234", false),
            ("uint32", "1234", true),
            ("uint64", "1234", true),
            ("number", "1234", true),
            ("number", "-1234", true),
            ("number", "1_234", false),
            ("number", "1.234", true),
            ("number", "-1.234", true),
            ("number", " 1234", false),
            ("number", " 1.234", false),
            ("number", "NaN", true),
            ("number", "xNaN", false),
            ("number", "nan", false),
            ("number", "Infinity", true),
            ("number", "-Infinity", true),
            ("number", "infinity", false),
            (
                "sha256",
                "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                true,
            ),
            (
                "sha256",
                "sha256:E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855",
                true,
            ),
            (
                "sha256",
                "sha256:a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3",
                true,
            ),
            (
                "sha256",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                false, // No prefix
            ),
            (
                "sha256",
                "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85",
                false, // Too short
            ),
            (
                "sha256",
                "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b8555",
                false, // Too long
            ),
            (
                "sha256",
                "sha256:g3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                false, // Invalid hex char 'g'
            ),
            (
                "sha256",
                "sha512:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                false, // Wrong prefix
            ),
            (
                "sha256",
                "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85 ",
                false, // Space at end
            ),
            ("sha256", "", false),
            ("sha256", "sha256:", false), // Prefix only
        ] {
            let format: Format =
                serde_json::from_value(serde_json::Value::String(format.to_string())).unwrap();

            match format.validate(value) {
                ValidationResult::Valid if expect => {}
                ValidationResult::Invalid(_) if !expect => {}
                ValidationResult::Valid => {
                    panic!("expected {format:?} with {value} to be invalid, but it's valid")
                }
                ValidationResult::Invalid(reason) => {
                    panic!(
                        "expected {format:?} with {value} to be valid, but it's invalid with {reason:?}"
                    )
                }
            }
        }
    }
}
