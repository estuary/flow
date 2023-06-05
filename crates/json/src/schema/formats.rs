use std::{net::IpAddr, str::FromStr};

use addr::{parse_domain_name, parse_email_address};
use bigdecimal::BigDecimal;
use fancy_regex::Regex;
use iri_string::spec::{IriSpec, UriSpec};
use serde::{Deserialize, Serialize};
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
    Integer,
    Number,
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
    static ref ISO_8601_DURATION_RE: Regex = Regex::new(r"^P(?!$)(\d+(?:\.\d+)?Y)?(\d+(?:\.\d+)?M)?(\d+(?:\.\d+)?W)?(\d+(?:\.\d+)?D)?(T(?=\d)(\d+(?:\.\d+)?H)?(\d+(?:\.\d+)?M)?(\d+(?:\.\d+)?S)?)?$").expect("Is a valid regex");
    static ref ISO_8601_ONLY_WEEKS_RE: Regex = Regex::new(r"^[0-9P|W]*$").expect("Is a valid regex");
    static ref ISO_8601_NO_WEEKS_RE: Regex = Regex::new(r"^[^W]*$").expect("Is a valid regex");
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
                if !DATE_RE.is_match(val).unwrap_or(false) {
                    return ValidationResult::Invalid(None);
                }
                ValidationResult::from(time::Date::parse(
                    val,
                    &time::macros::format_description!("[year]-[month]-[day]"),
                ))
            }
            Self::DateTime => ValidationResult::from(time::OffsetDateTime::parse(
                val,
                &time::format_description::well_known::Rfc3339,
            )),
            Self::Time => ValidationResult::from(time::Time::parse(
                val,
                &time::macros::format_description!("[hour]:[minute]:[second].[subsecond]Z"),
            )),
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
            Self::Macaddr => ValidationResult::from(MACADDR.is_match(val).unwrap_or(false)),
            Self::Macaddr8 => ValidationResult::from(MACADDR8.is_match(val).unwrap_or(false)),
            // uuid crate supports non-hyphenated inputs, jsonschema does not
            Self::Uuid if val.len() == 36 => ValidationResult::from(Uuid::parse_str(val)),
            Self::Uuid => ValidationResult::Invalid(Some(format!(
                "{val} is the wrong length (missing hyphens?)"
            ))),

            Self::Duration => ValidationResult::from(match ISO_8601_DURATION_RE.is_match(val) {
                Ok(true) => {
                    if val.contains("W") {
                        // If we parse as weeks, ensure that ONLY weeks are provided
                        ISO_8601_ONLY_WEEKS_RE.is_match(val).unwrap_or(false)
                    } else {
                        // Otherwise, ensure that NO weeks are provided
                        ISO_8601_NO_WEEKS_RE.is_match(val).unwrap_or(false)
                    }
                }
                _ => false,
            }),
            Self::Iri => ValidationResult::from(iri_string::validate::iri::<IriSpec>(val)),
            Self::Uri => ValidationResult::from(iri_string::validate::iri::<UriSpec>(val)),
            Self::UriReference => {
                ValidationResult::from(iri_string::validate::iri_reference::<UriSpec>(val))
            }
            Self::IriReference => {
                ValidationResult::from(iri_string::validate::iri_reference::<IriSpec>(val))
            }
            Self::UriTemplate => {
                ValidationResult::from(URI_TEMPLATE_RE.is_match(val).unwrap_or(false))
            }
            Self::JsonPointer => {
                ValidationResult::from(JSON_POINTER_RE.is_match(val).unwrap_or(false))
            }
            Self::Regex => ValidationResult::from(Regex::new(val)),
            Self::RelativeJsonPointer => {
                ValidationResult::from(RELATIVE_JSON_POINTER_RE.is_match(val).unwrap_or(false))
            }
            Self::Integer => ValidationResult::from(
                BigDecimal::from_str(val)
                    .map(|d| d.is_integer())
                    .unwrap_or(false),
            ),
            Self::Number => ValidationResult::from(
                BigDecimal::from_str(val).is_ok()
                    || ["NaN", "Infinity", "-Infinity"].contains(&val),
            ),
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
            ("datetime", "2022-09-11T10:31:25.123Z", true), // Accepted alias.
            ("date-time", "10:31:25.123Z", false),
            ("time", "10:31:25.123Z", true),
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
            ("integer", "1_234", true),
            ("integer", "1.234", false),
            ("integer", " 1234", false),
            ("number", "1234", true),
            ("number", "-1234", true),
            ("number", "1_234", true),
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
                    panic!("expected {format:?} with {value} to be valid, but it's invalid with {reason:?}")
                }
            }
        }
    }
}
