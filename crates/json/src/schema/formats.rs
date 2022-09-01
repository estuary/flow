use std::{net::IpAddr, str::FromStr};

use addr::{parse_domain_name, parse_email_address};
use fancy_regex::Regex;
use iri_string::spec::{IriSpec, UriSpec};
use uuid::Uuid;

use crate::validator::ValidationResult;

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
}

pub fn validate_format(format: &str, val: &str) -> ValidationResult {
    match format {
        "date" => {
            // Padding with zeroes is ignored by the underlying parser. The most efficient
            // way to check it will be to use a custom parser that won't ignore zeroes,
            // but this regex will do the trick and costs ~20% extra time in this validator.
            if !DATE_RE.is_match(val).unwrap_or(false) {
                return ValidationResult::Invalid(None)
            }
            ValidationResult::from(time::Date::parse(
                val,
                &time::macros::format_description!("[year]-[month]-[day]"),
            ))
        }
        "date-time" => {
            ValidationResult::from(time::OffsetDateTime::parse(val, &time::format_description::well_known::Rfc3339))
        }
        "time" => ValidationResult::from(time::Time::parse(
            val,
            &time::macros::format_description!("[hour]:[minute]:[second].[subsecond]Z"),
        )),
        "email" => ValidationResult::from(parse_email_address(val)),
        "hostname" => ValidationResult::from(parse_domain_name(val)),
        // The rules/test cases for these are absolutely bonkers
        // If we end up needing this let's revisit (jshearer)
        "idn-hostname" | "idn-email" => {
            tracing::warn!("Unsupported string format {}", format);
            ValidationResult::Invalid(None)
        }
        "ipv4" => {
            if val.starts_with('0') {
                return ValidationResult::Invalid(None)
            }
            match IpAddr::from_str(val) {
                Ok(i) => ValidationResult::from(i.is_ipv4()),
                Err(e) => ValidationResult::Invalid(Some(e.to_string())),
            }
        }
        "ipv6" => ValidationResult::from(match IpAddr::from_str(val) {
            Ok(i) => ValidationResult::from(i.is_ipv6()),
            Err(e) => ValidationResult::Invalid(Some(e.to_string())),
        }),
        // uuid crate supports non-hyphenated inputs, jsonschema does not
        "uuid" if val.len() == 36 => ValidationResult::from(Uuid::parse_str(val)),

        "duration" => ValidationResult::from(match ISO_8601_DURATION_RE.is_match(val) {
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
        "iri" => ValidationResult::from(iri_string::validate::iri::<IriSpec>(val)),
        "uri" => ValidationResult::from(iri_string::validate::iri::<UriSpec>(val)),
        "uri-reference" => ValidationResult::from(iri_string::validate::iri_reference::<UriSpec>(val)),
        "iri-reference" => ValidationResult::from(iri_string::validate::iri_reference::<IriSpec>(val)),
        "uri-template" => ValidationResult::from(URI_TEMPLATE_RE.is_match(val).unwrap_or(false)),
        "json-pointer" => ValidationResult::from(JSON_POINTER_RE.is_match(val).unwrap_or(false)),
        "regex" => ValidationResult::from(Regex::new(val)),
        "relative-json-pointer" => ValidationResult::from(RELATIVE_JSON_POINTER_RE.is_match(val).unwrap_or(false)),
        _ => ValidationResult::Invalid(None),
    }
}
