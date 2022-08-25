use std::{net::IpAddr, str::FromStr};

use addr::{parse_domain_name, parse_email_address};
use fancy_regex::Regex;
use iri_string::spec::{IriSpec, UriSpec};
use uuid::Uuid;

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
    static ref JSON_POINTER_RE: Regex = Regex::new(r"[^([^/~]|(~[01]))]+").expect("Is a valid regex");
}

pub fn validate_format(format: &str, val: &str) -> bool {
    match format {
        "date" => {
            time::Date::parse(
                val,
                &time::macros::format_description!("[year]-[month]-[day]"),
            ).is_ok() &&
            // Padding with zeroes is ignored by the underlying parser. The most efficient
            // way to check it will be to use a custom parser that won't ignore zeroes,
            // but this regex will do the trick and costs ~20% extra time in this validator.
            DATE_RE
            .is_match(val).unwrap_or(false)
        }
        "date-time" => {
            time::OffsetDateTime::parse(val, &time::format_description::well_known::Rfc3339).is_ok()
        }
        "time" => time::Time::parse(
            val,
            &time::macros::format_description!("[hour]:[minute]:[second].[subsecond]Z"),
        )
        .is_ok(),
        "email" => parse_email_address(val).is_ok(),
        "hostname" => parse_domain_name(val).is_ok(),
        // The rules/test cases for these are absolutely bonkers
        // If we end up needing this let's revisit (jshearer)
        "idn-hostname" | "idn-email" => {tracing::warn!("Unsupported string format {}", format); return false},
        "ipv4" => {
            if val.starts_with('0') {
                return false;
            }
            match IpAddr::from_str(val) {
                Ok(i) => i.is_ipv4(),
                Err(_) => false,
            }
        }
        "ipv6" => match IpAddr::from_str(val) {
            Ok(i) => i.is_ipv6(),
            Err(_) => false,
        },
        // uuid crate supports non-hyphenated inputs, jsonschema does not
        "uuid" if val.len() == 36 => Uuid::parse_str(val).is_ok(),

        "duration" => match ISO_8601_DURATION_RE.is_match(val) {
            Ok(true) => if val.contains("W") {
                // If we parse as weeks, ensure that ONLY weeks are provided
                ISO_8601_ONLY_WEEKS_RE.is_match(val).unwrap_or(false)
            } else{
                // Otherwise, ensure that NO weeks are provided
                ISO_8601_NO_WEEKS_RE.is_match(val).unwrap_or(false)
            },
            _ => false,
        },
        "iri" => iri_string::validate::iri::<IriSpec>(val).is_ok(),
        "uri" => iri_string::validate::iri::<UriSpec>(val).is_ok(),
        "uri-reference" => iri_string::validate::iri_reference::<UriSpec>(val).is_ok(),
        "iri-reference" => iri_string::validate::iri_reference::<IriSpec>(val).is_ok(),
        "uri-template" => URI_TEMPLATE_RE.is_match(val).unwrap_or(false),
        "json-pointer" => JSON_POINTER_RE.is_match(val).unwrap_or(false),
        "regex" => Regex::new(val).is_ok(),
        "relative-json-pointer" => RELATIVE_JSON_POINTER_RE.is_match(val).unwrap_or(false),
        _ => false,
    }
}
