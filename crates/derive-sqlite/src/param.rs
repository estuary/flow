use super::Error;
use proto_flow::flow;

/// Projection is a document location that can be projected into a SQLite parameter.
/// This is a proto_flow::flow::Projection that's been mapped into the SQLite domain.
#[derive(Debug, Clone)]
pub struct Param {
    // Projection which is being related as a SQLite parameter.
    pub projection: flow::Projection,
    // Canonical SQLite parameter encoding for this field.
    pub canonical_encoding: String,
    // Extractor of this projection within documents.
    pub extractor: doc::Extractor,
    // Location uses string format: "integer" ?
    pub is_format_integer: bool,
    // Location uses string format: "number" ?
    pub is_format_number: bool,
    // Location uses string contentEncoding: "base64" ?
    pub is_content_encoding_base64: bool,
}

impl Param {
    pub fn new(p: &flow::Projection) -> Result<Self, Error> {
        Ok(Self {
            projection: p.clone(),
            canonical_encoding: canonical_param_encoding(&p.field),
            extractor: extractors::for_projection(&p, &doc::SerPolicy::default())?,
            is_format_integer: matches!(&p.inference, Some(flow::Inference{string: Some(str), ..}) if str.format == "integer"),
            is_format_number: matches!(&p.inference, Some(flow::Inference{string: Some(str), ..}) if str.format == "number"),
            is_content_encoding_base64: matches!(&p.inference, Some(flow::Inference{string: Some(str), ..}) if str.content_encoding == "base64"),
        })
    }

    pub fn resolve<'p>(encoding: &str, params: &'p [Self]) -> Result<&'p Self, Error> {
        let is_explicit = encoding.starts_with(Self::EXPLICIT_PREFIX);

        let field = if is_explicit {
            encoding[Self::EXPLICIT_PREFIX.len()..encoding.len() - 1].to_string()
        } else {
            encoding[1..].replace("$", "/")
        };

        // Look for an exact field match.
        if let Some(param) = params.iter().find(|p| p.projection.field == *field) {
            return Ok(param);
        }

        // Look for a wildcard match, where '_' may substitute for whitespace or '-'.
        for param in params {
            let mut p = param.projection.field.chars();

            if field.chars().all(|f| {
                let p = p.next().unwrap_or_default();
                if f == p {
                    true
                } else if f == '_' && p.is_whitespace() {
                    true
                } else if f == '_' && p == '-' && !is_explicit {
                    true
                } else {
                    false
                }
            }) && p.next().is_none()
            {
                return Ok(param);
            }
        }

        let (_, closest) = params
            .iter()
            .map(|p| {
                (
                    strsim::osa_distance(&field, &p.projection.field),
                    &p.canonical_encoding,
                )
            })
            .min()
            .unwrap();

        Err(Error::ParamNotFound {
            param: encoding.to_string(),
            closest: closest.clone(),
        })
    }

    pub const EXPLICIT_PREFIX: &str = "$p::(";
}

// Map a projection field into its canonical SQLite parameter encoding.
fn canonical_param_encoding(field: &str) -> String {
    let simple = field
        .chars()
        .all(|c| c.is_alphanumeric() || c.is_whitespace() || c == '_' || c == '-' || c == '/');

    let param: String = field
        .chars()
        .map(|c| match c {
            _ if c.is_whitespace() => '_',
            '/' if simple => '$',
            '-' if simple => '_',
            _ => c,
        })
        .collect();

    if simple {
        format!("${param}")
    } else {
        format!("{}{param})", Param::EXPLICIT_PREFIX)
    }
}

#[cfg(test)]
mod test {
    use super::super::test_param;
    use super::Param;

    #[test]
    fn test_field_and_param_conversion() {
        let fixtures = [
            ("Simple", "$Simple"),
            ("under_Score", "$under_Score"),
            ("with space", "$with_space"),
            ("with-hyphen", "$with_hyphen"),
            ("äëïöü/4-2", "$äëïöü$4_2"),
            ("With/Nesting", "$With$Nesting"),
            ("with$Dollar", "$p::(with$Dollar)"),
            (
                "With/Nesting/and+special-hyphen",
                "$p::(With/Nesting/and+special-hyphen)",
            ),
            ("hello, world!", "$p::(hello,_world!)"),
        ];
        let params: Vec<_> = fixtures
            .iter()
            .map(|(field, _)| test_param(field, "", false, false, false))
            .collect();

        for (param, (_, expect_encoding)) in params.iter().zip(fixtures.iter()) {
            // Each field maps into the expected parameter.
            assert_eq!(param.canonical_encoding, *expect_encoding);
            // Each parameter maps back into its expected projection.
            assert_eq!(
                Param::resolve(expect_encoding, &params)
                    .unwrap()
                    .projection
                    .field,
                param.projection.field
            );
        }

        insta::assert_display_snapshot!(
            Param::resolve("$simplee", &params).unwrap_err(),
            @"parameter $simplee not found: did you mean $Simple ?");

        insta::assert_display_snapshot!(
            Param::resolve("$with$nesting", &params).unwrap_err(),
            @"parameter $with$nesting not found: did you mean $With$Nesting ?");

        insta::assert_display_snapshot!(
            Param::resolve("$With$Nesting$and_special_hyphen", &params).unwrap_err(),
            @"parameter $With$Nesting$and_special_hyphen not found: did you mean $p::(With/Nesting/and+special-hyphen) ?");

        insta::assert_display_snapshot!(
            Param::resolve("$way_off", &params).unwrap_err(),
            @"parameter $way_off not found: did you mean $Simple ?");
    }
}
