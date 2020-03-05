use super::{index, types, Annotation, Application, CoreAnnotation, Keyword, Schema, Validation};
use itertools::{self, EitherOrBoth};

#[derive(Debug)]
pub struct Inference {
    pub ptr: String,
    pub is_pattern: bool,
    pub type_set: types::Set,
    pub is_base64: bool,
    pub content_type: Option<String>,
    pub format: Option<String>,
}

fn fold<I>(v: Vec<Inference>, it: I) -> Vec<Inference>
where
    I: Iterator<Item = Inference>,
{
    itertools::merge_join_by(v.into_iter(), it, |lhs, rhs| lhs.ptr.cmp(&rhs.ptr))
        .map(|eob| -> Inference {
            match eob {
                EitherOrBoth::Both(lhs, rhs) => Inference {
                    ptr: lhs.ptr,
                    is_pattern: lhs.is_pattern,
                    type_set: lhs.type_set & rhs.type_set,
                    is_base64: lhs.is_base64 || rhs.is_base64,
                    content_type: if lhs.content_type.is_some() {
                        lhs.content_type
                    } else {
                        rhs.content_type
                    },
                    format: if lhs.format.is_some() {
                        lhs.format
                    } else {
                        rhs.format
                    },
                },
                EitherOrBoth::Left(lhs) => lhs,
                EitherOrBoth::Right(rhs) => rhs,
            }
        })
        .collect()
}

fn prefix<I>(pre: String, is_pattern: bool, it: I) -> impl Iterator<Item = Inference>
where
    I: Iterator<Item = Inference>,
{
    it.map(move |i| Inference {
        ptr: pre.chars().chain(i.ptr.chars()).collect(),
        is_pattern: is_pattern || i.is_pattern,
        type_set: i.type_set,
        is_base64: i.is_base64,
        content_type: i.content_type,
        format: i.format,
    })
}

pub fn extract<'s, A>(
    schema: &'s Schema<A>,
    idx: &index::Index<'s, A>,
    location_must_exist: bool,
) -> Result<impl Iterator<Item = Inference>, index::Error>
where
    A: Annotation,
{
    let mut local = Inference {
        ptr: String::new(),
        is_pattern: false,
        type_set: types::ANY,
        is_base64: false,
        content_type: None,
        format: None,
    };

    let mut min_items = 0;
    let mut required_props = 0;

    // Walk validation and annotation keywords which affect the inference result
    // at the current location.
    for kw in &schema.kw {
        match kw {
            Keyword::Validation(Validation::Type(type_set)) => {
                if location_must_exist {
                    local.type_set = *type_set;
                } else {
                    local.type_set = types::NULL | *type_set;
                }
            }
            Keyword::Validation(Validation::MinItems(m)) => {
                min_items = *m; // Track for later use.
            }
            Keyword::Validation(Validation::Required(r)) => {
                required_props = *r; // Track for later use.
            }
            Keyword::Annotation(annot) => match annot.as_core() {
                Some(CoreAnnotation::ContentEncodingBase64) => {
                    local.is_base64 = true;
                }
                Some(CoreAnnotation::ContentMediaType(mt)) => {
                    local.content_type = Some(mt.clone());
                }
                _ => {} // Other CoreAnnotation. No-op.
            },
            _ => {} // Not a CoreAnnotation. No-op.
        }
    }

    let mut out = vec![local];

    // Repeatedly extract and merge inference results from
    // in-place and child applications.

    for kw in &schema.kw {
        let (app, sub) = match kw {
            Keyword::Application(app, sub) => (app, sub),
            _ => continue, // No-op.
        };

        match app {
            Application::Ref(uri) => {
                out = fold(
                    out,
                    extract(idx.must_fetch(uri)?, idx, location_must_exist)?,
                );
            }
            Application::AllOf { .. } => {
                out = fold(out, extract(sub, idx, location_must_exist)?);
            }
            Application::Properties {
                name,
                name_interned,
            } => {
                let prop_must_exist = location_must_exist && (required_props & name_interned) != 0;

                out = fold(
                    out,
                    prefix(
                        format!("/{}", name),
                        false,
                        extract(sub, idx, prop_must_exist)?,
                    ),
                );
            }
            /*
            Application::PatternProperties{re} => {
                // TODO(johnny): This is probably wrong; fix me!
                let mut pat = re.as_str().to_owned();
                if pat.starts_with("^") {
                    pat.drain(0..1);
                } else {
                    pat = format!(r"[^/]*");
                }

                out = fold(out, prefix(
                    format!("/{}", pat),
                    true,
                    extract(sub, idx, false)?));
            }
            */
            Application::Items { index: None } => {
                out = fold(
                    out,
                    prefix(r"/\d+".to_owned(), true, extract(sub, idx, false)?),
                );
            }
            Application::Items { index: Some(index) } => {
                let item_must_exist = location_must_exist && min_items > *index;

                out = fold(
                    out,
                    prefix(
                        format!("/{}", index),
                        false,
                        extract(sub, idx, item_must_exist)?,
                    ),
                );
            }
            _ => continue,
        };
    }

    Ok(out.into_iter())
}
