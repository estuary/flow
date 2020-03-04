use super::{index, types, Annotation, CoreAnnotation, Keyword, Schema, Validation, Application};
use itertools::{self, EitherOrBoth};

#[derive(Debug)]
pub struct Inference {
    ptr: String,
    is_pattern: bool,
    type_set: types::Set,
    is_base64: bool,
    content_type: Vec<String>,
    format: Vec<String>,
}

fn fold<I>(v: Vec<Inference>, it: I) -> Vec<Inference>
where
    I: Iterator<Item = Inference>,
{
    itertools::merge_join_by(
        v.into_iter(),
        it,
        |lhs, rhs| lhs.ptr.cmp(&rhs.ptr),
    ).map(|eob| -> Inference {
        match eob {
            EitherOrBoth::Both(mut lhs, rhs) => Inference {
                ptr: lhs.ptr,
                is_pattern: lhs.is_pattern,
                type_set: lhs.type_set & rhs.type_set,
                is_base64: lhs.is_base64 || rhs.is_base64,
                content_type: {
                    lhs.content_type.extend(rhs.content_type.into_iter());
                    lhs.content_type
                },
                format: {
                    lhs.format.extend(rhs.format.into_iter());
                    lhs.format
                }
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
) -> Result<impl Iterator<Item = Inference>, index::Error> // TODO: fails using impl IntoIterator.
where
    A: Annotation,
{
    let mut local = Inference {
        ptr: String::new(),
        is_pattern: false,
        type_set: types::ANY,
        is_base64: false,
        content_type: Vec::new(),
        format: Vec::new(),
    };

    let mut min_items = 0;

    // Walk validation and annotation keywords which affect the inference result
    // at the current location.
    for kw in &schema.kw {
        match kw {
            Keyword::Validation(Validation::Type(type_set)) => {
                local.type_set = *type_set;
            }
            Keyword::Validation(Validation::MinItems(m)) => {
                min_items = *m; // Track for later use.
            }
            Keyword::Annotation(annot) => match annot.as_core() {
                Some(CoreAnnotation::ContentEncodingBase64) => {
                    local.is_base64 = true;
                },
                Some(CoreAnnotation::ContentMediaType(mt)) => {
                    local.content_type.push(mt.clone());
                },
                _ => {}, // Other CoreAnnotation. No-op.
            }
            _ => {}, // Not a CoreAnnotation. No-op.
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
                out = fold(out, extract(idx.must_fetch(uri)?, idx)?);
            }
            Application::AllOf{..} => {
                out = fold(out, extract(sub, idx)?);
            }
            Application::Properties{name, ..} => {
                out = fold(out, prefix(
                    format!("/{}", name),
                    false,
                    extract(sub, idx)?));
            }
            Application::PatternProperties{re} => {
                let mut pat = re.as_str().to_owned();
                if pat.starts_with("^") {
                    pat.drain(0..1);
                } else {
                    pat = format!(r"[^/]*");
                }

                out = fold(out, prefix(
                    format!("/{}", pat),
                    true,
                    extract(sub, idx)?));
            }
            Application::Items{index: None} => {
                let bound = if min_items < 3 { min_items } else { 3 };

                for i in 0..bound {
                    out = fold(out, prefix(
                        format!("/{}", i),
                        false,
                        extract(sub, idx)?));
                }
            }
            Application::Items{index: Some(index)} if min_items > *index => {
                out = fold(out, prefix(
                    format!("/{}", index),
                    false,
                    extract(sub, idx)?));
            }
            _ => continue,
        };
    }

    Ok(out.into_iter())
}
