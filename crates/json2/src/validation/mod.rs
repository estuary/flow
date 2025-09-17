use crate::{
    node::{Field, Fields},
    schema::{self, Annotation, Keyword, Schema},
    AsNode, Node,
};

#[derive(Debug)]
pub enum Outcome<'s, A: Annotation> {
    Annotation(&'s A),
    AnyOfNotMatched,
    Invalid(&'s Keyword<A>),
    NotIsValid,
    OneOfMultipleMatched,
    OneOfNotMatched,
    ReferenceNotFound(&'s Keyword<A>),
}

struct Frame<'s, A>
where
    A: Annotation,
{
    // Parent of this Evaluation, as (evaluations index, keyword).
    parent: Option<(u32, &'s Keyword<A>)>,
    // Keywords being evaluated.
    keywords: &'s [Keyword<A>],
    // Current index within a Keyword::Properties.
    properties_idx: u32,

    // Outcomes of this Frame and its unwound children, as (tape-index, outcome).
    outcomes: Vec<(i32, Outcome<'s, A>)>,
    // `invalid` is true if `outcomes` contains an Outcome other than Annotation.
    invalid: bool,
    // True if a Keyword::If was evaluated and matched.
    valid_if: bool,
    // True if a Keyword::AnyOf in-place application validated.
    valid_any_of: bool,
    // True if a Keyword::OneOf in-place application validated.
    valid_one_of: bool,
    // Number of Keyword::Contains applications that validated against child items.
    valid_contains: u32,

    // Bit field of children that have not been evaluated by this Frame,
    // or an unwound in-place child application thereof. "evaluation" means the
    // child was validated by "properties", "patternProperties",
    // "additionalProperties", "prefixItems", "items", or "contains".
    // `unevaluated` is None if evaluated children are not being tracked
    // (because there are no unevaluatedItems/Properties keywords in this Frame
    // or an in-place parent).
    unevaluated: Option<Box<[u32]>>,
    // Outcomes of Keyword::UnevaluatedItems/Properties of *this* Frame,
    // as (child-index, tape-index, outcome).
    outcomes_unevaluated: Vec<(u32, i32, Outcome<'s, A>)>,
}

pub struct Stack<'s, A>
where
    A: Annotation,
{
    // Index of schemas over canonical URIs and anchors.
    index: &'s schema::Index<'s, A>,
    // Stack-like frames of concurrent evaluations.
    frames: Vec<Frame<'s, A>>,
    // Stack of offsets tracking frames which are active (being evaluated concurrently).
    active_frames: Vec<usize>,
}

impl<'s, A> Stack<'s, A>
where
    A: Annotation,
{
    fn foobar(&mut self) {
        let active_begin = *self.active_frames.last().unwrap();
        let active_end = self.frames.len();

        for eval_idx in (active_end..active_begin).rev() {}
    }

    fn push_property(&mut self, tape_index: i32, property: &str) {
        let active_begin = *self.active_frames.last().unwrap();
        let active_end = self.frames.len();

        // Push propertyNames applications to evaluate the property name.
        for eval_idx in active_begin..active_end {
            for kw in self.frames[eval_idx].keywords {
                if let Keyword::PropertyNames { property_names } = &kw {
                    wind_frame(
                        self,
                        Some((eval_idx as u32, kw)),
                        property_names,
                        tape_index,
                        false,
                    );
                }
            }
        }

        if self.frames.len() != active_end {
            // Mark eval_begin..eval_end as inactive.
            self.active_frames.push(active_end);
            // Apply the property name as a string, which pops propertyName applications.
            self.end_str(tape_index, property);
        }
    }

    fn push_item(&mut self, tape_index: i32, item_index: u32) {}

    fn begin_array(&mut self, num_items: usize) -> bool {
        false
    }
    fn begin_object(&mut self, num_fields: usize) -> bool {
        false
    }

    fn end_array<'n, N: AsNode>(&mut self, tape_index: i32, items: &'n [N]) {}
    fn end_bool(&mut self, tape_index: i32, value: bool) {}
    fn end_bytes<'n>(&mut self, tape_index: i32, value: &'n [u8]) {}
    fn end_null(&mut self, tape_index: i32) {}
    fn end_number<'n, N: AsNode>(&mut self, tape_index: i32, value: Node<'n, N>) {}
    fn end_object<'n, N: AsNode>(&mut self, tape_index: i32, fields: &'n N::Fields) {}
    fn end_str<'n>(&mut self, tape_index: i32, value: &'n str) {}
}

fn walk<'s, 'n, N: AsNode, A: Annotation>(
    validator: &mut Stack<'s, A>,
    tape_index: &mut i32,
    doc: &'n N,
) {
    match doc.as_node() {
        Node::Array(items) => {
            let arr_tape_index = *tape_index;

            if validator.begin_array(items.len()) {
                *tape_index += 1; // Consume self.
                for (i, item) in items.iter().enumerate() {
                    validator.push_item(*tape_index, i as u32);
                    walk(validator, tape_index, item);
                }
            } else {
                *tape_index += doc.tape_length(); // Consume self and children.
            }

            validator.end_array(arr_tape_index, items);
        }
        Node::Object(fields) => {
            let obj_tape_index = *tape_index;

            if validator.begin_object(fields.len()) {
                *tape_index += 1; // Consume self.
                for field in fields.iter() {
                    validator.push_property(*tape_index, field.property());
                    walk(validator, tape_index, field.value());
                }
            } else {
                *tape_index += doc.tape_length(); // Consume self and children.
            }

            validator.end_object::<N>(obj_tape_index, fields);
        }
        Node::Bool(b) => {
            validator.end_bool(*tape_index, b);
            *tape_index += 1;
        }
        Node::Bytes(b) => {
            validator.end_bytes(*tape_index, b);
            *tape_index += 1;
        }
        node @ (Node::Float(_) | Node::NegInt(_) | Node::PosInt(_)) => {
            validator.end_number(*tape_index, node);
            *tape_index += 1;
        }
        Node::Null => {
            validator.end_null(*tape_index);
            *tape_index += 1;
        }
        Node::String(s) => {
            validator.end_str(*tape_index, s);
            *tape_index += 1;
        }
    }
}

fn wind_frame<'s, A: Annotation>(
    stack: &mut Stack<'s, A>,
    parent: Option<(u32, &'s Keyword<A>)>,
    schema: &'s Schema<A>,
    tape_index: i32,
    mut track_validations: bool,
) {
    let frame = stack.frames.len();

    // TODO: bail if `frame` is too deep.

    let keywords = &*schema.kw;

    for kw in keywords {
        match kw {
            Keyword::UnevaluatedItems { .. } | Keyword::UnevaluatedProperties { .. } => {
                // Track evaluated children for this Frame and its in-place applications.
                track_validations = true;
            }
            _ => (), // Not an Unevaluated keyword.
        }
    }

    stack.frames.push(Frame {
        parent,
        keywords,
        properties_idx: 0,

        outcomes: Vec::new(),
        invalid: false,
        valid_if: false,
        valid_any_of: false,
        valid_one_of: false,
        valid_contains: 0,
        unevaluated: if track_validations {
            // This is resized later, upon begin_object/array.
            Some(vec![0].into_boxed_slice())
        } else {
            None
        },
        outcomes_unevaluated: Vec::new(),
    });

    // Look for in-place applications which also need to be wound.
    // Use a helper macro to reduce repetition in wind_frame calls.
    macro_rules! wind {
        ($kw:expr, $schema:expr) => {
            wind_frame(
                stack,
                Some((frame as u32, $kw)),
                $schema,
                tape_index,
                track_validations,
            )
        };
    }

    for kw in keywords {
        match kw {
            Keyword::AllOf { all_of } => {
                for all_of in &**all_of {
                    wind!(kw, all_of);
                }
            }
            Keyword::AnyOf { any_of } => {
                for any_of in &**any_of {
                    wind!(kw, any_of);
                }
            }
            Keyword::DependentSchemas { dependent_schema } => {
                let (_, schema) = dependent_schema.as_ref();
                wind!(kw, schema);
            }
            Keyword::DynamicRef { dynamic_ref } => {
                if let Some(referent) = resolve_dynamic_ref(dynamic_ref, stack, frame, true) {
                    wind!(kw, referent);
                } else {
                    stack.frames[frame]
                        .outcomes
                        .push((tape_index, Outcome::ReferenceNotFound(kw)));
                    stack.frames[frame].invalid = true;
                }
            }
            Keyword::Else { r#else } => wind!(kw, r#else),
            Keyword::If { r#if } => wind!(kw, r#if),
            Keyword::Not { r#not } => wind!(kw, r#not),
            Keyword::OneOf { one_of } => {
                for one_of in &**one_of {
                    wind!(kw, one_of);
                }
            }
            Keyword::Ref { r#ref } => {
                if let Some(referent) = stack.index.fetch(r#ref) {
                    wind!(kw, referent);
                } else {
                    stack.frames[frame]
                        .outcomes
                        .push((tape_index, Outcome::ReferenceNotFound(kw)));
                    stack.frames[frame].invalid = true;
                }
            }
            Keyword::Then { then } => wind!(kw, then),
            _ => (), // Not an in-place application.
        }
    }
}

fn resolve_dynamic_ref<'s, A: Annotation>(
    dynamic_ref: &'s str,
    stack: &Stack<'s, A>,
    frame: usize,
    scope_change: bool,
) -> Option<&'s Schema<A>> {
    // Walk up through each parent to the root, then walk back down propagating
    // a resolution supplied by a parent first.
    if let Some((parent_frame, kw)) = stack.frames[frame].parent {
        if let Some(schema) = resolve_dynamic_ref(
            dynamic_ref,
            stack,
            parent_frame as usize,
            // Only $ref keywords can change the dynamic scope.
            // If this is not a $ref, then the parent's base URI is the same
            // as ours and an index lookup will have the same result.
            matches!(kw, Keyword::Ref { .. }),
        ) {
            return Some(schema);
        }
    }
    // No parent was able to resolve the dynamic_ref.

    if scope_change {
        let id = stack.frames[frame].keywords.first().unwrap();
        let Keyword::Id { curi, .. } = id else {
            panic!("Keyword::Id must be first Schema keyword");
        };

        url::Url::parse(curi)
            .unwrap()
            .join(dynamic_ref)
            .ok()
            .and_then(|url| stack.index.fetch(url.as_str()))
    } else {
        None // Let our child query `index`.
    }
}
