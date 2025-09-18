use crate::{
    node::{Field, Fields},
    schema::{self, types, Annotation, Keyword, Schema},
    AsNode, Node, Number,
};
use bitvec::{order::Lsb0, view::BitView};
use itertools::Itertools;
use std::cmp::Ordering;

#[derive(Debug, Clone, Copy)]
pub enum Outcome<'s, A: Annotation> {
    Annotation(&'s A),
    AnyOfNotMatched,
    Invalid(&'s Keyword<A>),
    NotIsValid,
    OneOfMultipleMatched,
    OneOfNotMatched,
    ReferenceNotFound(&'s Keyword<A>),
    MissingRequiredProperty(&'s str),
}

struct Frame<'s, A>
where
    A: Annotation,
{
    // Index of this Frame's parent within the stack.
    parent_frame: u32,
    // Keyword which created this Frame. None for the root Frame.
    parent_keyword: Option<&'s Keyword<A>>,
    // Keywords being evaluated by this Frame.
    keywords: &'s [Keyword<A>],

    // `invalid` is set if a validation error occurred.
    invalid: bool,
    // Outcomes of this Frame and its unwound children, as (tape-index, outcome).
    outcomes: Vec<(i32, Outcome<'s, A>)>,
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
    // child was examined by "properties", "patternProperties",
    // "additionalProperties", "prefixItems", "items", or "contains".
    // `unevaluated` is None if evaluated children are not being tracked
    // (because there are no unevaluatedItems/Properties keywords in this Frame
    // or an in-place parent).
    unevaluated: Option<Box<[u32]>>,

    // Bit field of speculative validation outcomes of children under an
    // unevaluatedItems/Properties keyword of this Frame.
    // None if there is no such keyword in this Frame.
    invalid_unevaluated: Option<Box<[u32]>>,

    // Outcomes of Keyword::UnevaluatedItems/Properties of *this* Frame,
    // as (child-index, tape-index, outcome).
    outcomes_unevaluated: Vec<(u32, i32, Outcome<'s, A>)>,

    properties_index: u32,
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

fn walk<'s, 'n, N, A, F>(
    active: &mut Vec<u32>,
    child_index: u32,
    filter: &F,
    index: &schema::Index<'s, A>,
    node: &'n N,
    property: &str,
    stack: &mut Vec<Frame<'s, A>>,
    tape_index: &mut i32,
) where
    A: Annotation,
    F: Fn(Outcome<'s, A>) -> Option<Outcome<'s, A>>,
    N: AsNode,
{
    match node.as_node() {
        Node::Array(items) => {
            let arr_tape_index = *tape_index;

            *tape_index += 1; // Consume self.
            for (child_index, item) in items.iter().enumerate() {
                if push_item(active, child_index, filter, index, node, stack, *tape_index) {
                    // recursive walk
                } else {
                    // consume child but don't walk
                }

                validator.push_item(*tape_index, i as u32);
                walk(validator, tape_index, item);
            }
            *tape_index += doc.tape_length(); // Consume self and children.

            validator.end_array(arr_tape_index, items);
        }
        Node::Object(fields) => {
            let obj_tape_index = *tape_index;

            if validator.begin_object(fields.len()) {
                *tape_index += 1; // Consume self.
                for field in fields.iter() {
                    // validator.push_property(*tape_index, field.property());
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

fn push_property<'n, 's, A, F, N>(
    active: &mut Vec<u32>,
    child_index: usize,
    filter: &F,
    index: &schema::Index<'s, A>,
    node: &'n N,
    property: &str,
    stack: &mut Vec<Frame<'s, A>>,
    tape_index: i32,
) -> bool
where
    A: Annotation,
    F: Fn(Outcome<'s, A>) -> Option<Outcome<'s, A>>,
    N: AsNode,
{
    let active_begin = *active.last().unwrap() as usize;
    let active_end = stack.len();

    // Push propertyNames applications to evaluate the property name.
    for frame in active_begin..active_end {
        for kw in stack[frame].keywords {
            if let Keyword::PropertyNames { property_names } = &kw {
                wind_frame(
                    filter,
                    index,
                    node,
                    frame as u32,
                    Some(kw),
                    property_names,
                    stack,
                    tape_index,
                    0,
                );
            }
        }
    }

    if stack.len() != active_end {
        // Mark eval_begin..eval_end as inactive.
        active.push(active_end as u32);

        pop_string(
            &mut stack[active_begin..active_end],
            filter,
            property,
            tape_index,
        );
        pop_node(
            &mut stack[active_begin..active_end],
            filter,
            Node::<N>::String(property),
            types::STRING,
            tape_index,
        );
        unwind(active_end, child_index, filter, stack, tape_index);
        active.pop();
    }

    for frame in active_begin..active_end {
        let mut evaluated = false;

        for kw in stack[frame].keywords {
            // Property applications have preference rules (which keywords are sorted by).
            // C.f. https://json-schema.org/draft/2019-09/json-schema-core.html#rfc.section.9.3.2
            match kw {
                Keyword::Properties { properties } => {
                    for (next, schema) in &(*properties)[stack[frame].properties_index as usize..] {
                        match next[1..].cmp(property) {
                            Ordering::Less => {
                                if next.as_bytes()[0] != b'?' {
                                    if let Some(outcome) =
                                        filter(Outcome::MissingRequiredProperty(&next[1..]))
                                    {
                                        stack[frame].outcomes.push((tape_index, outcome));
                                    }
                                    stack[frame].invalid = true;
                                }
                                stack[frame].properties_index += 1;
                            }
                            Ordering::Equal => {
                                wind_frame(
                                    filter,
                                    index,
                                    node,
                                    frame as u32,
                                    Some(kw),
                                    schema,
                                    stack,
                                    tape_index,
                                    0,
                                );
                                evaluated = true;
                                stack[frame].properties_index += 1;
                                break;
                            }
                            Ordering::Greater => {
                                break;
                            }
                        }
                    }
                }
                Keyword::PatternProperties { pattern_properties } => {
                    for (pattern, schema) in &**pattern_properties {
                        if pattern.is_match(property) {
                            wind_frame(
                                filter,
                                index,
                                node,
                                frame as u32,
                                Some(kw),
                                schema,
                                stack,
                                tape_index,
                                0,
                            );
                            evaluated = true;
                        }
                    }
                }
                Keyword::AdditionalProperties {
                    additional_properties,
                } => {
                    if !evaluated {
                        wind_frame(
                            filter,
                            index,
                            node,
                            frame as u32,
                            Some(kw),
                            additional_properties,
                            stack,
                            tape_index,
                            0,
                        );
                        evaluated = true;
                    }
                }
                Keyword::UnevaluatedProperties {
                    unevaluated_properties,
                } => {
                    if !evaluated {
                        wind_frame(
                            filter,
                            index,
                            node,
                            frame as u32,
                            Some(kw),
                            unevaluated_properties,
                            stack,
                            tape_index,
                            0,
                        );
                    }
                }
                _ => (),
            }
        }

        if let Some(unevaluated) = stack[frame].unevaluated.as_mut() {
            if !evaluated {
                unevaluated
                    .view_bits_mut::<Lsb0>()
                    .set(child_index as usize, true);
            }
        }
    }

    if stack.len() != active_end {
        active.push(active_end as u32);
        return true;
    } else {
        return false;
    }
}

fn push_item<'n, 's, A, F, N>(
    active: &mut Vec<u32>,
    child_index: usize,
    filter: &F,
    index: &schema::Index<'s, A>,
    node: &'n N,
    stack: &mut Vec<Frame<'s, A>>,
    tape_index: i32,
) -> bool
where
    A: Annotation,
    F: Fn(Outcome<'s, A>) -> Option<Outcome<'s, A>>,
    N: AsNode,
{
    let active_begin = *active.last().unwrap() as usize;
    let active_end = stack.len();

    for frame in active_begin..active_end {
        let mut evaluated = false;

        for kw in stack[frame].keywords {
            // Property applications have preference rules (which keywords are sorted by).
            // C.f. https://json-schema.org/draft/2019-09/json-schema-core.html#rfc.section.9.3.2
            match kw {
                Keyword::PrefixItems { prefix_items } => {
                    let Some(schema) = prefix_items.get(child_index as usize) else {
                        continue;
                    };

                    wind_frame(
                        filter,
                        index,
                        node,
                        frame as u32,
                        Some(kw),
                        schema,
                        stack,
                        tape_index,
                        0,
                    );
                    evaluated = true;
                }
                Keyword::Items { items } => {
                    if !evaluated {
                        wind_frame(
                            filter,
                            index,
                            node,
                            frame as u32,
                            Some(kw),
                            items,
                            stack,
                            tape_index,
                            0,
                        );
                        evaluated = true;
                    }
                }
                Keyword::Contains { contains } => {
                    wind_frame(
                        filter,
                        index,
                        node,
                        frame as u32,
                        Some(kw),
                        contains,
                        stack,
                        tape_index,
                        0,
                    );
                    evaluated = true;
                }
                Keyword::UnevaluatedItems { unevaluated_items } => {
                    if !evaluated {
                        wind_frame(
                            filter,
                            index,
                            node,
                            frame as u32,
                            Some(kw),
                            unevaluated_items,
                            stack,
                            tape_index,
                            0,
                        );
                    }
                }
                _ => (),
            }
        }

        if let Some(unevaluated) = stack[frame].unevaluated.as_mut() {
            if !evaluated {
                unevaluated
                    .view_bits_mut::<Lsb0>()
                    .set(child_index as usize, true);
            }
        }
    }

    if stack.len() != active_end {
        active.push(active_end as u32);
        return true;
    } else {
        return false;
    }
}

fn wind_frame<'n, 's, A, F, N>(
    filter: &F,
    index: &schema::Index<'s, A>,
    node: &'n N,
    parent_frame: u32,
    parent_keyword: Option<&'s Keyword<A>>,
    schema: &'s Schema<A>,
    stack: &mut Vec<Frame<'s, A>>,
    tape_index: i32,
    mut track_unevaluated: usize,
) where
    A: Annotation,
    F: Fn(Outcome<'s, A>) -> Option<Outcome<'s, A>>,
    N: AsNode,
{
    let frame = stack.len();
    // TODO: if `frame` is too deep then mark parent frame as invalid, with a "too deep" outcome.

    let keywords = &*schema.kw;

    // Determine if this Frame has an active UnevaluatedItems or
    // UnevaluatedProperties keyword. If so, we must track validation status
    // of the keyword for each child, as that status will be required if it
    // turns out the child was unevaluated by this Frame or its children.
    let mut track_invalid_unevaluated: usize = 0;

    for kw in keywords {
        match kw {
            Keyword::UnevaluatedItems { .. } => match node.as_node() {
                Node::Array(items) => {
                    track_invalid_unevaluated = items.len();
                }
                _ => (), // Wrong node type.
            },
            Keyword::UnevaluatedProperties { .. } => match node.as_node() {
                Node::Object(fields) => {
                    track_invalid_unevaluated = fields.len();
                }
                _ => (), // Wrong node type.
            },
            _ => (), // Not an Unevaluated keyword.
        }
    }

    // If we're performing speculative Unevaluated* validations, then this Frame
    // and its children must track children which were not otherwise evaluated.
    if track_invalid_unevaluated != 0 {
        track_unevaluated = track_invalid_unevaluated;
    }

    // Pre-allocate bit fields when tracking validations of children.
    let unevaluated = if track_unevaluated != 0 {
        Some(vec![0u32; (track_unevaluated + 31) / 32].into_boxed_slice())
    } else {
        None
    };
    let invalid_unevaluated = if track_invalid_unevaluated != 0 {
        Some(vec![0u32; (track_invalid_unevaluated + 31) / 32].into_boxed_slice())
    } else {
        None
    };

    stack.push(Frame {
        parent_frame,
        parent_keyword,
        keywords,
        invalid: false,
        outcomes: Vec::new(),
        valid_if: false,
        valid_any_of: false,
        valid_one_of: false,
        valid_contains: 0,
        unevaluated,
        invalid_unevaluated,
        outcomes_unevaluated: Vec::new(),
        properties_index: 0,
    });

    // Look for in-place applications which also need to be wound.
    // Use a helper macro to reduce repetition in wind_frame calls.
    macro_rules! wind {
        ($kw:expr, $schema:expr) => {
            wind_frame(
                filter,
                index,
                node,
                frame as u32,
                Some($kw),
                $schema,
                stack,
                tape_index,
                track_unevaluated,
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
            Keyword::DependentSchemas { dependent_schemas } => {
                if let Node::Object(fields) = node.as_node() {
                    for (prop, schema) in &**dependent_schemas {
                        if fields.get(prop).is_some() {
                            wind!(kw, schema);
                        }
                    }
                }
            }
            Keyword::DynamicRef { dynamic_ref } => {
                if let Some(referent) = resolve_dynamic_ref(dynamic_ref, frame, index, true, stack)
                {
                    wind!(kw, referent);
                } else {
                    if let Some(outcome) = filter(Outcome::ReferenceNotFound(kw)) {
                        stack[frame].outcomes.push((tape_index, outcome));
                    }
                    stack[frame].invalid = true;
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
                if let Some(referent) = index.fetch(r#ref) {
                    wind!(kw, referent);
                } else {
                    if let Some(outcome) = filter(Outcome::ReferenceNotFound(kw)) {
                        stack[frame].outcomes.push((tape_index, outcome));
                    }
                    stack[frame].invalid = true;
                }
            }
            Keyword::Then { then } => wind!(kw, then),

            _ => (), // Not an in-place application.
        }
    }
}

#[inline]
fn pop_array<'n, 's, A, F, N>(frames: &mut [Frame<'s, A>], filter: &F, items: &[N], tape_index: i32)
where
    A: Annotation,
    F: Fn(Outcome<'s, A>) -> Option<Outcome<'s, A>>,
    N: AsNode,
{
    for frame in frames {
        for kw in frame.keywords {
            let invalid: Option<Outcome<'s, A>> = match kw {
                Keyword::MaxItems { max_items } => {
                    (items.len() > *max_items).then_some(Outcome::Invalid(kw))
                }
                Keyword::MinItems { min_items } => {
                    (items.len() < *min_items).then_some(Outcome::Invalid(kw))
                }
                Keyword::UniqueItems {} => {
                    let mut sorted = items.iter().collect::<Vec<_>>();
                    sorted.sort_by(|a, b| crate::node::compare(*a, *b));

                    sorted
                        .iter()
                        .tuple_windows()
                        .any(|(a, b)| crate::node::compare(*a, *b).is_eq())
                        .then_some(Outcome::Invalid(kw))
                }
                Keyword::MinContains { min_contains } => {
                    (frame.valid_contains < *min_contains as u32).then_some(Outcome::Invalid(kw))
                }
                Keyword::MaxContains { max_contains } => {
                    (frame.valid_contains > *max_contains as u32).then_some(Outcome::Invalid(kw))
                }
                Keyword::UnevaluatedItems { .. } => {
                    pop_unevaluated(frame);
                    None
                }
                _ => None,
            };

            if let Some(outcome) = invalid {
                if let Some(outcome) = filter(outcome) {
                    frame.outcomes.push((tape_index, outcome));
                }
                frame.invalid = true;
            }
        }
    }
}

#[inline]
fn pop_object<'n, 's, A, F, N>(
    frames: &mut [Frame<'s, A>],
    filter: &F,
    fields: &[N::Fields],
    tape_index: i32,
) where
    A: Annotation,
    F: Fn(Outcome<'s, A>) -> Option<Outcome<'s, A>>,
    N: AsNode,
    N::Fields: Sized,
{
    for frame in frames {
        for kw in frame.keywords {
            let invalid: Option<Outcome<'s, A>> = match kw {
                Keyword::MaxProperties { max_properties } => {
                    (fields.len() > *max_properties).then_some(Outcome::Invalid(kw))
                }
                Keyword::MinProperties { min_properties } => {
                    (fields.len() < *min_properties).then_some(Outcome::Invalid(kw))
                }
                Keyword::Properties { properties } => {
                    // Fail if any remaining, un-walked properties were required.
                    (*properties)[frame.properties_index as usize..]
                        .iter()
                        .filter_map(|(property, _)| {
                            if property.as_bytes()[0] != b'?' {
                                Some(Outcome::MissingRequiredProperty(&(&**property)[1..]))
                            } else {
                                None
                            }
                        })
                        .next()
                }
                Keyword::UnevaluatedProperties { .. } => {
                    pop_unevaluated(frame);
                    None
                }
                _ => None,
            };

            if let Some(outcome) = invalid {
                if let Some(outcome) = filter(outcome) {
                    frame.outcomes.push((tape_index, outcome));
                }
                frame.invalid = true;
            }
        }
    }
}

#[inline]
fn pop_string<'s, A, F>(frames: &mut [Frame<'s, A>], filter: &F, val: &str, tape_index: i32)
where
    A: Annotation,
    F: Fn(Outcome<'s, A>) -> Option<Outcome<'s, A>>,
{
    for frame in frames {
        for kw in frame.keywords {
            let invalid: Option<Outcome<'s, A>> = match kw {
                Keyword::Format { format } => {
                    (!format.validate(val)).then_some(Outcome::Invalid(kw))
                }
                Keyword::MaxLength { max_length } => {
                    (val.chars().count() > *max_length as usize).then_some(Outcome::Invalid(kw))
                }
                Keyword::MinLength { min_length } => {
                    (val.chars().count() < *min_length as usize).then_some(Outcome::Invalid(kw))
                }
                Keyword::Pattern { pattern } => {
                    (!pattern.is_match(val)).then_some(Outcome::Invalid(kw))
                }
                _ => None,
            };

            if let Some(outcome) = invalid {
                if let Some(outcome) = filter(outcome) {
                    frame.outcomes.push((tape_index, outcome));
                }
                frame.invalid = true;
            }
        }
    }
}

#[inline]
fn pop_number<'s, A, F>(
    frames: &mut [Frame<'s, A>],
    filter: &F,
    val: crate::Number,
    tape_index: i32,
) where
    A: Annotation,
    F: Fn(Outcome<'s, A>) -> Option<Outcome<'s, A>>,
{
    for frame in frames {
        for kw in frame.keywords {
            let invalid: Option<Outcome<'s, A>> = match kw {
                Keyword::Minimum { minimum } => (val < *minimum).then_some(Outcome::Invalid(kw)),
                Keyword::Maximum { maximum } => (val > *maximum).then_some(Outcome::Invalid(kw)),
                Keyword::ExclusiveMinimum { exclusive_minimum } => {
                    (val <= *exclusive_minimum).then_some(Outcome::Invalid(kw))
                }
                Keyword::ExclusiveMaximum { exclusive_maximum } => {
                    (val >= *exclusive_maximum).then_some(Outcome::Invalid(kw))
                }
                Keyword::MultipleOf { multiple_of } => {
                    (!val.is_multiple_of(multiple_of)).then_some(Outcome::Invalid(kw))
                }

                _ => None,
            };

            if let Some(outcome) = invalid {
                if let Some(outcome) = filter(outcome) {
                    frame.outcomes.push((tape_index, outcome));
                }
                frame.invalid = true;
            }
        }
    }
}

#[inline]
fn pop_node<'n, 's, A, F, N>(
    frames: &mut [Frame<'s, A>],
    filter: &F,
    node: Node<'n, N>,
    node_type: types::Set,
    tape_index: i32,
) where
    A: Annotation,
    F: Fn(Outcome<'s, A>) -> Option<Outcome<'s, A>>,
    N: AsNode,
{
    for frame in frames {
        for kw in frame.keywords {
            let invalid: Option<Outcome<'s, A>> = match kw {
                // Keywords that are common across all node types.
                Keyword::False => Some(Outcome::Invalid(kw)),
                Keyword::Type { r#type } => {
                    (*r#type & node_type == types::INVALID).then_some(Outcome::Invalid(kw))
                }
                Keyword::Const { r#const } => {
                    (!crate::node::compare_node(&node, &r#const.as_node()).is_eq())
                        .then_some(Outcome::Invalid(kw))
                }
                Keyword::Enum { r#enum } => r#enum
                    .iter()
                    .all(|r#enum| !crate::node::compare_node(&node, &r#enum.as_node()).is_eq())
                    .then_some(Outcome::Invalid(kw)),
                Keyword::AnyOf { .. } => (!frame.valid_any_of).then_some(Outcome::AnyOfNotMatched),
                Keyword::OneOf { .. } => (!frame.valid_one_of).then_some(Outcome::OneOfNotMatched),

                _ => None,
            };

            if let Some(outcome) = invalid {
                if let Some(outcome) = filter(outcome) {
                    frame.outcomes.push((tape_index, outcome));
                }
                frame.invalid = true;
            }

            // Note that Annotation is ordered after all validation keywords.
            if let Keyword::Annotation { annotation } = kw {
                if !frame.invalid {
                    if let Some(outcome) = filter(Outcome::Annotation(annotation)) {
                        frame.outcomes.push((tape_index, outcome));
                    }
                }
            }
        }
    }
}

#[cold]
#[inline(never)]
fn pop_unevaluated<'s, A: Annotation>(frame: &mut Frame<'s, A>) {
    let invalid_unevaluated = frame
        .invalid_unevaluated
        .as_ref()
        .unwrap()
        .view_bits::<Lsb0>();
    let unevaluated = frame.unevaluated.as_mut().unwrap().view_bits_mut::<Lsb0>();

    // Remove outcomes from speculative unevaluatedProperties/Items applications
    // where the child was in fact evaluated elsewhere. Then apply the remainder
    // (from actually-unevaluated children) to outcomes.
    frame
        .outcomes
        .extend(frame.outcomes_unevaluated.drain(..).filter_map(
            |(child_index, tape_index, outcome)| {
                if unevaluated[child_index as usize] {
                    Some((tape_index, outcome))
                } else {
                    None
                }
            },
        ));

    // For each child, if our speculative validation succeeded then the
    // child is no longer unevaluated. Note it's possible that our parent
    // *also* has an unevaluated* keyword, so we need to yield a correct
    // bit-field of unevaluated children.
    *unevaluated &= invalid_unevaluated;

    // If any unevaluated child remains, then it was both unevaluated and
    // also failed speculative validation.
    frame.invalid = unevaluated.any();
}

fn unwind<'n, 's, A, F>(
    bound: usize,
    child_index: u32,
    filter: &F,
    stack: &mut Vec<Frame<'s, A>>,
    tape_index: i32,
) where
    A: Annotation,
    F: Fn(Outcome<'s, A>) -> Option<Outcome<'s, A>>,
{
    while stack.len() != bound as usize {
        unwind_frame(child_index, filter, stack, tape_index);
    }
}

#[inline]
fn unwind_frame<'n, 's, A, F>(
    child_index: u32,
    filter: &F,
    stack: &mut Vec<Frame<'s, A>>,
    tape_index: i32,
) where
    A: Annotation,
    F: Fn(Outcome<'s, A>) -> Option<Outcome<'s, A>>,
{
    let mut frame = stack.pop().unwrap();
    let parent = &mut stack[frame.parent_frame as usize];

    let required: bool; // Invalid `frame` also invalidates `parent`.
    let in_place: bool; // `frame` is an in-place application such as $ref.

    match frame.parent_keyword.unwrap() {
        Keyword::Not { .. } => {
            frame.outcomes.clear();

            if !frame.invalid {
                if let Some(outcome) = filter(Outcome::NotIsValid) {
                    frame.outcomes.push((tape_index, outcome));
                }
            }
            frame.invalid = !frame.invalid;

            (required, in_place) = (true, true);
        }
        Keyword::AllOf { .. }
        | Keyword::Ref { .. }
        | Keyword::DynamicRef { .. }
        | Keyword::DependentSchemas { .. } => {
            (required, in_place) = (true, true);
        }
        Keyword::If { .. } => {
            parent.valid_if = !frame.invalid;
            (required, in_place) = (false, true);
        }
        Keyword::AnyOf { .. } => {
            parent.valid_any_of |= !frame.invalid;
            (required, in_place) = (false, true);
        }
        Keyword::OneOf { .. } => {
            if parent.valid_one_of {
                if let Some(outcome) = filter(Outcome::OneOfMultipleMatched) {
                    parent.outcomes.push((tape_index, outcome));
                }
                parent.invalid = true;
            }
            parent.valid_one_of |= !frame.invalid;
            (required, in_place) = (false, true);
        }
        Keyword::Contains { .. } => {
            if !frame.invalid {
                parent.valid_contains += 1;
            }
            (required, in_place) = (false, false);
        }
        Keyword::Then { .. } => {
            (required, in_place) = (parent.valid_if, true);
        }
        Keyword::Else { .. } => {
            (required, in_place) = (!parent.valid_if, true);
        }
        Keyword::Pattern { .. }
        | Keyword::PatternProperties { .. }
        | Keyword::PrefixItems { .. }
        | Keyword::Items { .. }
        | Keyword::Properties { .. }
        | Keyword::PropertyNames { .. }
        | Keyword::AdditionalProperties { .. } => {
            (required, in_place) = (true, false);
        }

        Keyword::UnevaluatedItems { .. } | Keyword::UnevaluatedProperties { .. } => {
            if frame.invalid {
                parent
                    .invalid_unevaluated
                    .as_mut()
                    .unwrap()
                    .view_bits_mut::<Lsb0>()
                    .set(child_index as usize, true);
            }
            parent.outcomes_unevaluated.extend(
                frame
                    .outcomes
                    .drain(..)
                    .map(|(tape_index, outcome)| (child_index, tape_index, outcome)),
            );
            return;
        }

        _ => return,
    };

    parent.invalid |= required && frame.invalid;

    if required || !frame.invalid {
        if parent.outcomes.is_empty() {
            std::mem::swap(&mut parent.outcomes, &mut frame.outcomes);
        } else {
            parent.outcomes.extend(frame.outcomes.into_iter());
        }
    }
    if in_place && !frame.invalid && parent.unevaluated.is_some() {
        *parent.unevaluated.as_mut().unwrap().view_bits_mut::<Lsb0>() &=
            frame.unevaluated.as_ref().unwrap().view_bits::<Lsb0>();
    }
}

fn resolve_dynamic_ref<'s, A: Annotation>(
    dynamic_ref: &'s str,
    frame: usize,
    index: &schema::Index<'s, A>,
    scope_change: bool,
    stack: &[Frame<'s, A>],
) -> Option<&'s Schema<A>> {
    // Walk up through each parent to the root, then walk back down propagating
    // a resolution supplied by a parent first.
    if let Some(kw) = stack[frame].parent_keyword {
        if let Some(schema) = resolve_dynamic_ref(
            dynamic_ref,
            stack[frame].parent_frame as usize,
            index,
            // Only $ref keywords can change the dynamic scope.
            // If this is not a $ref, then the parent's base URI is the same
            // as ours and an index lookup will have the same result.
            matches!(kw, Keyword::Ref { .. }),
            stack,
        ) {
            return Some(schema);
        }
    }
    // No parent was able to resolve the dynamic_ref.

    if scope_change {
        let id = stack[frame].keywords.first().unwrap();
        let Keyword::Id { curi, .. } = id else {
            panic!("Keyword::Id must be first Schema keyword");
        };

        url::Url::parse(curi)
            .unwrap()
            .join(dynamic_ref)
            .ok()
            .and_then(|url| index.fetch(url.as_str()))
    } else {
        None // Let our child query `index`.
    }
}
