use crate::{
    node::{Field, Fields},
    schema::{self, Annotation, Keyword, Schema},
    AsNode, Node,
};
use bitvec::{order::Lsb0, view::BitView};

#[derive(Debug, Clone, Copy)]
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

    /*
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
    */

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

fn unwind_frame<'n, 's, A, F, N>(
    filter: &F,
    node: &'n N,
    schema: &'s Schema<A>,
    stack: &mut Vec<Frame<'s, A>>,
    tape_index: i32,
    child_index: u32,
) where
    A: Annotation,
    F: Fn(Outcome<'s, A>) -> Option<Outcome<'s, A>>,
    N: AsNode,
{
    let frame = &mut stack.last_mut().expect("stack must be non-empty");

    // Did we speculatively validate unevaluated children?
    if let Some(invalid_unevaluated) = &mut frame.invalid_unevaluated {
        let invalid_unevaluated = invalid_unevaluated.view_bits::<Lsb0>();
        let unevaluated = frame.unevaluated.as_mut().unwrap().view_bits_mut::<Lsb0>();

        // Filter outcomes from unevaluated applications if the child was in-fact evaluated.
        // Then apply the remainder (from unevaluated children) to outcomes.
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
        // child is no longer unevaluated.
        *unevaluated &= invalid_unevaluated;

        // If any unevaluated child remains, then it was both unevaluated and
        // also failed speculative validation.
        frame.invalid = unevaluated.any();
    }

    // Verify AnyOf & OneOf applications, and apply annotations.
    for kw in frame.keywords {
        match kw {
            Keyword::AnyOf { .. } => {
                if !frame.valid_any_of {
                    if let Some(outcome) = filter(Outcome::AnyOfNotMatched) {
                        frame.outcomes.push((tape_index, outcome));
                    }
                    frame.invalid = true;
                }
            }
            Keyword::OneOf { .. } => {
                if !frame.valid_one_of {
                    if let Some(outcome) = filter(Outcome::OneOfNotMatched) {
                        frame.outcomes.push((tape_index, outcome));
                    }
                    frame.invalid = true;
                }
            }
            // Note that Annotation is ordered after AnyOf & OneOf.
            Keyword::Annotation { annotation } if !frame.invalid => {
                if let Some(outcome) = filter(Outcome::Annotation(annotation)) {
                    frame.outcomes.push((tape_index, outcome));
                }
            }
            _ => (),
        }
    }

    let Some(keyword) = frame.parent_keyword else {
        return; // Root frame. No parent to return to.
    };
    let frame = stack.pop().unwrap();
    let parent = &mut stack[frame.parent_frame as usize];

    enum Handling {
        RequiredInPlace,
        RequiredChild,
        OptionalInPlace,
        UnevaluatedChild,
        Ignore,
    }
    use Handling::*;

    let handling = match keyword {
        // Speculative evaluations of children which may be otherwise unevaluated.
        Keyword::UnevaluatedItems { .. } | Keyword::UnevaluatedProperties { .. } => {
            UnevaluatedChild
        }

        Keyword::Not { .. } => {
            // Invert the child's outcome.
            frame.invalid = !frame.invalid;
            frame.outcomes.clear();

            if frame.invalid {
                if let Some(outcome) = filter(Outcome::NotIsValid) {
                    frame.outcomes.push((tape_index, outcome));
                }
            }

            RequiredInPlace
        }

        Keyword::AllOf { .. }
        | Keyword::Ref { .. }
        | Keyword::DynamicRef { .. }
        | Keyword::DependentSchemas { .. } => RequiredInPlace,

        Keyword::If { .. } => {
            parent.valid_if = !frame.invalid;
            OptionalInPlace
        }
        Keyword::AnyOf { .. } => {
            parent.valid_any_of |= !frame.invalid;
            OptionalInPlace
        }
        Keyword::OneOf { .. } => {
            if parent.valid_one_of {
                if let Some(outcome) = filter(Outcome::OneOfMultipleMatched) {
                    parent.outcomes.push((tape_index, outcome));
                }
                parent.invalid = true;
            }
            OptionalInPlace
        }
        Keyword::Contains { .. } => {
            if !frame.invalid {
                parent.valid_contains += 1;
                parent.outcomes.extend(frame.outcomes.into_iter());
            }
            Ignore
        }

        Keyword::Then { .. } => parent.valid_if.then_some(RequiredInPlace).unwrap_or(Ignore),
        Keyword::Else { .. } => parent.valid_if.then_some(Ignore).unwrap_or(RequiredInPlace),

        // Child applications which must always succeed.
        Keyword::Pattern { .. }
        | Keyword::PatternProperties { .. }
        | Keyword::PrefixItems { .. }
        | Keyword::Items { .. }
        | Keyword::Properties { .. }
        | Keyword::PropertyNames { .. }
        | Keyword::AdditionalProperties { .. } => RequiredChild,
    };

    match handling {
        UnevaluatedChild => {
            if frame.invalid {
                parent
                    .invalid_unevaluated
                    .as_mut()
                    .unwrap()
                    .view_bits_mut::<Lsb0>()[child_index as usize] = true;
            }
            parent.outcomes_unevaluated.extend(
                frame
                    .outcomes
                    .drain(..)
                    .map(|(tape_index, outcome)| (child_index, tape_index, outcome)),
            );
        }
        RequiredChild => {
            parent.invalid |= frame.invalid;
            parent.outcomes.extend(frame.outcomes.into_iter());
        }
    }

    // Don't forget Not

    todo()
}

fn unwind<'n, 's, A, F, N>(
    filter: &F,
    index: &schema::Index<'s, A>,
    node: &'n N,
    parent: Option<(u32, &'s Keyword<A>)>,
    schema: &'s Schema<A>,
    stack: &mut Vec<Frame<'s, A>>,
    active: &mut Vec<u32>,
    tape_index: i32,
    mut track_validations: bool,
) where
    A: Annotation,
    F: Fn(Outcome<'s, A>) -> Option<Outcome<'s, A>>,
    N: AsNode,
{
    let active_begin = *active.last().unwrap() as usize;
    let active_end = stack.len();

    for frame in (active_end..active_begin).rev() {

        /*
        if let Some(unevaluated) = &mut stack[frame].unevaluated {
            let unevaluated = unevaluated.view_bits_mut::<bitvec::order::Lsb0>();

            let mu = vec![0u32; 8];
            let unevaluated_valid = mu.view_bits::<bitvec::order::Lsb0>();

            unevaluated_valid.iter_ones()

            for (index, (unevaluated, unevaluated_valid)) in
                unevaluated.iter().zip(unevaluated_valid.iter()).enumerate()
            {

                // If *unevaluated
                let baz = *bar;
            }

            for index in bar.iter_ones() {}

            for (foo, bar, baz) in stack[frame].outcomes_unevaluated.drain(..) {}
        }
        */

        // Note that child applications "allOf", "if", "then", "else", and "not"
        // already applied outcomes to this Frame when their Frame was unwound.
    }

    todo!()
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
