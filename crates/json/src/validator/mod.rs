use crate::{
    AsNode,
    schema::{self, Annotation, Keyword, Schema},
};
mod output;
mod validation;

pub use output::build_basic_output;

/// Outcome is an error or annotation produced by the validation process.
#[derive(Debug, Clone, Copy)]
pub enum Outcome<'s, A: Annotation> {
    Annotation(&'s A),
    AnyOfNotMatched,
    ConstNotMatched,
    EnumNotMatched,
    ExclusiveMaximumExceeded,
    ExclusiveMinimumNotMet,
    False,
    FormatNotMatched(&'s schema::formats::Format),
    ItemsNotUnique,
    MaxContainsExceeded(u32, u32),
    MaxItemsExceeded(u32, u32),
    MaxLengthExceeded(u32, u32),
    MaxPropertiesExceeded(u32, u32),
    MaximumExceeded,
    MinContainsNotMet(u32, u32),
    MinItemsNotMet(u32, u32),
    MinLengthNotMet(u32, u32),
    MinPropertiesNotMet(u32, u32),
    MinimumNotMet,
    MissingRequiredProperty(&'s schema::PackedStr),
    MultipleOfNotMet,
    NotIsValid,
    OneOfMultipleMatched,
    OneOfNotMatched,
    PatternNotMatched,
    RecursionDepthExceeded,
    ReferenceNotFound(&'s schema::PackedStr),
    TypeNotMet(schema::types::Set),
}

/// ScopedOutcome is an Outcome with its tape index and schema URI.
#[derive(Debug)]
pub struct ScopedOutcome<'s, A: Annotation> {
    pub outcome: Outcome<'s, A>,
    pub schema_curi: &'s schema::PackedStr,
    pub tape_index: i32,
}

pub struct Validator<'s, A>
where
    A: Annotation,
{
    index: &'s schema::Index<'s, A>,
    stack: Vec<Frame<'s, A>>,
    active: Vec<u32>,
}

struct Frame<'s, A>
where
    A: Annotation,
{
    // Index of this Frame's parent within the stack.
    parent_frame: u16,
    // Keyword which created this Frame. None for the root Frame.
    parent_keyword: Option<&'s Keyword<A>>,
    // Keywords being evaluated by this Frame.
    keywords: &'s [Keyword<A>],
    // Bit flags of this Frame.
    flags: u8,
    // Counter retains:
    // * The current index into Keyword::Properties (when an object).
    // * The number of valid Keyword::Contains applications (when an array).
    counter: u32,
    // Fields related to tracking speculative execution for unevaluatedItems/Properties.
    // None in the common case where these keywords aren't present or active.
    speculative: Option<Box<FrameSpeculative<'s, A>>>,
    // Scoped outcomes of this Frame and its unwound children.
    outcomes: Vec<ScopedOutcome<'s, A>>,
}

// FLAG_INVALID is set if a validation error occurred.
const FLAG_INVALID: u8 = 0x01;
// FLAG_VALID_IF_THEN is set if a Keyword::If validated the "then" branch.
const FLAG_VALID_IF_THEN: u8 = 0x02;
// FLAG_VALID_IF_ELSE is set if a Keyword::If validated the "else" branch.
const FLAG_VALID_IF_ELSE: u8 = 0x04;
// FLAG_VALID_ANY_OF is set if a Keyword::AnyOf in-place application validated.
const FLAG_VALID_ANY_OF: u8 = 0x08;
// FLAG_VALID_ONE_OF is set if a Keyword::OneOf in-place application validated.
const FLAG_VALID_ONE_OF: u8 = 0x10;

struct FrameSpeculative<'s, A>
where
    A: Annotation,
{
    // Bit field of children that have been evaluated by this Frame,
    // or an unwound in-place child application thereof. "evaluation" means the
    // child was matched by "properties", "patternProperties", "prefixItems",
    // or "items", or was successfully validated by "contains".
    evaluated: Box<[u32]>,
    // Bit field of speculative validation outcomes of children that failed,
    // under an unevaluatedItems/Properties keyword of this Frame.
    // None if there is no such keyword in this Frame.
    invalid_unevaluated: Option<Box<[u32]>>,
    // Outcomes of Keyword::UnevaluatedItems/Properties of *this* Frame,
    // as (child-index, tape-index, outcome).
    outcomes_unevaluated: Vec<(u32, i32, &'s schema::PackedStr, Outcome<'s, A>)>,
}

struct Validation<'s, 'v, A, F>
where
    A: Annotation,
    F: for<'o> Fn(Outcome<'o, A>) -> Option<Outcome<'o, A>>,
{
    // Stack of offsets tracking frames which are active (being evaluated concurrently).
    active: &'v mut Vec<u32>,
    // Filter function to apply to outcomes.
    filter: F,
    // Index of schemas for $ref resolution.
    index: &'s schema::Index<'s, A>,
    // Current tape index within the document.
    next_tape_index: i32,
    // Stack-like frames of concurrent evaluations.
    stack: &'v mut Vec<Frame<'s, A>>,
}

impl<'s, A> Validator<'s, A>
where
    A: Annotation,
{
    #[inline]
    pub fn new(index: &'s schema::Index<'s, A>) -> Self {
        Self {
            active: Vec::new(),
            index,
            stack: Vec::new(),
        }
    }

    #[inline]
    pub fn validate<'n, N, F>(
        &mut self,
        schema: &'s Schema<A>,
        doc: &'n N,
        filter: F,
    ) -> (bool, Vec<ScopedOutcome<'s, A>>)
    where
        F: for<'o> Fn(Outcome<'o, A>) -> Option<Outcome<'o, A>>,
        N: AsNode,
    {
        // Inactive root frame which is unwind into.
        self.stack.push(Frame {
            parent_frame: 0,
            parent_keyword: None,
            keywords: &[],
            flags: 0,
            outcomes: Vec::new(),
            counter: 0,
            speculative: None,
        });
        self.active.push(1);

        let mut val = Validation {
            active: &mut self.active,
            filter,
            index: self.index,
            next_tape_index: 0,
            stack: &mut self.stack,
        };

        val.wind_frame(0, None, schema, false, doc);
        val.walk(0, doc);

        let root = val.stack.pop().unwrap();
        (root.flags & FLAG_INVALID == 0, root.outcomes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sizes() {
        assert_eq!(
            std::mem::size_of::<Outcome<crate::schema::CoreAnnotation>>(),
            16
        );
        assert_eq!(
            std::mem::size_of::<Frame<crate::schema::CoreAnnotation>>(),
            64 // Frame is exactly one cache line.
        );
    }
}
