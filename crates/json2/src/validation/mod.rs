use crate::{
    node::{Field, Fields},
    schema::{Annotation, Index, Keyword, Schema},
    AsNode, Node,
};

#[derive(Debug)]
pub enum Outcome<'s, A: Annotation> {
    Invalid(&'s Keyword<A>, Option<String>),
    NotIsValid,
    AnyOfNotMatched,
    OneOfNotMatched,
    OneOfMultipleMatched,
    ReferenceNotFound(url::Url),
    Annotation(&'s A),
}

// In-progress validation of a Schema.
struct Evaluation<'s, A>
where
    A: Annotation,
{
    // Keywords being evaluated.
    keywords: &'s [Keyword<A>],

    // Parent of this Evaluation, as (evaluations index, keyword).
    parent: Option<(u32, &'s Keyword<A>)>,

    // Validation result.
    invalid: bool,

    // Outcomes of this Evaluation and its children, as (tape index, outcome).
    outcomes: Vec<(u32, Outcome<'s, A>)>,

    // Index into ordered Keyword::Properties
    properties_idx: u32,
}

pub struct Validator<'s, A>
where
    A: Annotation,
{
    // Index of schemas over canonical URIs and anchors.
    index: &'s Index<'s, A>,

    // Stack of evaluations.
    evaluations: Vec<Evaluation<'s, A>>,
    // Stack of offsets marking evaluations which are active.
    active: Vec<usize>,

    // Pool of idle bit vectors.
    bits_pool: Vec<Vec<u8>>,
}

impl<'s, A> Validator<'s, A>
where
    A: Annotation,
{
    pub fn new(index: &'s Index<'s, A>) -> Self {
        Self {
            index,
            evaluations: Vec::new(),
            active: Vec::new(),
            bits_pool: Vec::new(),
        }
    }

    fn push_eval(&mut self, parent: Option<(u32, &'s Keyword<A>)>, schema: &'s Schema<A>) {
        let (id, keywords) = schema.kw.split_first().unwrap();

        let index = self.evaluations.len();
        self.evaluations.push(Evaluation {
            keywords,
            parent,
            invalid: false,
            outcomes: Vec::new(),
            properties_idx: 0,
        });

        // Look for in-place applications which also need to be pushed.
        todo!()
    }

    fn foobar(&mut self) {
        let active_begin = *self.active.last().unwrap();
        let active_end = self.evaluations.len();

        for eval_idx in (active_end..active_begin).rev() {}
    }

    fn push_property(&mut self, tape_index: i32, property: &str) {
        let active_begin = *self.active.last().unwrap();
        let active_end = self.evaluations.len();

        // Push propertyNames applications to evaluate the property name.
        // (We cannot use an iterator because self.scopes is mutated).
        for eval_idx in active_begin..active_end {
            for kw in self.evaluations[eval_idx].keywords {
                if let Keyword::PropertyNames { property_names } = &kw {
                    self.push_eval(Some((eval_idx as u32, kw)), property_names);
                }
            }
        }

        if self.evaluations.len() != active_end {
            // Mark eval_begin..eval_end as inactive.
            self.active.push(active_end);
            // Apply the property name as a string, which pops propertyName applications.
            self.end_str(tape_index, property);
        }
    }

    fn push_item(&mut self, tape_index: i32, item_index: u32) {}

    fn begin_array(&mut self) -> bool {
        false
    }
    fn begin_object(&mut self) -> bool {
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
    validator: &mut Validator<'s, A>,
    tape_index: &mut i32,
    doc: &'n N,
) {
    match doc.as_node() {
        Node::Array(items) => {
            let arr_tape_index = *tape_index;

            if validator.begin_array() {
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

            if validator.begin_object() {
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
