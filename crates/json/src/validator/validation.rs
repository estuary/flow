use super::{
    Frame, FrameSpeculative, Outcome, ScopedOutcome, Validation, FLAG_INVALID, FLAG_VALID_ANY_OF,
    FLAG_VALID_IF_ELSE, FLAG_VALID_IF_THEN, FLAG_VALID_ONE_OF,
};
use crate::{
    node::{Field, Fields},
    number,
    schema::{self, types, Annotation, Keyword, Schema},
    AsNode, Node,
};
use bitvec::{order::Lsb0, view::BitView};
use std::cmp::Ordering;

impl<'s, 'v, A, F> Validation<'s, 'v, A, F>
where
    A: Annotation,
    F: for<'o> Fn(Outcome<'o, A>) -> Option<Outcome<'o, A>>,
{
    #[inline(never)]
    pub fn walk<'n, N: AsNode>(&mut self, child_index: usize, node: &'n N) {
        let my_tape_index = self.next_tape_index;
        self.next_tape_index += 1; // Consume this node.

        let (node, node_type) = match node.as_node() {
            node @ Node::Array(items) => {
                for (child_index, item) in items.iter().enumerate() {
                    if self.wind_item(child_index, item) {
                        self.walk(child_index, item);
                    } else {
                        self.next_tape_index += item.tape_length(); // Skip child.
                    }
                }
                self.visit_array(items, my_tape_index);
                (node, types::ARRAY)
            }
            node @ Node::Object(fields) => {
                for (child_index, field) in fields.iter().enumerate() {
                    let property = field.property();

                    if self.wind_property_name() {
                        self.walk_property_name::<N>(child_index, property);
                    }

                    if self.wind_property(property, field.value()) {
                        self.walk(child_index, field.value());
                    } else {
                        self.next_tape_index += field.value().tape_length(); // Skip child.
                    }
                }
                self.visit_object::<N>(fields, my_tape_index);
                (node, types::OBJECT)
            }
            node @ Node::Bool(_) => (node, types::BOOLEAN),
            node @ Node::Bytes(_) => (node, types::INVALID),
            node @ Node::PosInt(n) => {
                self.visit_number(n, my_tape_index);
                (node, types::INTEGER)
            }
            node @ Node::NegInt(n) => {
                self.visit_number(n, my_tape_index);
                (node, types::INTEGER)
            }
            node @ Node::Float(f) => {
                self.visit_number(f, my_tape_index);
                (
                    node,
                    if f.fract() == 0.0 {
                        types::INTEGER
                    } else {
                        types::FRACTIONAL
                    },
                )
            }
            node @ Node::Null => (node, types::NULL),
            node @ Node::String(val) => {
                self.visit_string(val, my_tape_index);
                (node, types::STRING)
            }
        };
        self.visit_node(node, node_type, my_tape_index);

        // Unwind all active frames for `node`.
        let active = self.active.pop().unwrap() as usize;
        while self.stack.len() != active {
            self.unwind_frame(child_index, my_tape_index);
        }
    }

    #[inline(never)]
    fn walk_property_name<'n, N: AsNode>(&mut self, child_index: usize, property: &str) {
        self.visit_string(property, self.next_tape_index);
        self.visit_node(
            Node::<N>::String(property),
            types::STRING,
            self.next_tape_index,
        );

        // Unwind all active frames for this `property`.
        let active = self.active.pop().unwrap() as usize;
        while self.stack.len() != active {
            self.unwind_frame(child_index, self.next_tape_index);
        }
    }

    #[inline]
    fn wind_item<'n, N: AsNode>(&mut self, child_index: usize, node: &'n N) -> bool {
        let active = *self.active.last().unwrap() as usize..self.stack.len();

        for frame in active.start..active.end {
            let mut matched = false;

            for kw in self.stack[frame].keywords {
                // Property applications have preference rules (which keywords are sorted by).
                // C.f. https://json-schema.org/draft/2019-09/json-schema-core.html#rfc.section.9.3.2
                match kw {
                    Keyword::PrefixItems { prefix_items } => {
                        if let Some(schema) = prefix_items.get(child_index) {
                            self.wind_frame(frame, Some(kw), schema, false, node);
                            matched = true;
                        }
                    }
                    Keyword::Items { items } => {
                        if !matched {
                            self.wind_frame(frame, Some(kw), items, false, node);
                            matched = true;
                        }
                    }
                    Keyword::Contains { contains } => {
                        self.wind_frame(frame, Some(kw), contains, false, node);
                    }
                    Keyword::UnevaluatedItems { unevaluated_items } => {
                        if !matched {
                            self.wind_frame(frame, Some(kw), unevaluated_items, false, node);
                        }
                    }
                    _ => (),
                }
            }
        }

        if self.stack.len() != active.end {
            self.active.push(active.end as u32);
            true
        } else {
            false
        }
    }

    #[inline]
    fn wind_property_name(&mut self) -> bool {
        let active = *self.active.last().unwrap() as usize..self.stack.len();

        for frame in active.start..active.end {
            for kw in self.stack[frame].keywords {
                if let Keyword::PropertyNames { property_names } = &kw {
                    self.wind_frame(
                        frame,
                        Some(kw),
                        property_names,
                        false,
                        // Use of a placeholder Null is okay because `node` is
                        // matched against Object or Array types only
                        // (for "unevaluated*" and "dependentSchemas").
                        &serde_json::Value::Null,
                    );
                }
            }
        }

        if self.stack.len() != active.end {
            self.active.push(active.end as u32);
            true
        } else {
            false
        }
    }

    #[inline]
    fn wind_property<'n, N: AsNode>(&mut self, property: &str, node: &'n N) -> bool {
        let active = *self.active.last().unwrap() as usize..self.stack.len();

        for frame in active.start..active.end {
            let mut matched = false;

            for kw in self.stack[frame].keywords {
                // Property applications have preference rules (which keywords are sorted by).
                // C.f. https://json-schema.org/draft/2019-09/json-schema-core.html#rfc.section.9.3.2
                match kw {
                    Keyword::Properties { properties } => {
                        let me = &mut self.stack[frame];

                        while let Some((next, schema)) = properties.get(me.counter as usize) {
                            match next[1..].cmp(property) {
                                Ordering::Less => {
                                    me.counter += 1;
                                    if next.as_bytes()[0] != b'?' {
                                        let outcome = Outcome::MissingRequiredProperty(next);
                                        invalidate(&self.filter, me, outcome, self.next_tape_index);
                                    }
                                }
                                Ordering::Equal => {
                                    me.counter += 1;
                                    if next.as_bytes()[0] == b'+' {
                                        // `property` appeared in a "required" keyword but not "properties".
                                        // It's schema is a placeholder and it's not evaluated with respect
                                        // to "unevaluatedProperties". See build_object_keywords().
                                    } else {
                                        self.wind_frame(frame, Some(kw), schema, false, node);
                                        matched = true;
                                    }
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
                                self.wind_frame(frame, Some(kw), schema, false, node);
                                matched = true;
                            }
                        }
                    }
                    Keyword::AdditionalProperties {
                        additional_properties,
                    } => {
                        if !matched {
                            self.wind_frame(frame, Some(kw), additional_properties, false, node);
                            matched = true;
                        }
                    }
                    Keyword::UnevaluatedProperties {
                        unevaluated_properties,
                    } => {
                        if !matched {
                            self.wind_frame(frame, Some(kw), unevaluated_properties, false, node);
                        }
                    }
                    _ => (),
                }
            }
        }

        if self.stack.len() != active.end {
            self.active.push(active.end as u32);
            true
        } else {
            false
        }
    }

    #[inline(never)]
    pub fn wind_frame<'n, N: AsNode>(
        &mut self,
        parent_frame: usize,
        parent_keyword: Option<&'s Keyword<A>>,
        schema: &'s Schema<A>,
        in_place: bool,
        node: &'n N,
    ) {
        let frame = self.stack.len();
        let keywords = &*schema.keywords;

        // Ensure we have capacity for another Frame.
        if self.stack.len() == self.stack.capacity() {
            self.stack.reserve(1);
        }

        // At depths beyond this we risk stack overflows in debug builds,
        // observed at ~834 frames, with release supporting ~8k.
        if frame >= 512 {
            invalidate(
                &self.filter,
                &mut self.stack[parent_frame],
                Outcome::RecursionDepthExceeded,
                self.next_tape_index,
            );
            return;
        }

        // Construct the new Frame in-place. This was an observed hot-spot during
        // profiling. It's likely that the capacity / reserve check above is
        // enough to convince the compiler to build in-place, but we use unsafe
        // to make this explicit.
        self.stack.spare_capacity_mut()[0].write(Frame {
            parent_frame: parent_frame as u16,
            parent_keyword,
            keywords,
            flags: 0,
            outcomes: Vec::new(),
            counter: 0,
            speculative: None,
        });
        unsafe {
            self.stack.set_len(frame + 1);
        }

        // Determine whether this Frame requires a FrameSpeculative add-on:
        //
        // * If this Frame has an active unevaluatedItems/Properties then it
        //   must track both evaluated children and speculative evaluations of
        //   the unevaluated keyword's schema.
        // * Or, if this Frame is an in-place application (e.g. $ref) of a
        //   parent that tracks evaluated children, then it too must track
        //   evaluated children (but does not track speculative validations).
        for kw in keywords {
            match kw {
                Keyword::UnevaluatedItems { .. } => {
                    if let Node::Array(items) = node.as_node() {
                        self.stack[frame].speculative = Some(Box::new(FrameSpeculative {
                            evaluated: vec![0u32; (items.len() + 31) / 32].into(),
                            invalid_unevaluated: Some(vec![0u32; (items.len() + 31) / 32].into()),
                            outcomes_unevaluated: Vec::new(),
                        }))
                    }
                }
                Keyword::UnevaluatedProperties { .. } => {
                    if let Node::Object(fields) = node.as_node() {
                        self.stack[frame].speculative = Some(Box::new(FrameSpeculative {
                            evaluated: vec![0u32; (fields.len() + 31) / 32].into(),
                            invalid_unevaluated: Some(vec![0u32; (fields.len() + 31) / 32].into()),
                            outcomes_unevaluated: Vec::new(),
                        }))
                    }
                }
                _ => (),
            }
        }
        if in_place {
            if let Some(parent) = self.stack[parent_frame].speculative.as_ref() {
                if self.stack[frame].speculative.is_none() {
                    self.stack[frame].speculative = Some(Box::new(FrameSpeculative {
                        evaluated: parent.evaluated.clone(),
                        invalid_unevaluated: None, // Not tracked by child.
                        outcomes_unevaluated: Vec::new(),
                    }));
                }
            }
        }

        for kw in keywords {
            match kw {
                Keyword::AllOf { all_of } => {
                    for all_of in &**all_of {
                        self.wind_frame(frame, Some(kw), all_of, true, node);
                    }
                }
                Keyword::AnyOf { any_of } => {
                    for any_of in &**any_of {
                        self.wind_frame(frame, Some(kw), any_of, true, node);
                    }
                }
                Keyword::DependentSchemas { dependent_schemas } => {
                    if let Node::Object(fields) = node.as_node() {
                        for (prop, schema) in &**dependent_schemas {
                            if fields.get(prop).is_some() {
                                self.wind_frame(frame, Some(kw), schema, true, node);
                            }
                        }
                    }
                }
                Keyword::DynamicRef { dynamic_ref } => {
                    let referent = resolve_dynamic_ref(dynamic_ref, frame, self.index, self.stack);
                    if let Some(referent) = referent {
                        self.wind_frame(frame, Some(kw), referent, true, node);
                    } else {
                        invalidate(
                            &self.filter,
                            &mut self.stack[frame],
                            Outcome::ReferenceNotFound(dynamic_ref),
                            self.next_tape_index,
                        );
                    }
                }
                Keyword::Else { r#else } => self.wind_frame(frame, Some(kw), r#else, true, node),
                Keyword::If { r#if } => self.wind_frame(frame, Some(kw), r#if, true, node),
                Keyword::Not { r#not } => self.wind_frame(frame, Some(kw), r#not, true, node),
                Keyword::OneOf { one_of } => {
                    for one_of in &**one_of {
                        self.wind_frame(frame, Some(kw), one_of, true, node);
                    }
                }
                Keyword::Ref { r#ref } => {
                    if let Some((referent, _dynamic)) = self.index.fetch(r#ref) {
                        self.wind_frame(frame, Some(kw), referent, true, node);
                    } else {
                        invalidate(
                            &self.filter,
                            &mut self.stack[frame],
                            Outcome::ReferenceNotFound(r#ref),
                            self.next_tape_index,
                        );
                    }
                }
                Keyword::Then { then } => self.wind_frame(frame, Some(kw), then, true, node),

                _ => (), // Not an in-place application.
            }
        }
    }

    #[inline]
    fn visit_array<'n, N: AsNode>(&mut self, items: &[N], my_tape_index: i32) {
        let active = *self.active.last().unwrap() as usize..self.stack.len();

        #[inline(never)]
        fn all_unique<N: AsNode>(items: &[N]) -> bool {
            use itertools::Itertools;

            let mut sorted = items.iter().collect::<Vec<_>>();
            sorted.sort_by(|a, b| crate::node::compare(*a, *b));

            sorted
                .iter()
                .tuple_windows()
                .all(|(a, b)| crate::node::compare(*a, *b).is_ne())
        }

        for frame in &mut self.stack[active] {
            for kw in frame.keywords {
                let invalid: Outcome<'s, A> = match kw {
                    Keyword::MaxItems { max_items } => {
                        if items.len() <= *max_items {
                            continue;
                        }
                        Outcome::MaxItemsExceeded(*max_items as u32, items.len() as u32)
                    }
                    Keyword::MinItems { min_items } => {
                        if items.len() >= *min_items {
                            continue;
                        }
                        Outcome::MinItemsNotMet(*min_items as u32, items.len() as u32)
                    }
                    Keyword::UniqueItems {} => {
                        if all_unique(items) {
                            continue;
                        }
                        Outcome::ItemsNotUnique
                    }
                    Keyword::MinContains { min_contains } => {
                        if frame.counter >= *min_contains as u32 {
                            continue;
                        }
                        Outcome::MinContainsNotMet(*min_contains as u32, items.len() as u32)
                    }
                    Keyword::MaxContains { max_contains } => {
                        if frame.counter <= *max_contains as u32 {
                            continue;
                        }
                        Outcome::MaxContainsExceeded(*max_contains as u32, items.len() as u32)
                    }
                    _ => continue,
                };
                invalidate(&self.filter, frame, invalid, my_tape_index);
            }
        }
    }

    #[inline]
    fn visit_object<'n, N: AsNode>(&mut self, fields: &N::Fields, my_tape_index: i32) {
        let active = *self.active.last().unwrap() as usize..self.stack.len();

        for frame in &mut self.stack[active] {
            for kw in frame.keywords {
                let invalid: Outcome<'s, A> = match kw {
                    Keyword::MaxProperties { max_properties } => {
                        if fields.len() <= *max_properties {
                            continue;
                        }
                        Outcome::MaxPropertiesExceeded(*max_properties as u32, fields.len() as u32)
                    }
                    Keyword::MinProperties { min_properties } => {
                        if fields.len() >= *min_properties {
                            continue;
                        }
                        Outcome::MinPropertiesNotMet(*min_properties as u32, fields.len() as u32)
                    }
                    Keyword::Properties { properties } => {
                        // Fail if any remaining, un-walked properties were required.
                        let Some((property, _schema)) = (*properties)[frame.counter as usize..]
                            .iter()
                            .filter(|(property, _)| property.as_bytes()[0] != b'?')
                            .next()
                        else {
                            continue;
                        };
                        Outcome::MissingRequiredProperty(property)
                    }
                    _ => continue,
                };
                invalidate(&self.filter, frame, invalid, my_tape_index);
            }
        }
    }

    #[inline]
    fn visit_string(&mut self, val: &str, my_tape_index: i32) {
        let active = *self.active.last().unwrap() as usize..self.stack.len();

        for frame in &mut self.stack[active] {
            for kw in frame.keywords {
                let invalid: Outcome<'s, A> = match kw {
                    Keyword::Format { format } => {
                        if format.validate(val) {
                            continue;
                        }
                        Outcome::FormatNotMatched(format)
                    }
                    Keyword::MaxLength { max_length } => {
                        if val.len() <= *max_length {
                            continue; // If UTF-8 bytes fit, then chars fit.
                        }
                        let chars = val.chars().count();
                        if chars <= *max_length {
                            continue;
                        }
                        Outcome::MaxLengthExceeded(*max_length as u32, chars as u32)
                    }
                    Keyword::MinLength { min_length } => {
                        if val.len() / 4 >= *min_length {
                            continue; // If 1/4 UTF-8 bytes fit, then chars fit.
                        }
                        let chars = val.chars().count();
                        if chars >= *min_length {
                            continue;
                        }
                        Outcome::MinLengthNotMet(*min_length as u32, chars as u32)
                    }
                    Keyword::Pattern { pattern } => {
                        if pattern.is_match(val) {
                            continue;
                        }
                        Outcome::PatternNotMatched
                    }
                    _ => continue,
                };
                invalidate(&self.filter, frame, invalid, my_tape_index);
            }
        }
    }

    #[inline]
    fn visit_number<T>(&mut self, val: T, my_tape_index: i32)
    where
        T: number::Ops<u64> + number::Ops<i64> + number::Ops<f64> + Copy,
    {
        let active = *self.active.last().unwrap() as usize..self.stack.len();

        for frame in &mut self.stack[active] {
            for kw in frame.keywords {
                let invalid: Outcome<'s, A> = match kw {
                    Keyword::MinimumPosInt { minimum } => {
                        if val.json_cmp(*minimum).is_ge() {
                            continue;
                        }
                        Outcome::MinimumNotMet
                    }
                    Keyword::MinimumNegInt { minimum } => {
                        if val.json_cmp(*minimum).is_ge() {
                            continue;
                        }
                        Outcome::MinimumNotMet
                    }
                    Keyword::MinimumFloat { minimum } => {
                        if val.json_cmp(*minimum).is_ge() {
                            continue;
                        }
                        Outcome::MinimumNotMet
                    }

                    Keyword::MaximumPosInt { maximum } => {
                        if val.json_cmp(*maximum).is_le() {
                            continue;
                        }
                        Outcome::MaximumExceeded
                    }
                    Keyword::MaximumNegInt { maximum } => {
                        if val.json_cmp(*maximum).is_le() {
                            continue;
                        }
                        Outcome::MaximumExceeded
                    }
                    Keyword::MaximumFloat { maximum } => {
                        if val.json_cmp(*maximum).is_le() {
                            continue;
                        }
                        Outcome::MaximumExceeded
                    }

                    Keyword::ExclusiveMinimumPosInt { exclusive_minimum } => {
                        if val.json_cmp(*exclusive_minimum).is_gt() {
                            continue;
                        }
                        Outcome::ExclusiveMinimumNotMet
                    }
                    Keyword::ExclusiveMinimumNegInt { exclusive_minimum } => {
                        if val.json_cmp(*exclusive_minimum).is_gt() {
                            continue;
                        }
                        Outcome::ExclusiveMinimumNotMet
                    }
                    Keyword::ExclusiveMinimumFloat { exclusive_minimum } => {
                        if val.json_cmp(*exclusive_minimum).is_gt() {
                            continue;
                        }
                        Outcome::ExclusiveMinimumNotMet
                    }

                    Keyword::ExclusiveMaximumPosInt { exclusive_maximum } => {
                        if val.json_cmp(*exclusive_maximum).is_lt() {
                            continue;
                        }
                        Outcome::ExclusiveMaximumExceeded
                    }
                    Keyword::ExclusiveMaximumNegInt { exclusive_maximum } => {
                        if val.json_cmp(*exclusive_maximum).is_lt() {
                            continue;
                        }
                        Outcome::ExclusiveMaximumExceeded
                    }
                    Keyword::ExclusiveMaximumFloat { exclusive_maximum } => {
                        if val.json_cmp(*exclusive_maximum).is_lt() {
                            continue;
                        }
                        Outcome::ExclusiveMaximumExceeded
                    }

                    Keyword::MultipleOfPosInt { multiple_of } => {
                        if val.is_multiple_of(*multiple_of) {
                            continue;
                        }
                        Outcome::MultipleOfNotMet
                    }
                    Keyword::MultipleOfNegInt { multiple_of } => {
                        if val.is_multiple_of(*multiple_of) {
                            continue;
                        }
                        Outcome::MultipleOfNotMet
                    }
                    Keyword::MultipleOfFloat { multiple_of } => {
                        if val.is_multiple_of(*multiple_of) {
                            continue;
                        }
                        Outcome::MultipleOfNotMet
                    }

                    _ => continue,
                };
                invalidate(&self.filter, frame, invalid, my_tape_index);
            }
        }
    }

    #[inline]
    fn visit_node<'n, N: AsNode>(
        &mut self,
        node: Node<'n, N>,
        node_type: types::Set,
        tape_index: i32,
    ) {
        let active = *self.active.last().unwrap() as usize..self.stack.len();

        for frame in &mut self.stack[active] {
            for kw in frame.keywords {
                let invalid: Outcome<'s, A> = match kw {
                    Keyword::False => Outcome::False,
                    Keyword::Type { r#type } => {
                        if *r#type & node_type != types::INVALID {
                            continue;
                        }
                        Outcome::TypeNotMet(*r#type)
                    }
                    Keyword::Const { r#const } => {
                        if crate::node::compare_node(&node, &r#const.as_node()).is_eq() {
                            continue;
                        }
                        Outcome::ConstNotMatched
                    }
                    Keyword::Enum { r#enum } => {
                        if r#enum.iter().any(|r#enum| {
                            crate::node::compare_node(&node, &r#enum.as_node()).is_eq()
                        }) {
                            continue;
                        }
                        Outcome::EnumNotMatched
                    }
                    Keyword::Annotation { annotation } => {
                        if let Some(outcome) = (self.filter)(Outcome::Annotation(annotation)) {
                            frame.outcomes.push(ScopedOutcome {
                                outcome,
                                schema_curi: schema::get_curi(frame.keywords),
                                tape_index,
                            });
                        }
                        continue;
                    }

                    _ => continue,
                };
                invalidate(&self.filter, frame, invalid, tape_index);
            }
        }
    }

    #[inline]
    fn unwind_frame(&mut self, child_index: usize, tape_index: i32) {
        let mut frame = self.stack.pop().unwrap();
        let parent = &mut self.stack[frame.parent_frame as usize];

        for kw in frame.keywords {
            let invalid = match kw {
                Keyword::AnyOf { .. } => {
                    if frame.flags & FLAG_VALID_ANY_OF != 0 {
                        continue;
                    }
                    Outcome::AnyOfNotMatched
                }
                Keyword::OneOf { .. } => {
                    if frame.flags & FLAG_VALID_ONE_OF != 0 {
                        continue;
                    }
                    Outcome::OneOfNotMatched
                }
                _ => continue,
            };
            invalidate(&self.filter, &mut frame, invalid, tape_index);
        }
        if frame.speculative.is_some() {
            unwind_speculative(&mut frame);
        }

        let Some(parent_keyword) = frame.parent_keyword else {
            // We're unwinding our final result into the inert root frame.
            parent.flags = frame.flags;
            std::mem::swap(&mut parent.outcomes, &mut frame.outcomes);
            return;
        };

        enum Handling {
            InPlace,
            Child,
            Neither,
        }

        let handling = match parent_keyword {
            // Property name applications:
            Keyword::PropertyNames { .. } => Handling::Neither,

            // In-place applications:
            Keyword::AllOf { .. }
            | Keyword::Ref { .. }
            | Keyword::DynamicRef { .. }
            | Keyword::DependentSchemas { .. } => Handling::InPlace,
            Keyword::AnyOf { .. } => {
                if is_invalid(frame.flags) {
                    return;
                }
                parent.flags |= FLAG_VALID_ANY_OF;
                Handling::InPlace
            }
            Keyword::OneOf { .. } => {
                if is_invalid(frame.flags) {
                    return;
                } else if parent.flags & FLAG_VALID_ONE_OF != 0 {
                    invalidate(
                        &self.filter,
                        parent,
                        Outcome::OneOfMultipleMatched,
                        tape_index,
                    );
                } else {
                    parent.flags |= FLAG_VALID_ONE_OF;
                }
                Handling::InPlace
            }
            Keyword::If { .. } => {
                if is_invalid(frame.flags) {
                    parent.flags |= FLAG_VALID_IF_ELSE;
                    return;
                }
                parent.flags |= FLAG_VALID_IF_THEN;
                Handling::InPlace
            }
            Keyword::Then { .. } => {
                if parent.flags & FLAG_VALID_IF_THEN == 0 {
                    return;
                }
                Handling::InPlace
            }
            Keyword::Else { .. } => {
                if parent.flags & FLAG_VALID_IF_ELSE == 0 {
                    return;
                }
                Handling::InPlace
            }
            Keyword::Not { .. } => {
                frame.outcomes.clear();

                if is_valid(frame.flags) {
                    if let Some(outcome) = (self.filter)(Outcome::NotIsValid) {
                        frame.outcomes.push(ScopedOutcome {
                            outcome,
                            schema_curi: schema::get_curi(&frame.keywords),
                            tape_index,
                        });
                    }
                }
                frame.flags ^= FLAG_INVALID;
                Handling::InPlace
            }

            // Child applications:
            Keyword::Contains { .. } => {
                if is_invalid(frame.flags) {
                    return;
                }
                parent.counter += 1;
                Handling::Child
            }
            Keyword::PatternProperties { .. }
            | Keyword::PrefixItems { .. }
            | Keyword::Items { .. }
            | Keyword::Properties { .. }
            | Keyword::AdditionalProperties { .. } => Handling::Child,

            // Unevaluated applications have bespoke handling.
            Keyword::UnevaluatedItems { .. } | Keyword::UnevaluatedProperties { .. } => {
                unwind_unevaluated(child_index as u32, frame.flags, frame.outcomes, parent);
                return;
            }

            _ => unreachable!("{parent_keyword:?}"),
        };

        if parent.outcomes.is_empty() {
            std::mem::swap(&mut parent.outcomes, &mut frame.outcomes);
        } else {
            parent.outcomes.extend(frame.outcomes.into_iter());
        }

        if is_invalid(frame.flags) {
            parent.flags |= FLAG_INVALID;
            return;
        }

        let Some(parent_speculative) = parent.speculative.as_mut() else {
            return;
        };

        match handling {
            Handling::InPlace => {
                let frame_speculative = frame.speculative.as_ref().unwrap();

                *parent_speculative.evaluated.view_bits_mut::<Lsb0>() |=
                    frame_speculative.evaluated.view_bits::<Lsb0>();
            }
            Handling::Child => {
                parent_speculative
                    .evaluated
                    .view_bits_mut::<Lsb0>()
                    .set(child_index, true);
            }
            Handling::Neither => {}
        }
    }
}

#[inline(never)]
fn unwind_unevaluated<'s, A: Annotation>(
    child_index: u32,
    frame_flags: u8,
    mut frame_outcomes: Vec<ScopedOutcome<'s, A>>,
    parent: &mut Frame<'s, A>,
) {
    let parent_speculative = parent.speculative.as_mut().unwrap();

    if is_invalid(frame_flags) {
        parent_speculative
            .invalid_unevaluated
            .as_mut()
            .unwrap()
            .view_bits_mut::<Lsb0>()
            .set(child_index as usize, true);
    }
    parent_speculative
        .outcomes_unevaluated
        .extend(frame_outcomes.drain(..).map(
            |ScopedOutcome {
                 outcome,
                 schema_curi,
                 tape_index,
             }| (child_index, tape_index, schema_curi, outcome),
        ));
}

#[inline(never)]
fn unwind_speculative<'s, A: Annotation>(frame: &mut Frame<'s, A>) {
    let speculative = frame.speculative.as_mut().unwrap();

    let Some(invalid_unevaluated) = speculative.invalid_unevaluated.as_mut() else {
        // This in-place child tracks evaluations but doesn't
        // have an unevaluated* keyword itself. Nothing to do.
        return;
    };

    let evaluated = speculative.evaluated.view_bits_mut::<Lsb0>();
    let invalid_unevaluated = invalid_unevaluated.view_bits_mut::<Lsb0>();

    // Remove outcomes from speculative unevaluatedProperties/Items applications
    // where the child was in fact evaluated elsewhere. Then apply the remainder
    // (from actually-unevaluated children) to outcomes.
    frame
        .outcomes
        .extend(speculative.outcomes_unevaluated.drain(..).filter_map(
            |(child_index, tape_index, schema_curi, outcome)| {
                if evaluated[child_index as usize] {
                    None
                } else {
                    Some(ScopedOutcome {
                        outcome,
                        schema_curi,
                        tape_index,
                    })
                }
            },
        ));

    // For each child, if our speculative validation succeeded then the
    // child is no longer unevaluated.
    //
    // Note our parent could *also* have a unevaluatedItems/Properties keyword,
    // so a post-condition is that this frame's `evaluated` must reflect the
    // correct application of successful speculative validations.
    //
    // Also note that these bit fields may have extra untouched bits at the end,
    // because we allocate a whole number of u32's. This is the reason we track
    // invalid_unevaluated instead of valid_unevaluated: it means the untouched
    // bits are implicitly valid when inverted and or'd into `evaluated`.
    let valid_unevaluated = !invalid_unevaluated;
    *evaluated |= &*valid_unevaluated;

    // If any child remains un-evaluated, then we now
    // know it also failed speculative validation.
    if !evaluated.all() {
        frame.flags |= FLAG_INVALID;
    }
}

#[inline(never)]
fn invalidate<'s, A, F>(
    filter: &F,
    frame: &mut Frame<'s, A>,
    invalid: Outcome<'s, A>,
    tape_index: i32,
) where
    A: Annotation,
    F: Fn(Outcome<'s, A>) -> Option<Outcome<'s, A>>,
{
    frame.flags |= FLAG_INVALID;

    if let Some(outcome) = filter(invalid) {
        frame.outcomes.push(ScopedOutcome {
            outcome,
            schema_curi: schema::get_curi(frame.keywords),
            tape_index,
        });
    }
}

#[inline(never)]
fn resolve_dynamic_ref<'s, A: Annotation>(
    dynamic_ref: &'s str,
    mut frame: usize,
    index: &schema::Index<'s, A>,
    stack: &[Frame<'s, A>],
) -> Option<&'s Schema<A>> {
    let mut scope = Vec::new();
    let mut scratch = String::new();
    scratch.push_str(dynamic_ref);

    let Some((referent, dynamic)) = index.fetch(&scratch) else {
        return None;
    };
    if !dynamic {
        // "Bookending requirement": if the resource of a $dynamicRef does not itself
        // declare a matched $dynamicAnchor, then it behaves as a regular $ref.
        return Some(referent);
    }
    scope.push(referent);

    // Map http://some/path#anchor to #anchor.
    let anchor = dynamic_ref.rfind('#').map(|i| &dynamic_ref[i..]).unwrap();

    // Walk the stack, searching for an alternative $dynamicAnchor match.
    while let Some(kw) = stack[frame].parent_keyword {
        frame = stack[frame].parent_frame as usize;

        if !matches!(kw, Keyword::Ref { .. } | Keyword::DynamicRef { .. }) {
            continue; // Only $ref and $dynamicRef can change the base URL.
        }
        // Lookup and remove the fragment (if present) of `frame` URI to obtain its base URI.
        let curi = &**schema::get_curi(&stack[frame].keywords);
        let base_uri = curi.rfind("#").map_or(curi, |i| &curi[..i]);

        scratch.clear();
        scratch.push_str(base_uri);
        scratch.push_str(anchor);

        if let Some((referent, dynamic)) = index.fetch(&scratch) {
            if dynamic {
                scope.push(referent);
            }
        };
    }

    // The last (root-most) dynamic-scope schema is the one we want.
    scope.pop()
}

#[inline(always)]
fn is_valid(frame_flags: u8) -> bool {
    frame_flags & FLAG_INVALID == 0
}
#[inline(always)]
fn is_invalid(frame_flags: u8) -> bool {
    frame_flags & FLAG_INVALID != 0
}

#[cfg(test)]
mod tests {
    use crate::schema;

    // Use this test to debug a failing test case, by updating `schema` and `doc`.
    #[test]
    fn test_case_debug() {
        let schema: serde_json::Value = serde_json::json!(
            {
                "items": [{"const": "hi"}],
                "additionalItems": {"const": "a"}
            }
        );
        let doc: serde_json::Value = serde_json::json!(["hi", "a", "B"]);

        let schema = schema::build::build_schema::<schema::CoreAnnotation>(
            &url::Url::parse("https://example.com/schema.json").unwrap(),
            &schema,
        )
        .unwrap();

        insta::assert_debug_snapshot!(&schema, @r###"
        Schema {
            keywords: [
                Id {
                    curi: "https://example.com/schema.json",
                    explicit: false,
                },
                PrefixItems {
                    prefix_items: [
                        Schema {
                            keywords: [
                                Id {
                                    curi: "https://example.com/schema.json#/items/0",
                                    explicit: false,
                                },
                                Const {
                                    const: String("hi"),
                                },
                            ],
                        },
                    ],
                },
                Items {
                    items: Schema {
                        keywords: [
                            Id {
                                curi: "https://example.com/schema.json#/additionalItems",
                                explicit: false,
                            },
                            Const {
                                const: String("a"),
                            },
                        ],
                    },
                },
            ],
        }
        "###);

        let mut builder = schema::index::Builder::new();
        builder.add(&schema).unwrap();
        builder.verify_references().unwrap();
        let index = builder.into_index();

        let mut validator = crate::Validator::new(&index);
        let (valid, outcomes) = validator.validate(
            &schema,
            &doc,
            |outcome| Some(outcome), // No filtering.
        );

        insta::assert_debug_snapshot!((valid, outcomes), @r###"
        (
            false,
            [
                ScopedOutcome {
                    outcome: ConstNotMatched,
                    schema_curi: "https://example.com/schema.json#/additionalItems",
                    tape_index: 3,
                },
            ],
        )
        "###);
    }

    #[test]
    fn test_recursion_overflow() {
        let schema: serde_json::Value = serde_json::json!({
            "$id": "http://example.com/recursive-schema",
            "$defs": {
                "nodeA": {
                    "allOf": [
                        {"$ref": "#/$defs/nodeB"},
                        {"$ref": "#/$defs/nodeB"},
                        {"$ref": "#/$defs/nodeB"},
                    ]
                },
                "nodeB": {
                    "allOf": [
                        {"$ref": "#/$defs/nodeA"},
                        {"$ref": "#/$defs/nodeA"}
                    ]
                }
            },
            "$ref": "#/$defs/nodeA"
        });
        let doc = serde_json::json!("test");

        let schema = schema::build::build_schema::<schema::CoreAnnotation>(
            &url::Url::parse("http://example.com/recursive-schema").unwrap(),
            &schema,
        )
        .unwrap();

        let mut builder = schema::index::Builder::new();
        builder.add(&schema).unwrap();
        builder.verify_references().unwrap();
        let index = builder.into_index();

        let mut validator = crate::Validator::new(&index);
        let (valid, outcomes) = validator.validate(&schema, &doc, |outcome| {
            matches!(outcome, crate::validator::Outcome::RecursionDepthExceeded).then_some(outcome)
        });

        assert!(!valid);
        assert_eq!(outcomes.len(), 384);
    }
}
