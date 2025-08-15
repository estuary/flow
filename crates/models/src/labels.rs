use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Label is a key and value pair which can be attached to many catalog
/// entities within Flow.
#[derive(Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
#[schemars(example = Label::example())]
pub struct Label {
    /// # Name of the Label.
    pub name: String,
    /// # Value of the Label.
    /// When used within a selector, if value is empty or omitted than
    /// the label selection matches any value.
    #[serde(default)]
    pub value: String,
}

impl Label {
    pub fn example() -> Self {
        Self {
            name: "a/label".to_owned(),
            value: "value".to_owned(),
        }
    }
}

/// LabelSet is a collection of labels and their values.
#[derive(Deserialize, Serialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
#[schemars(example = LabelSet::example())]
pub struct LabelSet {
    /// Labels of the set.
    pub labels: Vec<Label>,
}

impl LabelSet {
    pub fn example() -> Self {
        Self {
            labels: vec![Label::example()],
        }
    }
}

/// LabelSelector defines a filter over LabelSets.
#[derive(Deserialize, Serialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
#[schemars(example = LabelSelector::example())]
pub struct LabelSelector {
    /// # Included labels of the selector.
    #[serde(default)]
    pub include: LabelSet,
    /// # Excluded labels of the selector.
    #[serde(default)]
    pub exclude: LabelSet,
}

impl LabelSelector {
    pub fn example() -> Self {
        Self {
            include: LabelSet::example(),
            exclude: LabelSet::example(),
        }
    }
}
