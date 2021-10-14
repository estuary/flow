use protocol::protocol as broker;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Label is a key and value pair which can be attached to many catalog
/// entities within Flow.
#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(example = "Label::example")]
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

impl From<broker::Label> for Label {
    fn from(l: broker::Label) -> Self {
        let broker::Label { name, value } = l;
        Self { name, value }
    }
}
impl Into<broker::Label> for Label {
    fn into(self) -> broker::Label {
        let Self { name, value } = self;
        broker::Label { name, value }
    }
}

/// LabelSet is a collection of labels and their values.
#[derive(Deserialize, Serialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
#[schemars(example = "LabelSet::example")]
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

impl From<broker::LabelSet> for LabelSet {
    fn from(l: broker::LabelSet) -> Self {
        Self {
            labels: l.labels.into_iter().map(Into::into).collect(),
        }
    }
}
impl Into<broker::LabelSet> for LabelSet {
    fn into(self) -> broker::LabelSet {
        let Self { mut labels } = self;

        // broker::LabelSet requires that labels be ordered on (name, value).
        // Establish this invariant.
        labels.sort_by(|lhs, rhs| (&lhs.name, &lhs.value).cmp(&(&rhs.name, &rhs.value)));

        broker::LabelSet {
            labels: labels.into_iter().map(Into::into).collect(),
        }
    }
}

/// LabelSelector defines a filter over LabelSets.
#[derive(Deserialize, Serialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
#[schemars(example = "LabelSelector::example")]
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

impl From<broker::LabelSelector> for LabelSelector {
    fn from(l: broker::LabelSelector) -> Self {
        let broker::LabelSelector { include, exclude } = l;
        Self {
            include: include.unwrap_or_default().into(),
            exclude: exclude.unwrap_or_default().into(),
        }
    }
}
impl Into<broker::LabelSelector> for LabelSelector {
    fn into(self) -> broker::LabelSelector {
        let Self { include, exclude } = self;
        broker::LabelSelector {
            include: Some(include.into()),
            exclude: Some(exclude.into()),
        }
    }
}
