use crate::names::*;

impl Lambda {
    pub fn example_typescript() -> Self {
        Self::Typescript
    }
    pub fn example_remote() -> Self {
        Self::Remote("http://example/api".to_string())
    }
}
