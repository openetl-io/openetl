//! What runs on a lane between the normalizer's two halves, for one stream:
//! it takes batches parsed and typed, under the source's names, and gives
//! what destination naming, metadata columns and holds apply to.

use openetl_normalize::Typed;

#[derive(Debug, Default)]
pub enum Stage {
    /// Batches pass as they are.
    #[default]
    Identity,
}

impl Stage {
    /// A lane that shapes a batch again, after another lane changed the
    /// stream's schema, runs it through again.
    pub fn apply(&mut self, typed: Typed) -> Typed {
        match self {
            Stage::Identity => typed,
        }
    }
}
