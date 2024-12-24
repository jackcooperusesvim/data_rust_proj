use polars::prelude::{DataFrame, LazyFrame, Schema};

pub struct LFConstructable {
    pub schema: Schema,
    pub name: String,
}

impl LFConstructable {
    pub fn construct(&self) -> LazyFrame {
        DataFrame::empty_with_schema(&self.schema.clone()).lazy()
    }
}

pub struct NamedLazyFrame {
    pub name: String,
    pub frame: LazyFrame,
}
