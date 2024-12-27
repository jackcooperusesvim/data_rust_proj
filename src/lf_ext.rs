use polars::prelude::{DataFrame, LazyFrame, PolarsResult, Schema};
pub trait LazyFrameExt {
    fn schema(&self) -> PolarsResult<Schema>;
    fn copy_empty(&self) -> PolarsResult<Schema>;
}
impl LazyFrameExt for LazyFrame {
    fn schema(&self) -> PolarsResult<Schema> {
        polars::prelude::PolarsResult::Ok(self.collect_schema()?.deref())
    }
    fn copy_empty(&self) -> PolarsResult<Schema> {
        polars::prelude::PolarsResult::Ok(self.collect_schema()?.deref())
    }
}

pub trait SchemaExt {
    fn empty(&self) -> DataFrame;
}
impl SchemaExt for Schema {
    fn empty(&self) -> DataFrame {
        DataFrame::empty_with_schema(self)
    }
}
