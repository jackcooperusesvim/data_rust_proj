//Processing Link 1: Single Table Single Query (Simple) Sql
pub mod sql;
use crate::{LazyFrame, Schema};

pub trait ProcessingLink {
    // Immutable
    fn schema_test(&self, input_schema: Schema) -> Result<Schema, String>;

    // Mutable
    fn data_pass_through(
        &mut self,
        input_data: Option<LazyFrame>,
        copy: bool,
    ) -> Result<LazyFrame, String>;
}
