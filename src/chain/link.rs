//Processing Link 1: Single Table Single Query (Simple) Sql
pub mod sql;
use crate::{LazyFrame, Schema};

pub trait ProcessingLink {
    // Immutable

    /// This method is used to test the expected output for a given input schema, which the chain can use to determine if the link fits the chain before insertion.
    fn schema_test(&self, input_schema: Schema) -> Result<Schema, String>;

    // Mutable
    fn prepare(&mut self, input_schema: Schema) -> Result<Schema, String> {}
    fn data_pass_through(&mut self, input_data: LazyFrame, copy: bool)
        -> Result<LazyFrame, String>;
}
