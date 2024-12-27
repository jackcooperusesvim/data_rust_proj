
struct base
impl LinkParams for Base{
    fn schema_test(&self, schema: Schema) -> Result<Schema, &str>;
    fn data_pass_through(&self, lf: LazyFrame) -> Result<LazyFrame, &str>;
}
