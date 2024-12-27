//Processing Link 1: Single Table Single Query (Simple) Sql
pub mod _base;
pub mod sql;
use sql::SimpleSql;

use crate::{LazyFrame, Schema};

//NOTE: FOR FUTURE ADDITIONS, ADD AN ENUM VARIANT HERE
pub enum Link {
    SimpleSql(LinkBase<SimpleSql>),
}

//
//
//
//

pub struct LinkBase<P: LinkParams> {
    params: P,
    input_schema: Option<Schema>,
    output_schema: Option<Schema>,
}

impl<P: LinkParams> LinkBase<P> {
    fn data_pass_through(&self, lf: LazyFrame) -> Result<LazyFrame, &str> {
        self.params.data_pass_through(lf)
    }

    fn prepare(&mut self, input_schema: Schema) -> Result<Schema, &str> {
        self.input_schema = Some(input_schema);
        let schema = self.params.schema_test(input_schema)?;
        self.output_schema = Some(schema);

        Ok(schema)
    }

    fn imprint_params(&mut self, params: P) {
        self.input_schema = None;
        self.output_schema = None;

        self.params = params
    }

    fn read_params(&self) -> P {
        self.params
    }
}

pub trait LinkParams {
    fn schema_test(&self, schema: Schema) -> Result<Schema, &str>;
    fn data_pass_through(&self, lf: LazyFrame) -> Result<LazyFrame, &str>;
}
