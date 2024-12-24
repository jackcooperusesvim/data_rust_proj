use super::ProcessingLink;
//-----------------------
use polars::prelude::{DataFrame, IntoLazy, LazyFrame, Schema};
use polars::sql::SQLContext;
use std::ops::Deref;

pub struct SimpleSql {
    input_schema: Schema,
    output_schema: Schema,
    query: String,
}
impl ProcessingLink for SimpleSql {
    fn schema_test(&self, input_schema: Schema) -> Result<Schema, String> {
        Ok(self.output_schema.clone())
    }

    //TODO: ACTUALLY IMPLEMENT THIS
    fn data_pass_through(
        &mut self,
        input_data: Option<LazyFrame>,
        copy: bool,
    ) -> Result<LazyFrame, String> {
        match input_data {
            Some(data) => {
                if copy {
                    Ok(data.clone())
                } else {
                    Ok(data)
                }
            }
            None => Err("No input data".to_string()),
        }
    }
}

impl SimpleSql {
    pub fn prepare(&mut self, query: String, input_schema: Schema) -> Result<Schema, String> {
        let mut sql_cont: SQLContext = SQLContext::new();

        sql_cont.register(
            query.as_str(),
            DataFrame::empty_with_schema(&input_schema).lazy(),
        );

        let res = sql_cont.execute(query.as_str());
        match res {
            Ok(mut out_lf) => {
                self.query = query;
                match out_lf.collect_schema() {
                    Ok(schema_arc) => {
                        self.output_schema = schema_arc.deref().clone();
                        Ok(self.output_schema.clone())
                    }
                    Err(err) => {
                        err.to_string().as_str();
                        Err(format!("Error with query: {}", err.to_string()))
                    }
                }
            }
            Err(_) => Err(format!("Error with query: {}", res.err().unwrap())),
        }
    }
}
