use super::LinkParams;
//-----------------------
use crate::config::DEFAULT_NAME;
use crate::lf_ext::*;
use polars::prelude::{LazyFrame, Schema};
use polars::sql::SQLContext;

pub struct SimpleSql {
    query: String,
    name: String,
}

impl LinkParams for SimpleSql {
    fn schema_test(&self, schema: Schema) -> Result<Schema, &str> {
        let mut sql_cont: SQLContext = SQLContext::new();
        sql_cont.register(self.query.as_str(), schema.empty().lazy());
        let res = sql_cont.execute(self.query.as_str())?;
        res.schema()
    }

    fn data_pass_through(&self, lf: LazyFrame) -> Result<LazyFrame, &str> {
        let cont = SQLContext::new();
        cont.register(self.name.as_str(), lf);

        cont.execute(self.query.as_str())
    }
}

impl SimpleSql {
    pub fn default() -> SimpleSql {
        SimpleSql {
            query: "SELECT * FROM tb;".to_string(),
            name: DEFAULT_NAME.to_string(),
        }
    }
}
