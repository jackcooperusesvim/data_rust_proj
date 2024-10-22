use polars::prelude::*;
use std::path::PathBuf;

struct DataSchema {
    dta: LazyFrame,
    output_schema: Option<Schema>,
}
struct CsvParser {
    data_source: PathBuf,
    has_header: bool,
    data_format: Option<SchemaRef>,
    dta: Option<DataFrame>,
    schema: Option<Schema>,
}

struct Sql {
    input_schema: Option<Schema>,
    output_schema: Option<Schema>,
    expression: String,
}
pub enum BlockOptions {
    Sql {
        expression: String,
        input_schema: Schema,
    },
    CsvParser {
        source: String,
    },
}

pub trait Block {
    // Immutable
    fn schema_test(&self, opts: BlockOptions) -> Result<Schema, String>;

    // Mutable
    fn data_pass_through(
        &mut self,
        input_data: Option<LazyFrame>,
        copy: bool,
    ) -> Result<LazyFrame, String>;
    fn prepare(&mut self, opts: BlockOptions) -> Result<Schema, String>;
}

impl Block for Sql {
    fn schema_test(&self, opts: BlockOptions) -> Result<Schema, String> {}

    fn prepare(&mut self, opts: BlockOptions) -> Result<Schema, String> {
        match opts {
            BlockOptions::Sql {
                expression,
                input_schema,
            } => {
                self.expression = expression;
                self.input_schema = Some(input_schema.clone());
                Ok(input_schema)
            }
            _ => Err("Must provide sql options".to_string()),
        }
    }

    // Mutable
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

impl Block for CsvParser {
    fn schema_test(&self, opts: BlockOptions) -> Result<Schema, String> {
        match opts {
            BlockOptions::CsvParser => match &self.schema {
                Some(schema) => Ok(schema.clone()),
                None => Err("No schema in CsvParser".to_string()),
            },
            _ => Err("Must provide CsvParser options".to_string()),
        }
    }

    //--------------------------------------------------------------------------------------------------

    fn data_pass_through(
        &mut self,
        _input_data: Option<LazyFrame>,
        clone: bool,
    ) -> Result<LazyFrame, String> {
        if self.dta.is_none() {
            self.dta = Some(
                CsvReadOptions::default()
                    .with_has_header(true)
                    .try_into_reader_with_file_path(Some("iris.csv".into()))
                    .expect("Loading DataFrame")
                    .finish()
                    .unwrap(),
            );
        };

        self.schema = Some(match &self.dta {
            Some(data) => data.schema(),
            None => {
                self.dta = Some(
                    CsvReadOptions::default()
                        .with_has_header(true)
                        .try_into_reader_with_file_path(Some("iris.csv".into()))
                        .expect("Loading DataFrame")
                        .finish()
                        .unwrap(),
                );
                self.dta.as_ref().unwrap().schema()
            }
        });

        match clone {
            true => Ok(self.dta.as_mut().unwrap().clone().lazy()),
            false => Ok(self.dta.take().unwrap().lazy()),
        }
    }

    fn prepare(&mut self, opts: BlockOptions) -> Result<Schema, String> {
        match opts {
            BlockOptions::CsvParser { source } => {
                if self.dta.is_none() {
                    self.dta = Some(
                        CsvReadOptions::default()
                            .with_has_header(true)
                            .try_into_reader_with_file_path(Some(source))
                            .expect("Loading DataFrame")
                            .finish()
                            .unwrap(),
                    );
                };

                match &self.schema {
                    Some(_schema) => {}
                    None => self.schema = Some(self.dta.as_ref().unwrap().schema()),
                };

                self.schema = Some(self.schema.as_ref().unwrap().clone());

                match &self.schema {
                    Some(schema) => Ok(schema.clone()),
                    None => Err("No schema in CsvParser".to_string()),
                }
            }
            _ => Err("Must provide csv parser options".to_string()),
        }
    }
}

fn main() {
    println!("Hello, world!");
}
