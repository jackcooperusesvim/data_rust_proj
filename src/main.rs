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
    output_lfc: Option<LFConstructable>,
}

struct Sql {
    input_schemas: Vec<LFConstructable>,
    output_schemas: Vec<Schema>,
    expression: String,
}

#[derive(Clone)]
struct LFConstructable {
    schema: Schema,
    name: String,
}
impl LFConstructable {
    fn new(schema: Schema, name: String) -> Self {
        Self { schema, name }
    }
    fn construct(&self) -> LazyFrame {
        //DataFrame::from_rows_and_schema(self.iter().map(|schema, name| None), self.schema.clone())?
    }
}

pub enum BlockOptions {
    Sql {
        expression: String,
        input_lfc: Vec<LFConstructable>,
    },
    CsvParser {
        source: String,
        name: Option<String>,
    },
}

pub trait Block {
    // Immutable
    fn schema_test(&self, opts: BlockOptions) -> Result<LFConstructable, String>;

    // Mutable
    fn data_pass_through(
        &mut self,
        input_data: Option<LazyFrame>,
        copy: bool,
    ) -> Result<LazyFrame, String>;
    fn prepare(&mut self, opts: BlockOptions) -> Result<LFConstructable, String>;
}

impl Sql {
    fn gen_schema(&self) -> Result<LFConstructable, String> {
        Err("Not Implemented".to_string())
    }
}
impl Block for Sql {
    fn schema_test(&self, opts: BlockOptions) -> Result<LFConstructable, String> {
        Err("Not Implemented".to_string())
    }

    fn prepare(&mut self, opts: BlockOptions) -> Result<LFConstructable, String> {
        match opts {
            BlockOptions::Sql {
                expression,
                input_lfc,
            } => {
                self.expression = expression;
                self.input_schemas = input_lfc;
                let mut lf_inputs: Vec<LFConstructable> = Vec::new();
                Ok(input_schemas)
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
            BlockOptions::CsvParser => match &self.output_lfc {
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

        self.output_lfc = Some(match &self.dta {
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

                match &self.output_lfc {
                    Some(_schema) => {}
                    None => self.output_lfc = Some(self.dta.as_ref().unwrap().schema()),
                };

                self.output_lfc = Some(self.output_lfc.as_ref().unwrap().clone());

                match &self.output_lfc {
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
