const SIMPLE_SQL_TABLE_NAME: &str = "simple_sql_table";
//Data Centered Types
struct NamedLazyFrame {
    name: String,
    frame: LazyFrame,
}

//Processing Links (Lowest Level of Encapsulation)
pub trait ProcessingLink {
    // Immutable
    fn schema_return(&self) -> Result<Schema, String>;

    // Mutable
    fn data_pass_through(
        &mut self,
        input_data: Option<LazyFrame>,
        copy: bool,
    ) -> Result<LazyFrame, String>;

    fn prepare(&mut self, opts: LinkOptions) -> Result<Schema, String>;
}

//Processing Link 1: Single Table Single Query (Simple) Sql
struct SimpleSql {
    input_schema: Schema,
    output_schema: Schema,
    query: String,
}

impl ProcessingLink for SimpleSql {
    fn schema_return(&self) -> Result<Schema, String> {
        Ok(self.output_schema.clone())
    }

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

    fn prepare(&mut self, opts: LinkOptions) -> Result<Schema, String> {
        match opts {
            LinkOptions::SimpleSql {
                query,
                input_schema,
            } => {
                let mut sql_cont: SQLContext = SQLContext::new();

                sql_cont.register(
                    SIMPLE_SQL_TABLE_NAME,
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
            _ => Err("Must provide SimpleSql options".to_string()),
        }
    }
}

fn build_ProcessingLink(opts: &LinkOptions) -> Result<Box<dyn ProcessingLink>, String> {
    match opts {
        // BlockOptions::Sql { query, input_lfc } => {
        //     let mut sql_cont: SQLContext = SQLContext::new();
        //     let mut lf_inputs: Vec<LFConstructable> = Vec::new();
        //
        //     input_lfc
        //         .iter()
        //         .map(|lfc| sql_cont.register(lfc.name.as_str(), lfc.construct()));
        //
        //     match sql_cont.execute(query.as_str()) {
        //         Ok(mut res) => match res.collect_schema() {
        //             Ok(schema_arc) => Ok(Box::new(Sql {
        //                 input_schemas: input_lfc.clone(),
        //                 output_schemas: schema_arc.deref().clone(),
        //                 query: query.clone(),
        //             })),
        //             Err(err) => Err(err.to_string()),
        //         },
        //         Err(err) => Err(err.to_string()),
        //     }
        // }
        _ => Err("Must provide sql options".to_string()),
    }
}

pub trait JoinerLink {
    fn schema_test(&self) -> Result<Vec<LFConstructable>, String>;

    fn data_pass_through(
        &mut self,
        input_data: Vec<NamedLazyFrame>,
    ) -> Result<Vec<NamedLazyFrame>, String>;
}

struct ProcessingChain {
    input_lfc: LFConstructable,
    output_lfc: LFConstructable,
    processing_links: Vec<Box<dyn ProcessingLink>>,
}

impl ProcessingChain {
    fn schema_test(&self) -> Result<LFConstructable, String>;

    fn data_pass_through(&mut self, input_data: Option<LazyFrame>) -> Result<LazyFrame, String>;

    fn prepare(&mut self, opts: LinkOptions) -> Result<LFConstructable, String>;
}

struct CsvParser {
    data_source: PathBuf,
    dta: Option<LazyFrame>,
    schema: Option<Schema>,
    name: Option<String>,
}

struct Sql {
    input_schemas: Vec<LFConstructable>,
    output_schemas: Vec<Schema>,
    query: String,
}

pub enum LinkOptions {
    SimpleSql {
        query: String,
        input_schema: Schema,
    },
    CsvParser {
        source: String,
        name: Option<String>,
    },
}

pub trait DataInputBlock {
    fn load_data(&mut self, opts: LinkOptions) -> Result<LFConstructable, String>;
    fn pop(&mut self, opts: LinkOptions) -> Result<LazyFrame, String>;
    fn set_name(&mut self, name: &str);
    fn lfc_return(&self) -> Result<LFConstructable, String>;
    fn rebuild(&mut self, source: &str);
}

// pub trait MultiBlock {
//     // Immutable
//     fn schema_test(&self) -> Result<Vec<LFConstructable>, String>;
//
//     // Mutable
//     fn data_pass_through(
//         &mut self,
//         input_data: Option<Vec<LazyFrame>>,
//         copy: bool,
//     ) -> Result<LazyFrame, String>;
//
//     // Class
//     fn build<'a>(opts: BlockOptions) -> Result<&'a Self, String>;
// }

// impl Block for CsvParser {
//     fn schema_test(&self) -> Result<LFConstructable, String> {
//         match &self.output_lfc {
//             Some(lfc) => Ok(lfc.clone()),
//             None => Err("No output_lfc in CsvParser".to_string()),
//         }
//     }
//
//     fn data_pass_through(
//         &mut self,
//         input_data: Option<LazyFrame>,
//         copy: bool,
//     ) -> Result<LazyFrame, String> {
//         if self.dta.is_none() {
//             self.dta = Some(
//                 CsvReadOptions::default()
//                     .with_has_header(true)
//                     .try_into_reader_with_file_path(Some("iris.csv".into()))
//                     .expect("Loading DataFrame")
//                     .finish()
//                     .unwrap(),
//             );
//         };
//
//         self.output_lfc = Some(match &self.dta {
//             Some(data) => LFConstructable::new(),
//             None => {
//                 self.= Some(
//                     CsvReadOptions::default()
//                         .with_has_header(true)
//                         .try_into_reader_with_file_path(Some("iris.csv".into()))
//                         .expect("Loading DataFrame")
//                         .finish()
//                         .unwrap(),
//                 );
//                 self.dta.as_ref().unwrap().schema()
//             }
//         });
//
//         match clone {
//             true => Ok(self.dta.as_mut().unwrap().clone().lazy()),
//             false => Ok(self.dta.take().unwrap().lazy()),
//         }
//     }
//     fn prepare(&mut self, opts: BlockOptions) -> Result<LFConstructable, String> {
//         match opts {
//             BlockOptions::CsvParser { source } => {
//                 if self.dta.is_none() {
//                     self.dta = Some(
//                         CsvReadOptions::default()
//                             .with_has_header(true)
//                             .try_into_reader_with_file_path(Some(source))
//                             .expect("Loading DataFrame")
//                             .finish()
//                             .unwrap(),
//                     );
//                 };
//
//                 match &self.output_lfc {
//                     Some(_schema) => {}
//                     None => self.output_lfc = Some(self.dta.as_ref().unwrap().schema()),
//                 };
//
//                 self.output_lfc = Some(self.output_lfc.as_ref().unwrap().clone());
//
//                 match &self.output_lfc {
//                     Some(schema) => Ok(schema.clone()),
//                     None => Err("No schema in CsvParser".to_string()),
//                 }
//             }
//             _ => Err("Must provide csv parser options".to_string()),
//         }
//     }
//     fn build<'a>(opts: BlockOptions) -> Result<&'a Self, String> {}
// }
//
// fn test_query(inputs: Vec<LFConstructable>, query: &str) -> Result<LFConstructable, String> {
//     //let mut lf_inputs: Vec<LFConstructable> = Vec::new();
//     let mut sql_cont: SQLContext = SQLContext::new();
//
//     inputs
//         .iter()
//         .map(|lfc| sql_cont.register(lfc.name.as_str(), lfc.construct()));
//
//     match sql_cont.execute(query.as_str()) {
//         Ok(mut res) => match res.collect_schema() {
//             Ok(schema_arc) => Ok(schema_arc.deref().clone()),
//             Err(err) => Err(err.to_string()),
//         },
//         Err(err) => Err(err.to_string()),
//     }
// }
fn main() {
    println!("Hello, world!");
}