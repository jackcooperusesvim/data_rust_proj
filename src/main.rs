use polars::prelude::*;

struct CsvParser {
    data_source: String,
    has_header: bool,
    data_format: Option<SchemaRef>,
    dta: Option<DataFrame>,
    schema: Option<Schema>,
}

struct Select {
    selection: [Expr],
}

pub trait Block {
    fn lazy_evaluate(&self, lf: Option<LazyFrame>) -> SchemaRef;
    fn prepare(&mut self, lf: Option<SchemaRef>) -> Result<(), String>;
    fn evaluate(&mut self) -> LazyFrame;
    fn check(&mut self, schema: Option<SchemaRef>) -> Result<(), String>;
}

impl Block for Select {
    fn lazy_evaluate(&self, lf: Option<LazyFrame>) -> SchemaRef {
        Arc::new(match &lf {
            Some(schema) => ,
            None => panic!("No schema provided"),
        })
    }
    fn prepare(&mut self, _lf: Option<SchemaRef>) -> Result<(), String> {
        Ok(())
    }
    fn evaluate(&mut self) -> LazyFrame {
        DataFrame::default().lazy()
    }
    fn check(&mut self, _schema: Option<SchemaRef>) {
        // TODO: implement check
    }
}

impl Block for CsvParser {
    fn prepare(&mut self, _lf: Option<SchemaRef>) -> Result<(), String> {
        if self.dta.is_none() {
            self.dta = Some(
                CsvReadOptions::default()
                    .with_has_header(true)
                    .try_into_reader_with_file_path(Some("iris.csv".into()))
                    .expect("Loading DataFrame")
                    .finish()
                    .unwrap(),
            );
        }

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
        Ok(())
    }

    fn lazy_evaluate(&self, _lf: Option<LazyFrame>) -> SchemaRef {
        Arc::new(match &self.schema {
            Some(schema) => schema.clone(),
            None => self.dta.as_ref().unwrap().schema(),
        })
    }

    fn evaluate(&mut self) -> LazyFrame {
        DataFrame::default().lazy()
    }

    fn check(&mut self, _schema: Option<SchemaRef>) {
        let mut lf = LazyCsvReader::new(self.data_source.clone())
            .with_has_header(self.has_header)
            .finish()
            .expect("Loading LazyFrame");

        self.data_format = Some(lf.collect_schema().expect("Loading Schema"));
    }
}

fn main() {
    println!("Hello, world!");
}
