pub mod link;
use link::ProcessingLink;
use polars::prelude::{DataFrame, IntoLazy, LazyFrame, Schema};
use std::ops::Deref;

impl LFConstructable {
    pub fn construct(&self) -> LazyFrame {
        DataFrame::empty_with_schema(&self.schema.clone()).lazy()
    }
}

struct ProcessingChain {
    processing_links: Vec<Box<dyn ProcessingLink>>,
}

impl ProcessingChain {
    fn schema_test(&self, in_lfc: LFConstructable) -> Result<LFConstructable, String> {
        let name = in_lfc.name;
        let mut current_schema = in_lfc.schema;

        for link in &self.processing_links {
            current_schema = link.deref().schema_test(current_schema)?
        }

        Ok(LFConstructable {
            schema: current_schema,
            name,
        })
    }

    fn data_pass_through(&mut self, input_data: LazyFrame) -> Result<LazyFrame, String> {
        let mut current_lf = in_lfc.construct();

        for link in &self.processing_links {
            current_lf = link.deref().data_pass_through(Some())?
        }

        Ok(LFConstructable {
            schema: current_lf,
            name,
        })
    }

    fn add_link(&mut self, index: usize, link: Box<dyn ProcessingLink>) -> Result<(), String>;
}
