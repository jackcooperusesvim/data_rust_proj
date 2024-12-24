pub mod link;
use crate::LazyFrame;
use link::ProcessingLink;

struct ProcessingChain {
    processing_links: Vec<Box<dyn ProcessingLink>>,
}

impl ProcessingChain {
    fn schema_test(&self) -> Result<LFConstructable, String> {
        let in_lfc = self.data_input.deref().lfc_return()?;
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

    fn data_pass_through(&mut self, input_data: Option<LazyFrame>) -> Result<LazyFrame, String> {
        let in_lfc = self.data_input.deref().lfc_return()?;
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

    fn add_link(&mut self, index: usize, link: Box<dyn ProcessingLink>) -> Result<(), String>;
}
