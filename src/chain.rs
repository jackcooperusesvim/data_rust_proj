pub mod link;
use link::Link;
use polars::prelude::{DataFrame, IntoLazy, LazyFrame, Schema};

struct ProcessingChain {
    links: Vec<Link>,
    rename: Option<String>,
}

impl ProcessingChain {
    fn schema_test(&self, in_lfc: LFConstructable) -> Result<LFConstructable, String> {
        let name = in_lfc.name;
        let mut current_schema = in_lfc.schema;

        for link in &self.links {
            current_schema = link.schema_test(current_schema)?
        }

        Ok(LFConstructable {
            schema: current_schema,
            name,
        })
    }

    fn data_pass_through(&mut self, input_data: NamedLazyFrame) -> Result<NamedLazyFrame, String> {
        let name = input_data.name;
        let mut current_lf = input_data.frame;

        for link in &mut self.links {
            current_lf = link.data_pass_through(current_lf)?
        }

        Ok(NamedLazyFrame {
            frame: current_lf,
            name: self.rename.clone().unwrap_or(name),
        })
    }

    fn add_link(&mut self, index: usize, link: Box<dyn Link>) -> Result<(), String> {
        let chain_len = self.links.len();

        let mut count: usize = 0;
        while count < index && count < chain_len {
            self.links.count += 1;
        }

        if index == chain_len {
            self.links.insert(index, link);
        } else {
            self.links.push(link);
        }

        Ok(())
    }
}
