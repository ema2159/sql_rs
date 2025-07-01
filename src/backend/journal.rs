use std::collections::BTreeMap;

use super::page::Page;

#[derive(Debug, Default)]
pub struct Journal {
    changed_pages: BTreeMap<usize, Option<Page>>,
}

impl Journal {
    pub fn log_page_change(&mut self, page_num: usize, page: Option<&Page>) {
        if self.changed_pages.contains_key(&page_num) {
            return;
        }
        self.changed_pages.insert(page_num, page.cloned());
    }

    pub fn changed_page_nums(&self) -> impl Iterator<Item = &usize> {
        self.changed_pages.keys()
    }
}
