use std::ops::{Deref, DerefMut};

#[derive(Debug, Default)]
pub struct Journal(Vec<usize>);

impl DerefMut for Journal {
    fn deref_mut(&mut self) -> &mut Vec<usize> {
        &mut self.0
    }
}

impl Deref for Journal {
    type Target = Vec<usize>;

    fn deref(&self) -> &Vec<usize> {
        &self.0
    }
}

impl Journal {
    pub fn log_page_change(&mut self, page_num: usize) {
        self.push(page_num);
    }
}
