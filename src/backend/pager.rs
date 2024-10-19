use std::cell::RefCell;
use std::fs::File;
use std::io;
use std::io::{Seek, SeekFrom, Write};
use std::rc::Rc;

use thiserror::Error;

use super::cursor::DBCursor;
use super::page::{Page, PageError, PAGE_SIZE};

const TABLE_MAX_PAGES: usize = 100;

#[derive(Error, Debug)]
pub enum PagerError {
    #[error("Could not insert row in page. The following error ocurred during insertion: {0}")]
    PageRowInsertError(#[from] PageError),
    #[error("Cannot insert row. Remaining page capacity is smaller than the row size")]
    PageFull,
    #[error("Table full")]
    TableFull,
    #[error("Page index out of range")]
    PageIdxOutOfRange,
    #[error("Cache miss")]
    CacheMiss,
}

#[derive(Debug)]
pub struct Pager {
    pages_cache: [Option<Page>; TABLE_MAX_PAGES],
    file_ref: Rc<RefCell<File>>,
}

impl Pager {
    pub fn new(file: Rc<RefCell<File>>) -> Pager {
        const INIT_NONE: Option<Page> = None;
        let pages_cache: [Option<Page>; TABLE_MAX_PAGES] = [INIT_NONE; TABLE_MAX_PAGES];

        Self {
            pages_cache,
            file_ref: file,
        }
    }

    pub fn insert<T>(&mut self, cursor: &mut DBCursor, key: u64, value: &T) -> Result<(), PagerError>
    where
        T: TryInto<Box<[u8]>, Error = ()> + Clone,
    {
        let page_option = self
            .pages_cache
            .get_mut(cursor.page_num as usize)
            .ok_or_else(|| PagerError::PageIdxOutOfRange)?;

        if let Some(page) = page_option.as_mut() {
            match page.insert(cursor, key, value) {
                Ok(()) => return Ok(()),
                Err(err) => panic!("Error while inserting record on page: {err}"),
            }
        } else {
            self.new_page(cursor.page_num as usize);
            return self.insert(cursor, key, value);
        }
    }

    pub fn new_page(&mut self, page_idx: usize) -> Result<(), PagerError> {
        let new_page = Page::new();
        if page_idx >= TABLE_MAX_PAGES {
            return Err(PagerError::TableFull);
        };
        self.pages_cache[page_idx] = Some(new_page);

        Ok(())
    }

    pub fn flush(&mut self, page_idx: usize) -> Result<(), PagerError> {
        if page_idx >= TABLE_MAX_PAGES {
            return Err(PagerError::PageIdxOutOfRange);
        }

        let mut file = self.file_ref.borrow_mut();
        let _ = file.seek(SeekFrom::Start((page_idx * PAGE_SIZE) as u64));
        let page_to_write = self.pages_cache.get(page_idx).unwrap().as_ref().unwrap().clone();
        let bytes: [u8; PAGE_SIZE] = page_to_write.into();
        let _ = file.write_all(&bytes);
        Ok(())
    }

    pub fn flush_all(&mut self) -> Result<(), PagerError> {
        let flush_indices: Vec<usize> = self
            .pages_cache
            .iter()
            .enumerate()
            .filter(|x| x.1.is_some())
            .map(|x| x.0)
            .collect();

        for i in flush_indices {
            match self.flush(i) {
                Ok(()) => {}
                Err(PagerError::PageIdxOutOfRange) => {}
                _ => unreachable!(),
            };
        }
        Ok(())
    }

    pub fn pages(&self) -> impl Iterator<Item = &Option<Page>> {
        self.pages_cache.iter()
    }
}
