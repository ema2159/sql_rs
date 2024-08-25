use std::cell::RefCell;
use std::fs::File;
use std::io;
use std::io::{Seek, SeekFrom, Write};
use std::rc::Rc;

use thiserror::Error;

use super::page::{Page, PageError, PAGE_SIZE};
use super::row::Row;

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

    pub fn insert<T>(&mut self, data: &T, page_idx: usize) -> Result<(), PagerError>
    where
        T: TryInto<Rc<[u8]>, Error = ()> + Clone,
    {
        if let Some(cache_elem) = self.pages_cache.get_mut(page_idx) {
            if let Some(ref mut curr_page) = cache_elem {
                match curr_page.insert(data.clone()) {
                    Err(PageError::PageFull) => {
                        if page_idx >= TABLE_MAX_PAGES {
                            Err(PagerError::TableFull)?;
                        };
                        Err(PagerError::PageFull)?;
                    }
                    Err(other_err) => Err(other_err)?,
                    Ok(()) => (),
                }
            } else {
                return Err(PagerError::CacheMiss);
            }
        } else {
            return Err(PagerError::TableFull);
        }

        Ok(())
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
        let page_to_write = *self.pages_cache.get(page_idx).unwrap().as_ref().unwrap();
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
