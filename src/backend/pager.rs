use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use thiserror::Error;

use super::page::{Page, PageError, PAGE_SIZE};
use super::row::Row;

const TABLE_MAX_PAGES: usize = 100;

#[derive(Error, Debug)]
pub enum PagerError {
    #[error("Could not insert row in page. The following error ocurred during insertion: {0}")]
    PageRowInsertError(String),
    #[error("Could not read table from disk. The following error occurred during read: {0}")]
    ReadFromDiskError(String),
    #[error("Cannot insert row. Remaining page capacity is smaller than the row size")]
    PageFull,
    #[error("Table full")]
    TableFull,
    #[error("Page index out of range")]
    PageIdxOutOfRange,
    #[error("Cache miss")]
    CacheMiss,
    #[error("DB connection closed")]
    DbClosed,
}

#[derive(Debug)]
pub struct Pager {
    pub pages_cache: [Option<Page>; TABLE_MAX_PAGES],
    file: Option<File>,
}

impl Pager {
    pub fn close(&mut self) {
        const INIT_NONE: Option<Page> = None;
        self.pages_cache = [INIT_NONE; TABLE_MAX_PAGES];
        self.file = None;
    }

    pub fn open(path_str: &str) -> Result<Self, PagerError> {
        let path = Path::new(path_str);
        let file = Some(
            File::options()
                .create(true)
                .append(true)
                .open(path)
                .map_err(|err| PagerError::ReadFromDiskError(err.to_string()))?,
        );

        const INIT_NONE: Option<Page> = None;
        let pages_cache: [Option<Page>; TABLE_MAX_PAGES] = [INIT_NONE; TABLE_MAX_PAGES];

        Ok(Self { pages_cache, file })
    }

    pub fn insert(&mut self, row: &Row, page_idx: usize) -> Result<(), PagerError> {
        if let Some(cache_elem) = self.pages_cache.get_mut(page_idx) {
            if let Some(ref mut curr_page) = cache_elem {
                match curr_page.insert(row.clone()) {
                    Err(PageError::PageFull) => {
                        curr_page.write_header();
                        if page_idx >= TABLE_MAX_PAGES {
                            return Err(PagerError::TableFull);
                        };
                        return Err(PagerError::PageFull);
                    }
                    Err(other_err) => {
                        return Err(PagerError::PageRowInsertError(other_err.to_string()))
                    }
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
        if let Some(file) = &mut self.file {
            let _ = file.seek(SeekFrom::Start((page_idx * PAGE_SIZE) as u64));
            let page_to_write = self.pages_cache.get(page_idx).unwrap().as_ref().unwrap();
            let _ = file.write_all(page_to_write.clone().serialize());
        } else {
            return Err(PagerError::DbClosed);
        }
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
                Err(PagerError::DbClosed) => return Err(PagerError::DbClosed),
                _ => unreachable!(),
            };
        }
        Ok(())
    }
}
