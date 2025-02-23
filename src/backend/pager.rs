use std::cell::RefCell;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::rc::Rc;

use thiserror::Error;
use tracing::{instrument, trace};

use super::cursor::DBCursor;
use super::page::{Page, PageError, PageType, PAGE_SIZE};

const TABLE_MAX_PAGES: usize = 100;

#[derive(Error, Debug)]
pub enum PagerError {
    #[error("Cache miss")]
    CacheMiss,
    #[error("Page already exists in pager")]
    PageExists,
    #[error("Page index out of range")]
    PageIdxOutOfRange,
    #[error("Cannot insert row. Remaining page capacity is smaller than the row size")]
    PageFull,
    #[error("Could not insert row in page. The following error ocurred during insertion: {0}")]
    PageRowInsertError(#[from] PageError),
    #[error("Table full")]
    TableFull,
}

#[derive(Debug)]
pub struct Pager {
    num_pages: u32,
    pages_cache: [Option<Page>; TABLE_MAX_PAGES],
    root_page_num: u32,
    file_ref: Rc<RefCell<File>>,
}

impl Pager {
    #[instrument(parent = None, level = "trace")]
    pub fn new(file: Rc<RefCell<File>>, root_page_num: u32) -> Pager {
        const INIT_NONE: Option<Page> = None;
        let mut pages_cache: [Option<Page>; TABLE_MAX_PAGES] = [INIT_NONE; TABLE_MAX_PAGES];
        pages_cache[root_page_num as usize] = Some(Page::new(PageType::Leaf));

        Self {
            num_pages: 1,
            pages_cache: pages_cache.into(),
            root_page_num,
            file_ref: file,
        }
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn insert(
        &mut self,
        cursor: &mut DBCursor,
        key: u64,
        value: &[u8],
    ) -> Result<(), PagerError> {
        let page_option = self
            .pages_cache
            .get_mut(cursor.page_num as usize)
            .ok_or(PagerError::PageIdxOutOfRange)?;

        if let Some(mut curr_page) = page_option.take() {
            let partition = curr_page.find_partition(key);
            if partition.is_err() {
                self.pages_cache[cursor.page_num as usize] = Some(curr_page);
                partition?;
                unreachable!()
            };
            cursor.cell_ptr_pos = partition?;

            match curr_page.insert(cursor.cell_ptr_pos, key, value) {
                Ok(()) => {
                    self.pages_cache[cursor.page_num as usize] = Some(curr_page);
                    Ok(())
                }
                Err(PageError::PageFull) => {
                    if self.num_pages as usize >= TABLE_MAX_PAGES {
                        return Err(PagerError::TableFull);
                    };

                    let new_page = curr_page.split_page();
                    let new_page_number = self.get_unused_page_number();
                    self.num_pages += 1;

                    // Handle root split
                    if cursor.page_num == self.root_page_num {
                        self.handle_root_split(curr_page, new_page, new_page_number)?;
                    }

                    Ok(())
                }
                Err(err) => {
                    self.pages_cache[cursor.page_num as usize] = Some(curr_page);
                    Err(PagerError::PageRowInsertError(err))
                }
            }
        } else {
            // Cache miss
            self.get_page_from_disk(cursor);
            self.insert(cursor, key, value)
        }
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    fn handle_root_split(
        &mut self,
        old_root: Page,
        new_page: Page,
        new_page_number: u32,
    ) -> Result<(), PagerError> {
        // Create a new root and make old root its left child
        let right_page_number = self.get_unused_page_number();
        let right_page = old_root;
        let mut new_root = Page::new(PageType::Interior);

        // New page is new root's left page
        let left_page = new_page;
        let left_page_last_key = left_page.get_last_key()?;
        let left_page_number = new_page_number;

        let key_insert_position = new_root.find_partition(left_page_last_key)?;
        new_root.insert(
            key_insert_position,
            left_page_last_key,
            &left_page_number.to_be_bytes(),
        )?;

        new_root.set_right_pointer(right_page_number);

        self.page_write(new_root, self.root_page_num as usize);
        self.page_write(right_page, right_page_number as usize);
        self.page_write(left_page, left_page_number as usize);

        Ok(())
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    fn get_unused_page_number(&self) -> u32 {
        self.num_pages
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    fn get_page_from_disk(&self, cursor: &DBCursor) {
        todo!()
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    fn page_write(&mut self, page: Page, page_idx: usize) {
        self.pages_cache[page_idx] = Some(page);
        self.flush_page(page_idx);
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn flush_page(&mut self, page_idx: usize) -> Result<(), PagerError> {
        if page_idx >= TABLE_MAX_PAGES {
            return Err(PagerError::PageIdxOutOfRange);
        }

        let mut file = self.file_ref.borrow_mut();
        let _ = file.seek(SeekFrom::Start((page_idx * PAGE_SIZE) as u64));
        let page_to_write = self.pages_cache[page_idx].as_ref().unwrap().clone();
        let bytes: [u8; PAGE_SIZE] = page_to_write.into();
        let _ = file.write_all(&bytes);
        Ok(())
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn flush_all(&mut self) -> Result<(), PagerError> {
        let flush_indices: Vec<usize> = self
            .pages_cache
            .iter()
            .enumerate()
            .filter(|x| x.1.is_some())
            .map(|x| x.0)
            .collect();

        for i in flush_indices {
            match self.flush_page(i) {
                Ok(()) => {}
                Err(PagerError::PageIdxOutOfRange) => {}
                _ => unreachable!(),
            };
        }
        Ok(())
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn pages(&self) -> impl Iterator<Item = &Option<Page>> {
        self.pages_cache.iter()
    }
}
