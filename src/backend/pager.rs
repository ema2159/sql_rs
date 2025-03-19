use std::cell::RefCell;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::rc::Rc;

use ptree::item::StringItem;
use ptree::{print_tree, TreeBuilder};
use thiserror::Error;
use tracing::{instrument, trace};

use super::cursor::DBCursor;
use super::page::{Page, PageError, PageType, PAGE_SIZE};

const TABLE_MAX_PAGES: usize = 100;

#[derive(Error, Debug)]
pub enum PagerError {
    #[error("Cannot insert element. Record with the same key already exists.")]
    DuplicateKey,
    #[error("Page index out of range")]
    PageIdxOutOfRange,
    #[error("Could not insert row in page. The following error ocurred during insertion: {0}")]
    PageRowInsertError(#[from] PageError),
    #[error("Trying to access non-existing page")]
    PageNonExistent,
    #[error("No parent node left in parents stack")]
    ParentStackEmpty,
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
            pages_cache,
            root_page_num,
            file_ref: file,
        }
    }

    #[instrument(parent = None, skip(self, cursor),ret, level = "trace")]
    pub fn get_leaf_insertion_position(
        &self,
        cursor: &mut DBCursor,
        key: u64,
    ) -> Result<(), PagerError> {
        let page_option = self
            .pages_cache
            .get(cursor.page_num as usize)
            .ok_or(PagerError::PageIdxOutOfRange)?;

        if let Some(curr_page) = page_option {
            if *curr_page.get_page_type() == PageType::Leaf {
                let (partition, partition_key_opt) = curr_page.find_partition(key)?;
                if let Some(partition_key) = partition_key_opt {
                    if partition_key == key {
                        return Err(PagerError::DuplicateKey);
                    }
                }
                cursor.cell_ptr_pos = partition;
            } else {
                let next_page_number = curr_page.get_next_page_pointer(key)?;
                cursor.parents_stack.push(cursor.page_num);
                cursor.page_num = next_page_number;
                self.get_leaf_insertion_position(cursor, key)?;
            }
        }
        Ok(())
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn insert(
        &mut self,
        cursor: &mut DBCursor,
        key: u64,
        payload: &[u8],
        left_child: Option<u32>,
    ) -> Result<(), PagerError> {
        let page_option = self
            .pages_cache
            .get_mut(cursor.page_num as usize)
            .ok_or(PagerError::PageIdxOutOfRange)?;

        if let Some(mut curr_page) = page_option.take() {
            match curr_page.insert(cursor.cell_ptr_pos, key, payload, left_child) {
                Ok(()) => {
                    self.page_write(curr_page, cursor.page_num as usize);
                }
                Err(PageError::PageFull) => {
                    if self.num_pages as usize >= TABLE_MAX_PAGES {
                        return Err(PagerError::TableFull);
                    };

                    let (mut page_left_split, page_right_split) = curr_page.split_page();
                    let left_split_last_key = page_left_split.get_last_key()?;
                    let new_page_number = self.get_unused_page_number();
                    self.num_pages += 1;

                    let (left_split_page_number, right_split_page_number) =
                        if cursor.page_num == self.root_page_num {
                            self.handle_root_split(
                                &page_left_split,
                                &page_right_split,
                                new_page_number,
                                cursor,
                            )?
                        } else {
                            self.handle_page_split(
                                &page_left_split,
                                &page_right_split,
                                new_page_number,
                                cursor,
                            )?
                        };

                    if *page_left_split.get_page_type() == PageType::Interior {
                        page_left_split.move_last_left_child_to_right_pointer()?;
                    }

                    // Retry insert after split
                    if key < left_split_last_key {
                        let (partition, _partition_key_opt) =
                            page_left_split.find_partition(key)?;
                        cursor.page_num = left_split_page_number;
                        cursor.cell_ptr_pos = partition;
                    } else {
                        let (partition, _partition_key_opt) =
                            page_right_split.find_partition(key)?;
                        cursor.cell_ptr_pos = partition;
                        cursor.page_num = right_split_page_number;
                    };

                    self.pages_cache[left_split_page_number as usize] = Some(page_left_split);
                    self.pages_cache[right_split_page_number as usize] = Some(page_right_split);
                    // Record still might be too large so it can retrigger a split
                    let _ = self.insert(cursor, key, payload, left_child);
                    self.flush_page(left_split_page_number as usize);
                    self.flush_page(right_split_page_number as usize);
                }
                Err(err) => {
                    self.pages_cache[cursor.page_num as usize] = Some(curr_page);
                    Err(PagerError::PageRowInsertError(err))?
                }
            }
        }
        Ok(())
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    fn handle_root_split(
        &mut self,
        page_left_split: &Page,
        page_right_split: &Page,
        new_page_number: u32,
        cursor: &mut DBCursor,
    ) -> Result<(u32, u32), PagerError> {
        let left_split_page_number = new_page_number;
        let right_split_page_number = self.get_unused_page_number();

        let mut new_root = Page::new(PageType::Interior);

        let left_split_last_key = page_left_split.get_last_key()?;

        let (key_insert_position, _) = new_root.find_partition(left_split_last_key)?;
        new_root.insert(
            key_insert_position,
            left_split_last_key,
            &[],
            Some(left_split_page_number),
        )?;

        new_root.set_right_pointer(right_split_page_number);

        cursor.page_num = left_split_page_number;

        self.page_write(new_root, self.root_page_num as usize);
        self.num_pages += 1;

        Ok((left_split_page_number, right_split_page_number))
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    fn handle_page_split(
        &mut self,
        page_left_split: &Page,
        page_right_split: &Page,
        new_page_number: u32,
        cursor: &mut DBCursor,
    ) -> Result<(u32, u32), PagerError> {
        let parent_page_num = cursor
            .parents_stack
            .pop()
            .ok_or(PagerError::ParentStackEmpty)?;
        let split_page_parent_opt = &mut self
            .pages_cache
            .get_mut(parent_page_num as usize)
            .ok_or(PagerError::PageIdxOutOfRange)?;
        let Some(mut split_page_parent) = split_page_parent_opt.take() else {
            return Err(PagerError::PageNonExistent);
        };

        let right_split_last_key = page_right_split.get_last_key()?;
        let right_split_page_number = new_page_number;

        // First modify parent page record to point to the right half of the page after the split
        let left_split_last_key = page_left_split.get_last_key()?;
        let left_split_page_number = cursor.page_num;
        let (key_insert_position, _) = split_page_parent.find_partition(right_split_last_key)?;
        if key_insert_position >= PAGE_SIZE {
            split_page_parent.set_right_pointer(right_split_page_number);
        } else {
            split_page_parent.update_same_size(
                key_insert_position,
                right_split_last_key,
                &[],
                Some(right_split_page_number),
            )?;
        }

        // Only after modifying the parent page in place, add a new record pointing to the left
        // half of the page. This has to be done this order because this recursive call to insert
        // might trigger splits further up in the tree.
        self.pages_cache[parent_page_num as usize] = Some(split_page_parent); // Page was taken
                                                                              // before, needs to
        cursor.page_num = parent_page_num;
        self.insert(
            cursor,
            left_split_last_key,
            &[],
            Some(left_split_page_number),
        )?;
        // Return cursor to its original position
        cursor.page_num = right_split_page_number;

        Ok((left_split_page_number, right_split_page_number))
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

    fn create_tree(&self) -> Result<StringItem, PagerError> {
        #[instrument(parent = None, skip(pager, tree_builder), ret, level = "trace")]
        fn add_page_recursively(
            pager: &Pager,
            tree_builder: &mut TreeBuilder,
            page_num: u32,
        ) -> Result<(), PagerError> {
            let page_opt = pager
                .pages_cache
                .get(page_num as usize)
                .ok_or(PagerError::PageIdxOutOfRange)?
                .as_ref();
            let page = page_opt.ok_or(PagerError::PageNonExistent)?;
            tree_builder.begin_child(format!("{}{}", page_num, page));
            for child_num in page.children_iter() {
                add_page_recursively(pager, tree_builder, child_num?)?
            }
            tree_builder.end_child();
            Ok(())
        }
        let mut tree_builder = TreeBuilder::new("DB tree".to_string());
        add_page_recursively(self, &mut tree_builder, self.root_page_num)?;
        Ok(tree_builder.build())
    }

    pub fn print_tree(&self) {
        let tree = self.create_tree().unwrap();
        print_tree(&tree);
    }
}
