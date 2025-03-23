use std::cell::RefCell;
use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};
use std::ops::DerefMut;
use std::rc::Rc;

use ptree::item::StringItem;
use ptree::{print_tree, TreeBuilder};
use thiserror::Error;
use tracing::instrument;

use super::cursor::DBCursor;
use super::page::{Page, PageError, PageType, PAGE_SIZE};

const TABLE_MAX_PAGES: usize = 100;

#[derive(Error, Debug)]
pub enum PagerError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
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
    num_pages: u32, // TODO: Needs to be a reference to a database element, not a per-table item
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
    /// Return a DB cursor pointing to the leaf node in which the record would be inserted.
    /// Recursively traverses the nodes from the root, loading the pages from disk if they are not
    /// present in the cache.
    ///
    /// * `cursor`: DB cursor. Passed by value and returned by the function * `key`: Key of the
    /// record to insert
    pub fn get_leaf_insertion_position<'a>(
        &mut self,
        mut cursor: DBCursor<'a>,
        key: u64,
    ) -> Result<DBCursor<'a>, PagerError> {
        let curr_page = self.retrieve_page(cursor.page_num)?;

        let cursor = if *curr_page.get_page_type() == PageType::Leaf {
            cursor
        } else {
            let next_page_number = curr_page.get_next_page_pointer(key)?;
            cursor.parents_stack.push(cursor.page_num);
            cursor.page_num = next_page_number;
            self.get_leaf_insertion_position(cursor, key)?
        };
        Ok(cursor)
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    /// Insert record into insertion position specified by the cursor
    ///
    /// * `cursor`: DB cursor containing pointing to the exact position in which the record shall
    /// be inserted in the page
    /// * `key`: Key of the record to insert
    /// * `payload`: Payload of the record to insert
    /// * `left_child`: Left child of the record to insert (for interior pages)
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
            match curr_page.insert(key, payload, left_child) {
                Ok(()) => {
                    self.page_write(curr_page, cursor.page_num as usize)?;
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
                        cursor.page_num = left_split_page_number;
                    } else {
                        cursor.page_num = right_split_page_number;
                    };

                    self.pages_cache[left_split_page_number as usize] = Some(page_left_split);
                    self.pages_cache[right_split_page_number as usize] = Some(page_right_split);
                    // Record still might be too large so it can retrigger a split
                    self.insert(cursor, key, payload, left_child)?;
                    self.flush_page(left_split_page_number as usize)?;
                    self.flush_page(right_split_page_number as usize)?;
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
    /// Called after a root node split. It creates a new root with a single record with the
    /// largest key of the left split of the old root which points to it, and a right pointer
    /// pointing to the right split of the old root.
    /// Pre:
    /// ```text
    ///
    ///         ┌─────────────┌┐
    ///         │ 0 1 2 3 4 5 ││
    ///         └─────────────└┘
    /// ```
    /// Post:
    /// ``` text
    ///              ┌───┌┐
    ///              │ 2 ││─────┐
    ///              └─┬─└┘     │
    /// ┌──────────────┘        │
    /// │  ┌───────┌┐       ┌───▼───┌┐
    /// └─►│ 0 1 2 ││       │ 3 4 5 ││
    ///    └───────└┘       └───────└┘
    /// ```
    /// * `page_left_split`: Old root split containing the lowest records.
    /// * `page_right_split`: Old root split containing the highest records.
    /// * `new_page_number`: New page number assigned to one of the splits. Other split reuses
    /// existing number.
    fn handle_root_split(
        &mut self,
        page_left_split: &Page,
        page_right_split: &Page,
        new_page_number: u32,
    ) -> Result<(u32, u32), PagerError> {
        let left_split_page_number = new_page_number;
        let right_split_page_number = self.get_unused_page_number();

        let mut new_root = Page::new(PageType::Interior);

        let left_split_last_key = page_left_split.get_last_key()?;

        new_root.insert(left_split_last_key, &[], Some(left_split_page_number))?;

        new_root.set_right_pointer(right_split_page_number);

        self.page_write(new_root, self.root_page_num as usize)?;
        self.num_pages += 1;

        Ok((left_split_page_number, right_split_page_number))
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    /// Called after a node split. It modifies the parent of the former node, modifying in place
    /// the existing record (or right page pointer) that pointed to it to point to its right split,
    /// and then tries to add a new record pointing to the left split. This action might
    /// recursively trigger a split in the former record parend and all subsequent nodes up the
    /// tree.
    /// Pre:
    /// ``` text
    ///            ┌───────┌┐
    ///            │ 0 3 9 ││
    ///            └─────┬─└┘
    ///              ┌───┘
    ///       ┌──────▼──────┌┐
    ///       │ 4 5 6 7 8 9 ││
    ///       └─────────────└┘
    /// ```
    /// Post:
    /// ``` text
    ///         ┌─────────┌┐
    ///         │ 0 3 6 9 ││
    ///         └─────┬─┬─└┘
    ///               │ │
    ///     ┌─────────┘ └─────┐
    /// ┌───▼───┌┐        ┌───▼───┌┐
    /// │ 4 5 6 ││        │ 7 8 9 ││
    /// └───────└┘        └───────└┘
    /// ```
    /// * `page_left_split`: Page split containing the lowest records.
    /// * `page_right_split`:  Page split containing the highest records.
    /// * `new_page_number`: New page number assigned to one of the splits. Other split reuses
    /// existing number.
    /// * `cursor`: Reference to DB cursor with pre-split page num and parents stack
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
        let (key_insert_position, curr_key_in_partition) = split_page_parent.find_partition(right_split_last_key)?;
        // TODO: Create a should insert in right pointer method
        if curr_key_in_partition.is_none() {
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
                                                                              // be put back
        cursor.page_num = parent_page_num;
        self.insert(
            cursor,
            left_split_last_key,
            &[],
            Some(left_split_page_number),
        )?;

        Ok((left_split_page_number, right_split_page_number))
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    fn get_unused_page_number(&self) -> u32 {
        self.num_pages
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    fn get_page_from_disk(&self, page_num: u32) -> Result<Page, PagerError> {
        let page_num = page_num as usize;
        let mut file = self.file_ref.borrow_mut();
        file.seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))?;
        Ok(Page::new_from_read(file.deref_mut())?)
    }

    fn retrieve_page(&mut self, page_num: u32) -> Result<&mut Page, PagerError> {
        if !self.page_exists(page_num) {
            return Err(PagerError::PageNonExistent);
        }

        // First, check if we need to load from disk
        let page_from_disk = if self
            .pages_cache
            .get(page_num as usize)
            .is_some_and(|p| p.is_none())
        {
            Some(self.get_page_from_disk(page_num)?)
        } else {
            None
        };

        // Now, we safely get a mutable reference since the immutable borrow is done
        let page_slot = self
            .pages_cache
            .get_mut(page_num as usize)
            .ok_or(PagerError::PageIdxOutOfRange)?;

        // Insert the page if necessary
        if let Some(page) = page_from_disk {
            *page_slot = Some(page);
        }

        page_slot.as_mut().ok_or(PagerError::PageIdxOutOfRange)
    }

    fn page_exists(&mut self, page_num: u32) -> bool {
        page_num < self.num_pages
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    fn page_write(&mut self, page: Page, page_idx: usize) -> Result<(), PagerError> {
        self.pages_cache[page_idx] = Some(page);
        self.flush_page(page_idx)?;
        Ok(())
    }

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn flush_page(&mut self, page_idx: usize) -> Result<(), PagerError> {
        let mut file = self.file_ref.borrow_mut();
        file.seek(SeekFrom::Start((page_idx * PAGE_SIZE) as u64))?;
        let page_to_write = self.pages_cache[page_idx].as_ref().unwrap().clone();
        let bytes: [u8; PAGE_SIZE] = page_to_write.into();
        file.write_all(&bytes)?;
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

    #[instrument(parent = None, skip(self), ret, level = "trace")]
    pub fn leaf_pages(&self) -> impl Iterator<Item = &Option<Page>> {
        fn get_page_children(page: &Page) -> impl Iterator<Item = Result<u32, PageError>> + '_ {
            page.children_iter()
        }
        self.pages_cache.iter()
    }

    fn create_tree(&mut self) -> Result<StringItem, PagerError> {
        fn add_page_recursively(
            pager: &mut Pager,
            tree_builder: &mut TreeBuilder,
            page_num: u32,
        ) -> Result<(), PagerError> {
            let page: &_ = pager.retrieve_page(page_num)?;
            tree_builder.begin_child(format!("{}{}", page_num, page));
            for child_num in page.children_iter().collect::<Vec<_>>() {
                add_page_recursively(pager, tree_builder, child_num?)?
            }
            tree_builder.end_child();
            Ok(())
        }
        let mut tree_builder = TreeBuilder::new("DB tree".to_string());
        add_page_recursively(self, &mut tree_builder, self.root_page_num)?;
        Ok(tree_builder.build())
    }

    pub fn print_tree(&mut self) -> Result<(), PagerError> {
        let tree = self.create_tree().unwrap();
        print_tree(&tree)?;
        Ok(())
    }
}
