use std::fmt;

use crate::backend::table::Table;

#[derive(Debug, Clone)]
/// A cursor is a pointer to a particular entry within a particular
/// b-tree within a database file.
/// A single database file can be shared by two more database connections,
/// but cursors cannot be shared.
///
/// * `table`: Table that contains the record the cursor is pointing to
/// * `page_num`: Page number of the record the cursor is pointing to
/// * `cell_ptr_pos`: The byte offset in the page that corresponds to the record
/// * `parents_stack`: A stack with the current page's parent pages
pub struct DBCursor<'a> {
    table: &'a Table,
    pub page_num: u32,
    pub cell_ptr_pos: usize,
    pub parents_stack: Vec<u32>,
}

impl<'a, 'b> DBCursor<'b>
where
    'a: 'b,
{
    pub fn new(table: &'a Table) -> Self {
        DBCursor {
            table,
            page_num: 0,
            cell_ptr_pos: 0,
            parents_stack: Vec::new(),
        }
    }
}

impl fmt::Display for DBCursor<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Cursor: [Table {}], [Page Num {}], [Cell Ptr Position {}]",
            self.table.name, self.page_num, self.cell_ptr_pos
        )
    }
}
