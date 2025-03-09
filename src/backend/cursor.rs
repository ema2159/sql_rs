use std::fmt;

use crate::backend::table::Table;

#[derive(Debug, Clone)]
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
