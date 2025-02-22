use crate::backend::table::Table;

#[derive(Debug, Clone)]
pub struct DBCursor<'a> {
    table: &'a Table,
    pub page_num: u32,
    pub cell_ptr_pos: usize,
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
        }
    }
}
