use super::insert::RowToInsert;

#[derive(Debug)]
pub enum StatementType {
    Insert,
    Select,
}

#[derive(Debug)]
pub enum StatementContents<'a> {
    Insert(RowToInsert<'a>),
    Select,
}

#[derive(Debug)]
pub struct Statement<'a> {
    pub statement_type: StatementType,
    pub statement_contents: StatementContents<'a>,
}

pub enum ParseError {
    UnknownStatement,
}
