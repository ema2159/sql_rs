#[derive(Debug)]
pub enum StatementType {
    Insert,
    Select,
}

#[derive(Debug)]
pub struct Statement {
    pub statement_type: StatementType,
}

pub enum ParseError {
    UnknownStatement,
}

pub fn parse_statement(statement_str: &str) -> Result<Statement, ParseError> {
    if statement_str.get(0..6) == Some("insert") {
        let parsed_statement = Statement {
            statement_type: StatementType::Insert,
        };
        Ok(parsed_statement)
    } else if statement_str.get(0..6) == Some("select") {
        let parsed_statement = Statement {
            statement_type: StatementType::Select,
        };
        Ok(parsed_statement)
    } else {
        Err(ParseError::UnknownStatement)
    }
}
