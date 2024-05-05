use core::fmt::Display;

use super::create::CreateTokens;
use super::insert::RowToInsert;

#[derive(Debug)]
pub enum Statement<'a> {
    Create(CreateTokens<'a>),
    Insert(RowToInsert<'a>),
    Select,
}

#[derive(Debug)]
pub enum StatementType {
    Create,
    Insert,
    Select,
}

impl TryFrom<&str> for StatementType {
    type Error = ParseError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "create" => Ok(StatementType::Create),
            "insert" => Ok(StatementType::Insert),
            "select" => Ok(StatementType::Select),
            _ => Err(ParseError::UnknownStatement),
        }
    }
}

#[derive(Debug)]
pub enum ParseError {
    MalformedStatement(String),
    UnknownStatement,
}


impl Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::MalformedStatement(parse_trace) => write!(
                f,
                "Error encountered when parsing statement: \n {}",
                parse_trace
            ),
            ParseError::UnknownStatement => write!(
                f,
                "Unrecognized statement"
            ),
        }
    }
}

impl std::error::Error for ParseError {}
