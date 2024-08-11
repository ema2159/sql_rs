use std::env;
use std::fs;
use std::str::FromStr;

use thiserror::Error;

use crate::backend::database::Database;

const SUCCESS: i32 = 0;

enum Metacommand {
    Close,
    Databases,
    Exit,
}

#[derive(Error, Debug)]
pub enum MetacommandErr {
    #[error("No database open.")]
    DBClosed,
    #[error("Unrecognized Metacommand: {0}")]
    UnrecognizedMetacommand(String),
    #[error("Not a metacommand")]
    NotAMetacommand,
    #[error("Error when executing .databases metacommand: {0}")]
    ListDatabasesError(String),
}

fn list_db_files() -> std::io::Result<()> {
    // Get the current working directory
    let current_dir = env::current_dir()?;

    // Define the file extension to search for
    let extension = "db"; // Change this to the desired file extension

    // Iterate over the entries in the directory
    for entry in fs::read_dir(current_dir)? {
        let entry = entry?;
        let path = entry.path();

        // Check if the path is a file and if it has the desired extension
        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == extension {
                    println!("{}", path.display());
                }
            }
        }
    }

    Ok(())
}

fn close_database(db_instance: &mut Option<Database>) -> Result<(), MetacommandErr> {
    match db_instance {
        Some(db) => {
            db.close();
            *db_instance = None;
            Ok(())
        }
        None => Err(MetacommandErr::DBClosed),
    }
}

fn databases_metacommand() -> Result<(), MetacommandErr> {
    if let Err(list_db_files_err) = list_db_files() {
        Err(MetacommandErr::ListDatabasesError(
            list_db_files_err.to_string(),
        ))
    } else {
        Ok(())
    }
}

impl FromStr for Metacommand {
    type Err = MetacommandErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.strip_prefix('.').ok_or(MetacommandErr::NotAMetacommand)? {
            "close" => Ok(Metacommand::Close),
            "databases" => Ok(Metacommand::Databases),
            "exit" => Ok(Metacommand::Exit),
            _ => Err(MetacommandErr::UnrecognizedMetacommand(s.to_string())),
        }
    }
}

pub fn process_metacommand(
    metacommand_str: &str,
    db_instance: &mut Option<Database>,
) -> Result<(), MetacommandErr> {
    let metacommand = Metacommand::from_str(metacommand_str)?;

    match metacommand {
        Metacommand::Close => close_database(db_instance),
        Metacommand::Databases => databases_metacommand(),
        Metacommand::Exit => std::process::exit(SUCCESS),
    }
}
