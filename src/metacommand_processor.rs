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
    Open,
}

#[derive(Error, Debug)]
pub enum MetacommandErr {
    #[error("No database open.")]
    DBClosed,
    #[error("Extra argument: : {0}")]
    ExtraArgument(String),
    #[error("Error when executing .databases metacommand: {0}")]
    ListDatabasesError(String),
    #[error("Not a metacommand")]
    NotAMetacommand,
    #[error("Cannot open database {0}. Encountered the following error: {1}")]
    OpenDBError(String, String),
    #[error("Unrecognized Metacommand: {0}")]
    UnrecognizedMetacommand(String),
}

fn list_db_files() -> std::io::Result<()> {
    let current_dir = env::current_dir()?;

    let extension = "db";

    for entry in fs::read_dir(current_dir)? {
        let entry = entry?;
        let path = entry.path();

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

fn close_metacommand(db_instance: &mut Option<Database>) -> Result<(), MetacommandErr> {
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

fn exit_metacommand(db_instance: &mut Option<Database>) -> ! {
    let _ = close_metacommand(db_instance);
    std::process::exit(SUCCESS)
}

pub fn open_metacommand(
    db_instance: &mut Option<Database>,
    args: Vec<String>,
) -> Result<(), MetacommandErr> {
    if args.len() > 1 {
        return Err(MetacommandErr::ExtraArgument(args[1].to_string()));
    }

    let _ = close_metacommand(db_instance);

    let db_filename = &args[0];

    *db_instance =
        Some(Database::open(db_filename).map_err(|err| {
            MetacommandErr::OpenDBError(db_filename.to_string(), err.to_string())
        })?);

    Ok(())
}

impl FromStr for Metacommand {
    type Err = MetacommandErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.strip_prefix('.').ok_or(MetacommandErr::NotAMetacommand)? {
            "close" => Ok(Metacommand::Close),
            "databases" => Ok(Metacommand::Databases),
            "exit" => Ok(Metacommand::Exit),
            "open" => Ok(Metacommand::Open),
            _ => Err(MetacommandErr::UnrecognizedMetacommand(s.to_string())),
        }
    }
}

pub fn process_metacommand(
    input_str: &str,
    db_instance: &mut Option<Database>,
) -> Result<(), MetacommandErr> {
    let tokens: Vec<_> = input_str.split(" ").collect();

    // input_str cannot be empty, so unrwap is ok
    let (metacommand_str, args) = tokens.split_first().unwrap();
    let metacommand = Metacommand::from_str(metacommand_str)?;

    match metacommand {
        Metacommand::Close => close_metacommand(db_instance),
        Metacommand::Databases => databases_metacommand(),
        Metacommand::Exit => exit_metacommand(db_instance),
        Metacommand::Open => {
            open_metacommand(db_instance, args.iter().map(|s| s.to_string()).collect())
        }
    }
}
