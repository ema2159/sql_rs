use std::env;
use std::fs;
use std::str::FromStr;

use crate::backend::database::Database;

const SUCCESS: i32 = 0;

enum Metacommand {
    Close,
    Databases,
    Exit,
    // Open,
}

enum MetacommandErr {
    UnrecognizedMetacommand,
    NotAMetacommand,
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

fn close_database(db_instance: &mut Option<Database>) {
    match db_instance {
        Some(db) => {
            db.close();
            *db_instance = None;
        }
        None => eprintln!("No database open."),
    }
}

fn databases_metacommand() {
    if let Err(list_db_files_err) = list_db_files() {
        println!(
            "Error when executing .databases metacommand: {}",
            list_db_files_err.to_string()
        );
    }
}

impl FromStr for Metacommand {
    type Err = MetacommandErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.strip_prefix('.').ok_or(MetacommandErr::NotAMetacommand)? {
            "close" => Ok(Metacommand::Close),
            "databases" => Ok(Metacommand::Databases),
            "exit" => Ok(Metacommand::Exit),
            _ => Err(MetacommandErr::UnrecognizedMetacommand),
        }
    }
}

pub fn process_metacommand(metacommand_str: &str, db_instance: &mut Option<Database>) {
    if let Ok(metacommand) = Metacommand::from_str(metacommand_str) {
        match metacommand {
            Metacommand::Close => close_database(db_instance),
            Metacommand::Databases => databases_metacommand(),
            Metacommand::Exit => std::process::exit(SUCCESS),
        }
    } else {
        println!("Unrecognized meta command {}", metacommand_str);
    }
}
