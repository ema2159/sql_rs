use std::env;
use std::error::Error;

use dialoguer::{theme::ColorfulTheme, BasicHistory, Input};

mod backend;
mod metacommand_processor;
mod sql_compiler;
mod virtual_machine;

use backend::database::Database;
use metacommand_processor::process_metacommand;
use sql_compiler::parse_statement;
use virtual_machine as VM;

fn process_input(input_str: &str, db_instance: Option<&mut Database>) {
    if input_str.starts_with('.') {
        process_metacommand(input_str);
        return;
    }
    match parse_statement(input_str) {
        Ok(parsed_statement) => {
            println!("{:?}", VM::execute_statement(parsed_statement, db_instance))
        }
        Err(parse_error) => println!("{}", parse_error),
    }
}

fn parse_args(args: Vec<String>) -> Option<Database> {
    match args.len() {
        1 => None,
        2 => {
            let db_file_name = &args[1];
            match Database::open(db_file_name) {
                Ok(db) => Some(db),
                Err(err) => {
                    eprintln!(
                        "Cannot open database {}. Encountered the following error: {}",
                        db_file_name, err
                    );
                    None
                }
            }
        }
        _ => {
            eprintln!("Too many arguments. Please provide a single database to open.");
            std::process::exit(1);
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let mut db_from_args = parse_args(args);
    let mut db_instance = db_from_args.take();

    let mut prompt_history = BasicHistory::new().max_entries(8).no_duplicates(true);

    loop {
        if let Ok(input) = Input::<String>::with_theme(&ColorfulTheme::default())
            .with_prompt("db")
            .history_with(&mut prompt_history)
            .interact_text()
        {
            process_input(input.trim(), db_instance.as_mut());
        }
    }
}
