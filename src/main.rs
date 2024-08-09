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
        Ok(parsed_statement) => println!("{:?}", VM::execute_statement(parsed_statement, db_instance)),
        Err(parse_error) => println!("{}", parse_error),
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();

    // if args.len() == 1 {
    //     db_instance = None;
    // } else if args.len() == 2 {
    //     let db_file_name = &args[1];
    //     db_instance = Some(&mut Database::open(db_file_name)?)
    // } else {
    //     panic!("Too many arguments. Please provide a single database to open.")
    // }
    let db_file_name = &args[1];
    let mut db_instance = Database::open(db_file_name)?;

    let mut prompt_history = BasicHistory::new().max_entries(8).no_duplicates(true);

    loop {
        if let Ok(input) = Input::<String>::with_theme(&ColorfulTheme::default())
            .with_prompt("db")
            .history_with(&mut prompt_history)
            .interact_text()
        {
            process_input(input.trim(), Some(&mut db_instance));
        }
    }
}
