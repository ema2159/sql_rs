use std::env;
use std::error::Error;

use dialoguer::{theme::ColorfulTheme, BasicHistory, Input};

mod backend;
mod metacommand_processor;
mod sql_compiler;
mod virtual_machine;

use backend::database::Database;
use metacommand_processor::{open_metacommand, process_metacommand};
use sql_compiler::parse_statement;
use virtual_machine as VM;

fn process_input(input_str: &str, db_instance: &mut Option<Database>) {
    if input_str.starts_with('.') {
        if let Err(metacommand_err) = process_metacommand(input_str, db_instance) {
            eprintln!("{}", metacommand_err)
        }
        return;
    }
    match parse_statement(input_str) {
        Ok(parsed_statement) => {
            let _ = VM::execute_statement(parsed_statement, db_instance.as_mut())
                .inspect_err(|err| eprintln!("{}", err.to_string()));
        }
        Err(parse_error) => eprintln!("{}", parse_error),
    }
}

fn parse_args(db_instance: &mut Option<Database>, args: Vec<String>) {
    if args.len() > 1 {
        let _ = open_metacommand(db_instance, args[1..].to_vec())
            .inspect_err(|err| eprintln!("{}", err.to_string()));
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let mut db_instance = None;

    parse_args(&mut db_instance, args);

    let mut prompt_history = BasicHistory::new().max_entries(8).no_duplicates(true);

    loop {
        if let Ok(input) = Input::<String>::with_theme(&ColorfulTheme::default())
            .with_prompt("db")
            .history_with(&mut prompt_history)
            .interact_text()
        {
            process_input(input.trim(), &mut db_instance);
        }
    }
}
