use std::error::Error;

use dialoguer::{theme::ColorfulTheme, BasicHistory, Input};

mod entities;
mod metacommand_processor;
mod sql_compiler;
mod virtual_machine;

use metacommand_processor::process_metacommand;
use sql_compiler::parse_statement;
use virtual_machine::execute_statement;


fn process_input(input_str: &str) {
    if input_str.starts_with('.') {
        process_metacommand(input_str);
        return;
    }
    match parse_statement(input_str) {
        Ok(parsed_statement) => execute_statement(parsed_statement),
        Err(parse_error) => println!("{}", parse_error),
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut history = BasicHistory::new().max_entries(8).no_duplicates(true);

    loop {
        if let Ok(input) = Input::<String>::with_theme(&ColorfulTheme::default())
            .with_prompt("db")
            .history_with(&mut history)
            .interact_text()
        {
            process_input(input.trim());
        }
    }
}
