use std::error::Error;
use std::io::{self, BufRead, Write};

mod metacommand_processor;
mod sql_compiler;
mod virtual_machine;

use metacommand_processor::process_metacommand;
use sql_compiler::parse_statement;
use virtual_machine::execute_statement;

fn print_prompt() -> Result<(), Box<dyn Error>> {
    print!("db > ");
    io::stdout().flush()?;
    Ok(())
}

fn process_input(input_str: &str) {
    if input_str.starts_with('.') {
        process_metacommand(input_str);
    } else {
        if let Ok(parsed_statement) = parse_statement(input_str) {
            execute_statement(parsed_statement);
        } else {
            println!("Unrecognized keyword at start of {}", input_str);
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut input_buffer = String::new();

    loop {
        print_prompt()?;
        input_buffer.clear();
        io::stdin().lock().read_line(&mut input_buffer)?;

        process_input(input_buffer.trim_end());
    }
}
