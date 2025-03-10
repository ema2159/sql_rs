use std::error::Error;
use std::fs::File;
use std::panic;
use std::sync::Mutex;
use std::{env, fmt::Debug};

use dialoguer::{theme::ColorfulTheme, BasicHistory, Input};

use tracing::{error, info, instrument, span, trace, Level};
use tracing_subscriber::{self, filter, fmt, fmt::format::FmtSpan, prelude::*, reload};
use tracing_subscriber::{EnvFilter, Registry};

mod backend;
mod metacommand_processor;
mod sql_compiler;
mod virtual_machine;

use backend::database::Database;
use metacommand_processor::{open_metacommand, process_metacommand};
use sql_compiler::parse_statement;
use virtual_machine as VM;

#[instrument(parent = None, ret, level = "trace", skip(db_instance))]
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
                .inspect_err(|err| eprintln!("{}", err));
        }
        Err(parse_error) => eprintln!("{}", parse_error),
    }
}

fn parse_args(db_instance: &mut Option<Database>, args: Vec<String>) {
    if args.len() > 1 {
        let _ = open_metacommand(db_instance, args[1..].to_vec())
            .inspect_err(|err| eprintln!("{}", err));
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let traces_file = File::create("output.log")?;
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_span_events(FmtSpan::ENTER)
        .with_writer(Mutex::new(traces_file))
        .with_file(true)
        .with_line_number(true)
        .without_time()
        .init();

    panic::set_hook(Box::new(move |panic_info| {
        error!(parent: None, "{}", panic_info);
        eprintln!("{}", panic_info);
    }));

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
