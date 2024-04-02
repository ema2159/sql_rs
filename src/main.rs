use std::io::{self, BufRead, Write};

fn print_prompt() {
    print!("db > ");
    io::stdout().flush().expect("Could not print prompt");
}

fn process_input(input_str: &str) {
    if input_str == ".exit" {
        std::process::exit(0);
    } else {
        println!("Unrecognized command {}", input_str);
    }
}

fn main() {
    let mut input_buffer = String::new();

    loop {
        print_prompt();
        input_buffer.clear();
        io::stdin().lock().read_line(&mut input_buffer).expect("Could not read line");

        process_input(input_buffer.trim_end());
    }
}
