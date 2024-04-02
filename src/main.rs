use std::io::{self, BufRead, Write};

fn print_prompt() {
    print!("db > ");
    io::stdout().flush();
}

fn main() {
    let mut input_buffer = String::new();

    loop {
        print_prompt();
        input_buffer.clear();
        io::stdin().lock().read_line(&mut input_buffer).expect("Could not read line");

        println!("{}", input_buffer.trim_end());
    }
}
