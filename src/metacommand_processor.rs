use std::str::FromStr;

const SUCCESS: i32 = 0;

enum Metacommand {
    Exit,
}

enum MetacommandErr {
    UnrecognizedMetacommand,
    NotAMetacommand,
}

impl FromStr for Metacommand {
    type Err = MetacommandErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.strip_prefix('.').ok_or(MetacommandErr::NotAMetacommand)? {
            "exit" => Ok(Metacommand::Exit),
            _ => Err(MetacommandErr::UnrecognizedMetacommand),
        }
    }
}

pub fn process_metacommand(metacommand_str: &str) {
    if let Ok(metacommand) = Metacommand::from_str(metacommand_str) {
        match metacommand {
            Metacommand::Exit => std::process::exit(SUCCESS),
        }
    } else {
        println!("Unrecognized meta command {}", metacommand_str);
    }
}
