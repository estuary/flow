use std::str::FromStr;

use control::models::Id;

fn main() {
    let mut args = std::env::args();
    let _ = args.next(); // Pop the command name off the args list.

    let cmd = args.next().unwrap_or_else(|| {
        eprintln!("Must pass encode or decode, then a value.");
        std::process::exit(1);
    });
    let value = args.next().unwrap_or_else(|| {
        eprintln!("Must pass encode or decode, then a value.");
        std::process::exit(2);
    });

    match &*cmd {
        "encode" => encode(value),
        "decode" => decode(value),
        other => {
            eprintln!(
                "Command not recognized: {}. Must pass encode or decode, then a value.",
                other
            );
            std::process::exit(2);
        }
    }
}

fn encode(value: String) {
    let i = i64::from_str_radix(&value, 10).expect("Value to be an i64");
    let id = Id::new(i);
    println!("{}", id);
}

fn decode(value: String) {
    let id = Id::from_str(&value).expect("A valid id");
    println!("{:?}", id);
}
