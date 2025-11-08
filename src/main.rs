use clap::{Parser, Subcommand};

mod errors;
mod peak;
mod table;
mod utils;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Peak {
        /// File to peek at
        file: std::path::PathBuf,
    },
}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Peak { file }) => peak::peak(file).unwrap(),
        None => todo!(),
    }
}
