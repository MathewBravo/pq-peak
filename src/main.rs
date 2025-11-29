use clap::{Parser, Subcommand};

mod errors;
mod peak;
mod sql_editor;
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
        
        /// Number of rows to load per batch (default: 100)
        #[arg(short, long, default_value_t = 100)]
        batch_size: usize,
    },
    Edit {
        /// File to edit with SQL
        file: std::path::PathBuf,
        
        /// Number of rows to load per batch (default: 100)
        #[arg(short, long, default_value_t = 100)]
        batch_size: usize,
    },
}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Peak { file, batch_size }) => peak::peak(file, *batch_size).unwrap(),
        Some(Commands::Edit { file, batch_size }) => sql_editor::edit(file, *batch_size).unwrap(),
        None => todo!(),
    }
}
