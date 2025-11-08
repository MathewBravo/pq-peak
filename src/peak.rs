use std::{
    fs,
    io::{self, Error},
    process::exit,
};

use arrow::{
    array::RecordBatch,
    util::{display::array_value_to_string, pretty::pretty_format_batches},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::{errors::PeakError, table::build_table, utils::validate_extension};

pub fn peak(path: &std::path::PathBuf) -> Result<(), Error> {
    let valid = validate_extension(path);
    if !valid {
        eprintln!("ERROR: {}", PeakError::UnsupportedFileType);
        exit(0)
    }
    let file = fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.with_batch_size(20).build()?;

    if let Some(Ok(batch)) = reader.next() {
        if batch.num_columns() > 12 {
            println!("PQ-PEAK is currently working on better parquet printing");
            println!("for now very wide parquet files are previewed differently.");

            peak_large(batch, 20);
        } else {
            println!("Number of rows: {}", batch.num_rows());
            println!("{}", pretty_format_batches(&[batch]).unwrap());
        }
    }

    Ok(())
}

fn peak_large(batch: RecordBatch, batch_length: usize) {
    let mut field_names = Vec::new();
    batch.schema().fields().iter().for_each(|f| {
        field_names.push(f.name().to_owned());
    });

    let mut rows: Vec<Vec<String>> = Vec::new();
    for i in 0..batch_length {
        let row_strings: Vec<String> = batch
            .columns()
            .iter()
            .map(|col| array_value_to_string(col, i).unwrap_or_else(|_| "NULL".to_string()))
            .collect();
        rows.push(row_strings);
    }
    println!("MAKING TABLE!");
    build_table(field_names, rows).unwrap();
}
