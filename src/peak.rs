use std::{
    fs::{self, File},
    io::Error,
    process::exit,
};

use arrow::{array::RecordBatch, util::display::array_value_to_string};
use parquet::arrow::arrow_reader::{
    ArrowReaderBuilder, ParquetRecordBatchReaderBuilder, SyncReader,
};

use crate::{errors::PeakError, table::build_table, utils::validate_extension};

pub fn peak(path: &std::path::PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let valid = validate_extension(path);
    if !valid {
        eprintln!("ERROR: {}", PeakError::UnsupportedFileType);
        exit(0)
    }
    let file = fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    peak_all(builder)?;

    Ok(())
}

pub fn peak_batch(
    builder: ArrowReaderBuilder<SyncReader<File>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut reader = builder.build()?;

    if let Some(Ok(batch)) = reader.next() {
        let rows = batch.num_rows();
        peak_table(batch, rows);
    }

    Ok(())
}

pub fn peak_some(batch: usize) {}

fn peak_table(batch: RecordBatch, batch_length: usize) {
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
    build_table(field_names, rows).unwrap();
}
