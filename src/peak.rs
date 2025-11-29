use std::process::exit;

use arrow::{array::RecordBatch, util::display::array_value_to_string};

use crate::{errors::PeakError, table::build_table, utils::validate_extension};

pub fn peak(path: &std::path::PathBuf, batch_size: usize) -> Result<(), Box<dyn std::error::Error>> {
    let valid = validate_extension(path);
    if !valid {
        eprintln!("ERROR: {}", PeakError::UnsupportedFileType);
        exit(0)
    }
    
    build_table(path.clone(), batch_size)?;

    Ok(())
}

pub fn batch_to_rows(batch: &RecordBatch) -> Vec<Vec<String>> {
    let batch_length = batch.num_rows();
    let mut rows: Vec<Vec<String>> = Vec::new();
    
    for i in 0..batch_length {
        let row_strings: Vec<String> = batch
            .columns()
            .iter()
            .map(|col| array_value_to_string(col, i).unwrap_or_else(|_| "NULL".to_string()))
            .collect();
        rows.push(row_strings);
    }
    
    rows
}
