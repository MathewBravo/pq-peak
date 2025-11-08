pub fn validate_extension(path: &std::path::PathBuf) -> bool {
    if let Some(ext) = path.extension() {
        return ext.eq("parquet") || ext.eq(".pqt");
    }
    false
}
