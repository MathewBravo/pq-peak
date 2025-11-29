# pq-peak

A fast terminal-based tool for viewing and querying Parquet files. Built in Rust with a focus on performance and usability.

## What it does

pq-peak lets you inspect and manipulate Parquet files without loading entire datasets into memory. It uses lazy loading and pagination to handle files of any size efficiently. You can browse data with keyboard navigation or use SQL queries to filter, aggregate, and transform your data interactively.

## Installation

Clone the repository and build with Cargo:

```bash
git clone <your-repo-url>
cd pq-peak
cargo build --release
```

The binary will be in `target/release/pq-peak`.

## Usage

There are two main commands: `peak` for viewing data and `edit` for querying with SQL.

### Viewing data (peak command)

The peak command loads data in batches so you can browse large files instantly. By default it loads 100 rows at a time, but you can adjust this.

```bash
pq-peak peak data.parquet

pq-peak peak data.parquet --batch-size 200
```

Navigation controls:
- Up/Down arrows navigate between rows
- PageUp/PageDown switch between batches
- Left/Right arrows scroll through columns

- Esc or Ctrl+Q to quit

The interface shows which batch you're viewing and how many total rows exist in the file. Column scrolling lets you see all fields even in wide tables.


### Querying with SQL (edit command)


The edit command opens a split view with a SQL editor on top and a table preview below. You can write SQL queries to filter, aggregate, or transform the data and see results immediately.


```bash
pq-peak edit data.parquet
pq-peak edit data.parquet --batch-size 150
```

The file is registered as a table named `data` in the SQL context. You can use standard SQL syntax including SELECT, WHERE, GROUP BY, ORDER BY, aggregations, and joins.

Example queries:

```sql
SELECT * FROM data WHERE quantity > 100 LIMIT 50

SELECT product_id, SUM(quantity) as total FROM data GROUP BY product_id
SELECT * FROM data ORDER BY price DESC LIMIT 20
```


Controls:
- F2 switches focus between the SQL editor and table preview
- Ctrl+E executes the current SQL query
- Ctrl+R resets both the data view and SQL query to defaults
- Ctrl+S saves query results to a new Parquet file
- Esc or Ctrl+Q to quit


When you press Ctrl+S, a dialog appears asking for an output filename. Type the name and press Enter to save, or Esc to cancel. The status bar shows whether you're viewing original data or SQL results, and displays any errors that occur during query execution.

## Performance

The tool is designed to be fast regardless of file size. The peak command loads only the current batch into memory, so opening a multi-gigabyte file is instant. The edit command automatically limits SELECT queries to 1000 rows unless you specify otherwise, preventing accidental full table scans.


For best performance with SQL queries, use LIMIT clauses and WHERE conditions that can be pushed down to the Parquet reader. DataFusion handles query optimization but will still need to scan data for complex aggregations.

## Dependencies

The project uses Arrow and Parquet libraries for data handling, DataFusion for SQL execution, and Ratatui for the terminal interface. All dependencies are managed through Cargo and will be installed automatically when you build.

## License

TODO - add your license here
