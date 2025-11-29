use std::{fs::File, path::PathBuf};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use ratatui::{
    DefaultTerminal, Frame,
    crossterm::event::{self, Event, KeyCode, KeyEventKind},
    style::{Style, Stylize},
    widgets::{Block, Borders, Row, Table, TableState},
};

use crate::peak::batch_to_rows;

const VISIBLE_COLS: usize = 10;

struct App {
    table_state: TableState,
    file_path: PathBuf,
    current_batch_idx: usize,
    current_rows: Vec<Vec<String>>,
    header: Vec<String>,
    col_offset: usize,
    batch_size: usize,
    total_batches: usize,
    total_rows: usize,
}

impl App {
    fn new(file_path: PathBuf, batch_size: usize) -> Result<Self, Box<dyn std::error::Error>> {
        // Open file just to get metadata
        let file = File::open(&file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(batch_size);

        let metadata = builder.metadata();
        let total_rows = metadata.file_metadata().num_rows() as usize;
        let total_batches = (total_rows + batch_size - 1) / batch_size; // ceiling division

        // Get schema/header from metadata
        let arrow_schema = builder.schema();
        let header: Vec<String> = arrow_schema
            .fields()
            .iter()
            .map(|f| f.name().to_owned())
            .collect();

        // Load first batch
        let mut reader = builder.build()?;
        let first_batch = reader.next().ok_or("No data in file")??;
        let current_rows = batch_to_rows(&first_batch);

        Ok(Self {
            table_state: TableState::default().with_selected(0),
            file_path,
            current_batch_idx: 0,
            current_rows,
            header,
            col_offset: 0,
            batch_size,
            total_batches,
            total_rows,
        })
    }

    fn load_batch(&mut self, batch_idx: usize) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::open(&self.file_path)?;
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(self.batch_size);

        let reader = builder.build()?;

        // Use skip() to efficiently jump to the desired batch
        let mut skipped_reader = reader.skip(batch_idx);

        // Read the target batch
        if let Some(batch_result) = skipped_reader.next() {
            let batch = batch_result?;
            self.current_rows = batch_to_rows(&batch);
            self.current_batch_idx = batch_idx;
            self.table_state.select(Some(0));
        }

        Ok(())
    }

    fn load_next_batch(&mut self) {
        if self.current_batch_idx + 1 < self.total_batches {
            if let Err(e) = self.load_batch(self.current_batch_idx + 1) {
                eprintln!("Error loading next batch: {}", e);
            }
        }
    }

    fn load_previous_batch(&mut self) {
        if self.current_batch_idx > 0 {
            if let Err(e) = self.load_batch(self.current_batch_idx - 1) {
                eprintln!("Error loading previous batch: {}", e);
            }
        }
    }

    fn run(mut self, mut terminal: DefaultTerminal) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            terminal.draw(|f| self.draw(f))?;

            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }

                match key.code {
                    KeyCode::Up => self.table_state.select_previous(),
                    KeyCode::Down => self.table_state.select_next(),
                    KeyCode::PageDown => self.load_next_batch(),
                    KeyCode::PageUp => self.load_previous_batch(),
                    KeyCode::Left => self.scroll_left(),
                    KeyCode::Right => self.scroll_right(),
                    KeyCode::Char('q') if key.modifiers.contains(event::KeyModifiers::CONTROL) => {
                        return Ok(());
                    }
                    KeyCode::Esc => return Ok(()),
                    _ => {}
                }
            }
        }
    }

    fn scroll_left(&mut self) {
        if self.col_offset > 0 {
            self.col_offset -= 1;
        }
    }

    fn scroll_right(&mut self) {
        if self.current_rows.is_empty() {
            return;
        }

        let total_cols = self.current_rows[0].len();
        if self.col_offset + VISIBLE_COLS < total_cols {
            self.col_offset += 1;
        }
    }

    fn draw(&mut self, f: &mut Frame) {
        let area = f.area();

        if self.current_rows.is_empty() {
            return;
        }

        let tc = self.current_rows[0].len();
        let start = self.col_offset;
        let end = (start + VISIBLE_COLS).min(tc);

        let hdr = Row::new(
            self.header[start..end]
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>(),
        )
        .bold()
        .height(1);

        let visible_rows = self.current_rows.iter().map(|r| {
            let slice = &r[start..end];
            Row::new(slice.iter().map(String::as_str).collect::<Vec<_>>())
        });

        // Fixed width columns so the table doesn't squish everything
        let widths = std::iter::repeat_n(12u16, end - start);

        let batch_start_row = self.current_batch_idx * self.batch_size;
        let current_batch_rows = self.current_rows.len();
        let batch_end_row = batch_start_row + current_batch_rows - 1;

        let title = format!(
            "Table | Cols {}–{}/{} | Rows {}–{}/{} | Batch {}/{} | [PgUp/PgDn: Batches | ←/→: Cols | ↑/↓: Rows | Esc: Quit]",
            start,
            end.saturating_sub(1),
            tc,
            batch_start_row,
            batch_end_row,
            self.total_rows,
            self.current_batch_idx + 1,
            self.total_batches,
        );

        let table = Table::new(visible_rows, widths)
            .header(hdr)
            .block(
                Block::new()
                    .title(title)
                    .borders(Borders::ALL)
                    .border_style(Style::default()),
            )
            .row_highlight_style(Style::new().underlined());

        f.render_stateful_widget(table, area, &mut self.table_state);
    }
}

pub fn build_table(
    file_path: PathBuf,
    batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    color_eyre::install()?;

    let terminal = ratatui::init();
    let app_result = App::new(file_path, batch_size)?.run(terminal);
    ratatui::restore();

    app_result
}
