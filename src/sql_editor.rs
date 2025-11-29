use std::{fs::File, path::PathBuf};

use arrow::array::RecordBatch;
use datafusion::prelude::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use ratatui::{
    DefaultTerminal, Frame,
    crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind},
    layout::{Constraint, Layout},
    style::{Color, Style, Stylize},
    widgets::{Block, Borders, Paragraph, Row, Table, TableState, Wrap},
};
use tui_textarea::TextArea;

use crate::{errors::PeakError, peak::batch_to_rows, utils::validate_extension};

const VISIBLE_COLS: usize = 10;
const MAX_PREVIEW_ROWS: usize = 1000;
const DEFAULT_SQL: &str = "SELECT * FROM data LIMIT 100";

enum FocusedPane {
    SqlEditor,
    TablePreview,
    SaveDialog,
}

enum ExecutionState {
    Idle,
    Executing,
    Success,
    Error(String),
}

struct App<'a> {
    file_path: PathBuf,
    batch_size: usize,

    sql_textarea: TextArea<'a>,
    save_dialog: TextArea<'a>,
    focused_pane: FocusedPane,
    execution_state: ExecutionState,
    show_save_dialog: bool,

    table_state: TableState,
    current_batch_idx: usize,
    current_rows: Vec<Vec<String>>,
    header: Vec<String>,
    col_offset: usize,
    total_batches: usize,
    total_rows: usize,

    is_filtered: bool,
}

impl<'a> App<'a> {
    fn new(file_path: PathBuf, batch_size: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::open(&file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(batch_size);

        let metadata = builder.metadata();
        let total_rows = metadata.file_metadata().num_rows() as usize;
        let total_batches = (total_rows + batch_size - 1) / batch_size;

        let arrow_schema = builder.schema();
        let header: Vec<String> = arrow_schema
            .fields()
            .iter()
            .map(|f| f.name().to_owned())
            .collect();

        let mut reader = builder.build()?;
        let first_batch = reader.next().ok_or("No data in file")??;
        let current_rows = batch_to_rows(&first_batch);

        let mut sql_textarea = TextArea::default();
        sql_textarea.set_block(Block::default().borders(Borders::ALL).title("SQL Editor"));
        sql_textarea.insert_str(DEFAULT_SQL);

        let mut save_dialog = TextArea::default();
        save_dialog.set_block(
            Block::default()
                .borders(Borders::ALL)
                .title("Save As (Enter to save, Esc to cancel)")
                .border_style(Style::default().fg(Color::Green)),
        );
        save_dialog.insert_str("output.parquet");

        Ok(Self {
            file_path,
            batch_size,
            sql_textarea,
            save_dialog,
            focused_pane: FocusedPane::SqlEditor,
            execution_state: ExecutionState::Idle,
            show_save_dialog: false,
            table_state: TableState::default().with_selected(0),
            current_batch_idx: 0,
            current_rows,
            header,
            col_offset: 0,
            total_batches,
            total_rows,
            is_filtered: false,
        })
    }

    async fn execute_sql(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let sql = self.sql_textarea.lines().join(" ").trim().to_string();

        if sql.is_empty() {
            self.execution_state = ExecutionState::Error("SQL query is empty".to_string());
            return Ok(());
        }

        let config = SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(self.batch_size);
        let ctx = SessionContext::new_with_config(config);

        ctx.register_parquet(
            "data",
            self.file_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?;

        let sql_with_limit = if !sql.to_uppercase().contains("LIMIT")
            && sql.to_uppercase().trim_start().starts_with("SELECT")
        {
            format!("{} LIMIT {}", sql, MAX_PREVIEW_ROWS)
        } else {
            sql.clone()
        };

        match ctx.sql(&sql_with_limit).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => {
                    if batches.is_empty() {
                        self.execution_state =
                            ExecutionState::Error("Query returned no results".to_string());
                        self.load_original_data()?;
                    } else {
                        self.update_with_results(batches)?;
                        self.execution_state = ExecutionState::Success;
                    }
                }
                Err(e) => {
                    self.execution_state = ExecutionState::Error(format!("Execution: {}", e));
                }
            },
            Err(e) => {
                self.execution_state = ExecutionState::Error(format!("SQL: {}", e));
            }
        }

        Ok(())
    }

    fn update_with_results(
        &mut self,
        batches: Vec<RecordBatch>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if batches.is_empty() {
            return Ok(());
        }

        let new_header: Vec<String> = batches[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_owned())
            .collect();

        let mut all_rows = Vec::new();
        for batch in &batches {
            all_rows.extend(batch_to_rows(batch));
        }

        self.header = new_header;
        self.current_rows = all_rows;
        self.current_batch_idx = 0;
        self.total_rows = self.current_rows.len();
        self.total_batches = 1;
        self.col_offset = 0;
        self.table_state.select(Some(0));
        self.is_filtered = true;

        Ok(())
    }

    fn load_original_data(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::open(&self.file_path)?;
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(self.batch_size);

        let metadata = builder.metadata();
        let total_rows = metadata.file_metadata().num_rows() as usize;
        let total_batches = (total_rows + self.batch_size - 1) / self.batch_size;

        let arrow_schema = builder.schema();
        let header: Vec<String> = arrow_schema
            .fields()
            .iter()
            .map(|f| f.name().to_owned())
            .collect();

        let mut reader = builder.build()?;
        let first_batch = reader.next().ok_or("No data in file")??;
        let current_rows = batch_to_rows(&first_batch);

        self.header = header;
        self.current_rows = current_rows;
        self.current_batch_idx = 0;
        self.total_rows = total_rows;
        self.total_batches = total_batches;
        self.col_offset = 0;
        self.table_state.select(Some(0));
        self.is_filtered = false;
        self.execution_state = ExecutionState::Idle;

        self.sql_textarea = TextArea::default();
        self.sql_textarea
            .set_block(Block::default().borders(Borders::ALL).title("SQL Editor"));
        self.sql_textarea.insert_str(DEFAULT_SQL);

        Ok(())
    }

    fn load_batch(&mut self, batch_idx: usize) -> Result<(), Box<dyn std::error::Error>> {
        if self.is_filtered {
            return Ok(());
        }

        let file = File::open(&self.file_path)?;
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(self.batch_size);

        let reader = builder.build()?;
        let mut skipped_reader = reader.skip(batch_idx);

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
                self.execution_state = ExecutionState::Error(format!("Error loading batch: {}", e));
            }
        }
    }

    fn load_previous_batch(&mut self) {
        if self.current_batch_idx > 0 {
            if let Err(e) = self.load_batch(self.current_batch_idx - 1) {
                self.execution_state = ExecutionState::Error(format!("Error loading batch: {}", e));
            }
        }
    }

    fn toggle_focus(&mut self) {
        if !self.show_save_dialog {
            self.focused_pane = match self.focused_pane {
                FocusedPane::SqlEditor => FocusedPane::TablePreview,
                FocusedPane::TablePreview => FocusedPane::SqlEditor,
                FocusedPane::SaveDialog => FocusedPane::SqlEditor,
            };
        }
    }

    fn save_results(&self, output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        use arrow::datatypes::Schema;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;

        if !self.is_filtered {
            return Err("No SQL results to save. Execute a query first.".into());
        }

        let schema = Schema::new(
            self.header
                .iter()
                .enumerate()
                .map(|(_i, name)| {
                    arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Utf8, true)
                })
                .collect::<Vec<_>>(),
        );

        let columns: Vec<Arc<dyn arrow::array::Array>> = (0..self.header.len())
            .map(|col_idx| {
                let string_array: arrow::array::StringArray = self
                    .current_rows
                    .iter()
                    .map(|row| Some(row[col_idx].as_str()))
                    .collect();
                Arc::new(string_array) as Arc<dyn arrow::array::Array>
            })
            .collect();

        let batch = RecordBatch::try_new(Arc::new(schema), columns)?;

        let file = File::create(output_path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        Ok(())
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

    fn handle_key_event(&mut self, key: KeyEvent) -> Option<Action> {
        if key.code == KeyCode::Esc
            || (key.code == KeyCode::Char('q')
                && key.modifiers.contains(event::KeyModifiers::CONTROL))
        {
            return Some(Action::Quit);
        }

        if key.code == KeyCode::F(2) {
            self.toggle_focus();
            return None;
        }

        if key.code == KeyCode::Char('e') && key.modifiers.contains(event::KeyModifiers::CONTROL) {
            return Some(Action::ExecuteSql);
        }

        if key.code == KeyCode::Char('r') && key.modifiers.contains(event::KeyModifiers::CONTROL) {
            if let Err(e) = self.load_original_data() {
                self.execution_state = ExecutionState::Error(format!("Error resetting: {}", e));
            }
            return None;
        }

        if key.code == KeyCode::Char('s') && key.modifiers.contains(event::KeyModifiers::CONTROL) {
            if self.is_filtered {
                self.show_save_dialog = true;
                self.focused_pane = FocusedPane::SaveDialog;
            } else {
                self.execution_state =
                    ExecutionState::Error("Execute a query first before saving".to_string());
            }
            return None;
        }

        match self.focused_pane {
            FocusedPane::SqlEditor => {
                self.sql_textarea.input(key);
            }
            FocusedPane::TablePreview => match key.code {
                KeyCode::Up => self.table_state.select_previous(),
                KeyCode::Down => self.table_state.select_next(),
                KeyCode::PageDown => self.load_next_batch(),
                KeyCode::PageUp => self.load_previous_batch(),
                KeyCode::Left => self.scroll_left(),
                KeyCode::Right => self.scroll_right(),
                _ => {}
            },
            FocusedPane::SaveDialog => match key.code {
                KeyCode::Enter => {
                    let filename = self.save_dialog.lines().join("");
                    match self.save_results(&filename) {
                        Ok(_) => {
                            self.execution_state = ExecutionState::Success;
                            self.show_save_dialog = false;
                            self.focused_pane = FocusedPane::SqlEditor;
                        }
                        Err(e) => {
                            self.execution_state =
                                ExecutionState::Error(format!("Save error: {}", e));
                            self.show_save_dialog = false;
                            self.focused_pane = FocusedPane::SqlEditor;
                        }
                    }
                }
                KeyCode::Esc => {
                    self.show_save_dialog = false;
                    self.focused_pane = FocusedPane::SqlEditor;
                }
                _ => {
                    self.save_dialog.input(key);
                }
            },
        }

        None
    }

    fn run(mut self, mut terminal: DefaultTerminal) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            terminal.draw(|f| self.draw(f))?;

            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }

                match self.handle_key_event(key) {
                    Some(Action::Quit) => return Ok(()),
                    Some(Action::ExecuteSql) => {
                        self.execution_state = ExecutionState::Executing;
                        terminal.draw(|f| self.draw(f))?;

                        let runtime = tokio::runtime::Runtime::new()?;
                        runtime.block_on(async {
                            if let Err(e) = self.execute_sql().await {
                                self.execution_state =
                                    ExecutionState::Error(format!("Error: {}", e));
                            }
                        });
                    }
                    None => {}
                }
            }
        }
    }

    fn draw(&mut self, f: &mut Frame) {
        let area = f.area();

        let chunks = Layout::vertical([
            Constraint::Percentage(25),
            Constraint::Length(3),
            Constraint::Percentage(70),
        ])
        .split(area);

        let sql_block = Block::default()
            .borders(Borders::ALL)
            .border_style(match self.focused_pane {
                FocusedPane::SqlEditor => Style::default().fg(Color::Cyan),
                FocusedPane::TablePreview | FocusedPane::SaveDialog => Style::default(),
            })
            .title("SQL Editor (F2: Switch | Ctrl+E: Execute | Ctrl+R: Reset | Ctrl+S: Save | Esc: Quit)");

        self.sql_textarea.set_block(sql_block);
        f.render_widget(&self.sql_textarea, chunks[0]);

        self.draw_status(f, chunks[1]);
        self.draw_table(f, chunks[2]);

        if self.show_save_dialog {
            self.draw_save_dialog(f, area);
        }
    }

    fn draw_save_dialog(&mut self, f: &mut Frame, area: ratatui::layout::Rect) {
        use ratatui::layout::Rect;
        use ratatui::widgets::Clear;

        let popup_area = Rect {
            x: area.width / 4,
            y: area.height / 2 - 2,
            width: area.width / 2,
            height: 5,
        };

        f.render_widget(Clear, popup_area);
        f.render_widget(&self.save_dialog, popup_area);
    }

    fn draw_status(&self, f: &mut Frame, area: ratatui::layout::Rect) {
        let (status_text, status_style) = match &self.execution_state {
            ExecutionState::Idle => {
                if self.is_filtered {
                    (
                        "✓ Showing SQL query results".to_string(),
                        Style::default().fg(Color::Green),
                    )
                } else {
                    (
                        "Ready (Ctrl+E to execute SQL)".to_string(),
                        Style::default().fg(Color::Yellow),
                    )
                }
            }
            ExecutionState::Executing => (
                "⏳ Executing SQL query... Please wait".to_string(),
                Style::default().fg(Color::Magenta).bold(),
            ),
            ExecutionState::Success => (
                "✓ Query executed successfully".to_string(),
                Style::default().fg(Color::Green),
            ),
            ExecutionState::Error(error) => {
                (format!("❌ {}", error), Style::default().fg(Color::Red))
            }
        };

        let status = Paragraph::new(status_text)
            .block(Block::default().borders(Borders::ALL).title("Status"))
            .style(status_style)
            .wrap(Wrap { trim: true });

        f.render_widget(status, area);
    }

    fn draw_table(&mut self, f: &mut Frame, area: ratatui::layout::Rect) {
        if self.current_rows.is_empty() {
            let empty_msg = Paragraph::new("No data to display")
                .block(Block::default().borders(Borders::ALL).title("Preview"))
                .style(Style::default().fg(Color::Gray));
            f.render_widget(empty_msg, area);
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

        let widths = std::iter::repeat_n(12u16, end - start);

        let data_source = if self.is_filtered {
            "SQL Results"
        } else {
            "Original Data"
        };

        let title = if self.is_filtered {
            let limit_note = if self.total_rows >= MAX_PREVIEW_ROWS {
                format!(" (limited to {} for preview)", MAX_PREVIEW_ROWS)
            } else {
                String::new()
            };

            format!(
                "{} | Cols {}–{}/{} | {} rows{} | [←/→: Cols | ↑/↓: Rows]",
                data_source,
                start,
                end.saturating_sub(1),
                tc,
                self.total_rows,
                limit_note,
            )
        } else {
            let batch_start_row = self.current_batch_idx * self.batch_size;
            let current_batch_rows = self.current_rows.len();
            let batch_end_row = batch_start_row + current_batch_rows - 1;

            format!(
                "{} | Cols {}–{}/{} | Rows {}–{}/{} | Batch {}/{} | [PgUp/PgDn: Batches | ←/→: Cols | ↑/↓: Rows]",
                data_source,
                start,
                end.saturating_sub(1),
                tc,
                batch_start_row,
                batch_end_row,
                self.total_rows,
                self.current_batch_idx + 1,
                self.total_batches,
            )
        };

        let table_block = Block::default()
            .borders(Borders::ALL)
            .border_style(match self.focused_pane {
                FocusedPane::TablePreview => Style::default().fg(Color::Cyan),
                FocusedPane::SqlEditor | FocusedPane::SaveDialog => Style::default(),
            })
            .title(title);

        let table = Table::new(visible_rows, widths)
            .header(hdr)
            .block(table_block)
            .row_highlight_style(Style::new().underlined());

        f.render_stateful_widget(table, area, &mut self.table_state);
    }
}

enum Action {
    Quit,
    ExecuteSql,
}

pub fn edit(file_path: &PathBuf, batch_size: usize) -> Result<(), Box<dyn std::error::Error>> {
    let valid = validate_extension(file_path);
    if !valid {
        eprintln!("ERROR: {}", PeakError::UnsupportedFileType);
        std::process::exit(0);
    }

    color_eyre::install()?;

    let terminal = ratatui::init();
    let app_result = App::new(file_path.clone(), batch_size)?.run(terminal);
    ratatui::restore();

    app_result
}
