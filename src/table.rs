use ratatui::{
    DefaultTerminal, Frame,
    crossterm::event::{self, Event, KeyCode, KeyEventKind},
    style::{Style, Stylize},
    widgets::{Block, Borders, Row, Table, TableState},
};

const VISIBLE_COLS: usize = 10;

struct App {
    table_state: TableState,
    rows: Vec<Vec<String>>,
    header: Vec<String>,
    col_offset: usize,
}

impl App {
    fn new(header_labels: Vec<String>, rows: Vec<Vec<String>>) -> Self {
        Self {
            table_state: TableState::default().with_selected(0),
            rows,
            header: header_labels,
            col_offset: 0,
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
                    KeyCode::PageDown => self.table_state.select_last(),
                    KeyCode::PageUp => self.table_state.select_first(),
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
        let total_cols = self.rows[0].len();
        if self.col_offset + VISIBLE_COLS < total_cols {
            self.col_offset += 1;
        }
    }

    fn draw(&mut self, f: &mut Frame) {
        let area = f.area();

        let tc = self.rows[0].len();
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

        let visible_rows = self.rows.iter().map(|r| {
            let slice = &r[start..end];
            Row::new(slice.iter().map(String::as_str).collect::<Vec<_>>())
        });

        // Fixed width columns so the table doesn't squish everything
        let widths = std::iter::repeat_n(12u16, end - start);

        let title = format!(
            "Table (Tab to switch) cols {}–{} of {}",
            start,
            end.saturating_sub(1),
            tc
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
    header: Vec<String>,
    rows: Vec<Vec<String>>,
) -> Result<(), Box<dyn std::error::Error>> {
    color_eyre::install()?;

    let terminal = ratatui::init();
    let app_result = App::new(header, rows).run(terminal);
    ratatui::restore();

    app_result
}
