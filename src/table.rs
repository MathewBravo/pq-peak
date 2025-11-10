use ratatui::{
    DefaultTerminal, Frame,
    crossterm::event::{self, Event, KeyCode, KeyEventKind},
    layout::{Constraint, Direction, Layout},
    style::{Color, Style, Stylize},
    widgets::{Block, Borders, Paragraph, Row, Table, TableState},
};
use tui_textarea::TextArea;

const VISIBLE_COLS: usize = 10;

#[derive(Debug, PartialEq)]
enum Focus {
    Table,
    TextArea,
}

struct App<'a> {
    table_state: TableState,
    rows: Vec<Vec<String>>,
    header: Vec<String>,
    col_offset: usize,
    textarea: TextArea<'a>,
    focus: Focus,
}

impl<'a> App<'a> {
    fn new(header_labels: Vec<String>, rows: Vec<Vec<String>>) -> Self {
        Self {
            table_state: TableState::default().with_selected(0),
            rows,
            header: header_labels,
            col_offset: 0,
            textarea: TextArea::default(),
            focus: Focus::Table,
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
                    KeyCode::Char('q') if key.modifiers.contains(event::KeyModifiers::CONTROL) => {
                        return Ok(());
                    }
                    KeyCode::Tab => {
                        self.focus = match self.focus {
                            Focus::Table => Focus::TextArea,
                            Focus::TextArea => Focus::Table,
                        };
                        continue;
                    }
                    KeyCode::Esc => return Ok(()),
                    _ => {}
                }

                match self.focus {
                    Focus::Table => match key.code {
                        KeyCode::Up => self.table_state.select_previous(),
                        KeyCode::Down => self.table_state.select_next(),
                        KeyCode::Left => self.scroll_left(),
                        KeyCode::Right => self.scroll_right(),
                        _ => {}
                    },
                    Focus::TextArea => {
                        self.textarea.input(key);
                        let lines = self.textarea.lines();
                        let mut semi = false;
                        for line in lines {
                            if line.ends_with(";") {
                                semi = true
                            }
                        }

                        if semi {
                            self.textarea = TextArea::default();
                            let index = self.header.iter().position(|col| col == "order_id");
                            self.rows
                                .retain(|row| row.get(index).map(|val| val = "1").unwrap_or(false));
                        }
                    }
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

        let table_border_style = if self.focus == Focus::Table {
            Style::default().fg(Color::Cyan)
        } else {
            Style::default()
        };

        let textarea_border_style = if self.focus == Focus::TextArea {
            Style::default().fg(Color::Cyan)
        } else {
            Style::default()
        };

        let table = Table::new(visible_rows, widths)
            .header(hdr)
            .block(
                Block::new()
                    .title(title)
                    .borders(Borders::ALL)
                    .border_style(table_border_style),
            )
            .row_highlight_style(Style::new().underlined());

        self.textarea.set_block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(textarea_border_style)
                .title("SQL Editor"),
        );

        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints(vec![
                Constraint::Percentage(60),
                Constraint::Percentage(20),
                Constraint::Percentage(20),
            ])
            .split(area);

        f.render_stateful_widget(table, layout[0], &mut self.table_state);
        f.render_widget(&self.textarea, layout[1]);
        let lines = self.textarea.lines();
        let output = Paragraph::new(lines.join("\n"))
            .block(Block::default().borders(Borders::ALL).title("OUTPUT"));

        f.render_widget(output, layout[2]);
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
