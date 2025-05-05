use color_eyre::Result;
use std::{fs::File, path::Path};

use polars::prelude::*;
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{
        Block, Borders, Cell, Padding, Paragraph, Row, StatefulWidget, Table, TableState, Widget,
    },
};

use crate::OpenOptions;

pub struct DataTableState {
    pub lf: LazyFrame,
    df: Option<DataFrame>,
    pub table_state: TableState,
    pub start_row: usize,
    pub visible_rows: usize,
    pub termcol_index: usize,
    pub visible_termcols: usize,
    error: Option<PolarsError>,
    pub schema: Arc<Schema>,
    pub num_rows: usize,
}

impl DataTableState {
    pub fn new(lf: LazyFrame) -> Result<Self> {
        let schema = lf.clone().collect_schema()?;
        Ok(Self {
            lf,
            df: None,
            table_state: TableState::default(),
            start_row: 0,
            visible_rows: 0,
            termcol_index: 0,
            visible_termcols: 0,
            error: None,
            schema,
            num_rows: 0,
        })
    }

    pub fn from_parquet(path: &Path) -> Result<Self> {
        let lf = LazyFrame::scan_parquet(path, Default::default())?;
        Self::new(lf)
    }

    pub fn from_csv(path: &Path, options: &OpenOptions) -> Result<Self> {
        Self::from_csv_customize(path, |mut reader| {
            if let Some(skip_lines) = options.skip_lines {
                reader = reader.with_skip_lines(skip_lines);
            }
            if let Some(skip_rows) = options.skip_rows {
                reader = reader.with_skip_rows(skip_rows);
            }
            if let Some(has_header) = options.has_header {
                reader = reader.with_has_header(has_header);
            }
            reader
        })
    }

    // takes a function that consumes a LazyCsvReader and returns a LazyCsvReader
    // this allows for customization of the csv reader
    pub fn from_csv_customize<F>(path: &Path, func: F) -> Result<Self>
    where
        F: FnOnce(LazyCsvReader) -> LazyCsvReader,
    {
        let reader = LazyCsvReader::new(path);
        let lf = func(reader).finish()?;
        Self::new(lf)
    }

    pub fn from_ndjson(path: &Path) -> Result<Self> {
        let lf = LazyJsonLineReader::new(path).finish()?;
        Self::new(lf)
    }

    pub fn from_json(path: &Path) -> Result<Self> {
        Self::from_json_with_format(path, JsonFormat::Json)
    }

    pub fn from_json_lines(path: &Path) -> Result<Self> {
        Self::from_json_with_format(path, JsonFormat::JsonLines)
    }

    fn from_json_with_format(path: &Path, format: JsonFormat) -> Result<Self> {
        let file = File::open(path)?;
        let lf = JsonReader::new(file)
            .with_json_format(format)
            .finish()?
            .lazy();
        Self::new(lf)
    }

    pub fn from_delimited(path: &Path, delimiter: u8) -> Result<Self> {
        let reader = LazyCsvReader::new(path).with_separator(delimiter);
        let lf = reader.finish()?;
        Self::new(lf)
    }

    fn slide_table(&mut self, rows: i64) {
        self.start_row = if self.start_row as i64 + rows <= 0 {
            0
        } else {
            // if currently a df is showing and it doesnt fill the screen
            // then we can't side down
            if let Some(df) = self.df.as_ref() {
                if rows > 0 && df.shape().0 <= self.visible_rows {
                    return;
                }
            }
            (self.start_row as i64 + rows) as usize
        };

        self.collect();
    }

    pub fn collect(&mut self) {
        self.num_rows = match self.lf.clone().select([len()]).collect() {
            Ok(df) => {
                if let Some(col) = df.get(0) {
                    if let Some(AnyValue::UInt32(len)) = col.get(0) {
                        *len as usize
                    } else {
                        0
                    }
                } else {
                    0
                }
            }
            Err(_) => 0,
        };
        match self
            .lf
            .clone()
            .select(
                self.schema
                    .iter_names()
                    .skip(self.termcol_index)
                    .map(|name| col(name.as_str()))
                    .collect::<Vec<_>>(),
            )
            .slice(self.start_row as i64, self.visible_rows as u32 + 1)
            .collect()
        {
            Ok(df) => {
                self.df = Some(df);
                self.error = None;
            }
            Err(e) => self.error = Some(e),
        }
    }

    pub fn select_next(&mut self) {
        self.table_state.select_next();
        if let Some(selected) = self.table_state.selected() {
            if selected >= self.visible_rows && self.visible_rows > 0 {
                self.slide_table(1);
            }
        }
    }

    pub fn page_down(&mut self) {
        self.slide_table(self.visible_rows as i64);
    }

    pub fn select_previous(&mut self) {
        if let Some(selected) = self.table_state.selected() {
            self.table_state.select_previous();
            if selected == 0 && self.start_row > 0 {
                self.slide_table(-1);
            }
        } else {
            self.table_state.select(Some(0));
        }
    }

    pub fn scroll_to(&mut self, index: usize) {
        self.start_row = index;
        self.collect();
    }

    pub fn page_up(&mut self) {
        self.slide_table(-(self.visible_rows as i64));
    }

    pub fn scroll_right(&mut self) {
        if self.termcol_index < self.schema.len() - 1 {
            self.termcol_index += 1;
            self.collect();
        }
    }

    pub fn scroll_left(&mut self) {
        if self.termcol_index > 0 {
            self.termcol_index -= 1;
            self.collect();
        }
    }
}
pub struct DataTable {}

impl DataTable {
    pub fn new() -> Self {
        Self {}
    }

    fn render_dataframe(
        &self,
        df: &DataFrame,
        area: Rect,
        buf: &mut Buffer,
        state: &mut TableState,
    ) {
        // make each column as wide as it needs to be to fit the content
        let (height, cols) = df.shape();

        // widths starts at the length of each column naame
        let mut widths: Vec<u16> = df
            .get_column_names()
            .iter()
            .map(|name| name.chars().count() as u16)
            .collect();

        let mut used_width = 0;

        // rows is a vector initialized to a vector of lenth "height" empty rows
        let mut rows: Vec<Vec<Cell>> = vec![vec![]; height as usize];
        let mut visible_columns = 0;

        for col_index in 0..cols {
            let mut max_len = widths[col_index];
            let col_data = &df[col_index];

            for row_index in 0..height.min(if area.height > 1 {
                area.height as usize - 1
            } else {
                0
            }) {
                let value = col_data.get(row_index as usize).unwrap();
                let val_str = value.str_value();
                let len = val_str.chars().count() as u16;
                max_len = max_len.max(len);
                rows[row_index as usize].push(Cell::from(Line::from(val_str)));
            }

            let overflows = (used_width + max_len) >= area.width;

            if overflows && col_data.dtype() == &DataType::String {
                let visible_width = area.width - used_width;
                visible_columns += 1;
                widths[col_index] = visible_width;
                break;
            } else if !overflows {
                visible_columns += 1;
                widths[col_index] = max_len;
                used_width += max_len + 1;
            } else {
                break;
            }
        }

        widths.truncate(visible_columns);
        // convert rows to a vector of Row
        let rows = rows
            .into_iter()
            .map(|mut row| {
                row.truncate(visible_columns);
                Row::new(row)
            })
            .collect::<Vec<Row>>();

        // for visible columsn
        let headers: Vec<Span> = df
            .get_column_names()
            .iter()
            .take(visible_columns)
            .map(|name| {
                Span::styled(
                    name.to_string(),
                    Style::default().add_modifier(Modifier::BOLD),
                )
            })
            .collect();

        StatefulWidget::render(
            Table::new(rows, widths)
                .column_spacing(1)
                .header(Row::new(headers).bold().underlined())
                .row_highlight_style(Style::new().bg(Color::Blue)),
            area,
            buf,
            state,
        );
    }
}

impl StatefulWidget for DataTable {
    type State = DataTableState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        state.visible_termcols = area.width as usize;
        state.visible_rows = if area.height > 0 {
            (area.height - 1) as usize
        } else {
            0
        };

        if let Some(selected) = state.table_state.selected() {
            if selected >= state.visible_rows as usize {
                state
                    .table_state
                    .select(Some(state.visible_rows as usize - 1))
            }
        }

        if let Some(df) = state.df.as_ref() {
            self.render_dataframe(df, area, buf, &mut state.table_state);
        } else if let Some(error) = state.error.as_ref() {
            Paragraph::new(format!("Error: {}", error))
                .centered()
                .block(
                    Block::default()
                        .borders(Borders::NONE)
                        .padding(Padding::top(area.height / 2)),
                )
                .wrap(ratatui::widgets::Wrap { trim: true })
                .render(area, buf);
        } else {
            Paragraph::new("No data").render(area, buf);
        }
    }
}
