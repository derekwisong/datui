
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    widgets::{
        Paragraph, Widget,
    },
};


pub struct Controls<'a> {
    help: &'a str,
}

impl<'a> Controls<'a> {
    pub fn new(help: &'a str) -> Self {
        Self { help }
    }
}

impl<'a> Widget for &'a Controls<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        Paragraph::new(self.help)
            .wrap(ratatui::widgets::Wrap { trim: true })
            .render(area, buf);
        }
}