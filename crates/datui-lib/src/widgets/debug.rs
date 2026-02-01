use ratatui::{
    buffer::Buffer,
    layout::Rect,
    widgets::{Paragraph, Widget},
};

#[derive(Default)]
pub struct DebugState {
    pub num_events: usize,
    pub num_frames: usize,
    pub num_key_events: usize,
    pub last_key_event_name: String,
    pub last_type_name: String,
    /// Last action taken (e.g. "scroll_left") for debugging key handling.
    pub last_action: String,
    pub enabled: bool,
}

impl DebugState {
    pub fn on_key(&mut self, event: &crossterm::event::KeyEvent) {
        self.num_key_events += 1;
        self.last_key_event_name = format!("{:?}", event.code);
        self.last_type_name = format!("{:?}", event.kind);
    }
}

impl Widget for &DebugState {
    fn render(self, area: Rect, buf: &mut Buffer) {
        Paragraph::new(format!(
            "events={} keys={} last_key={} kind={} last_action={} frames={}",
            self.num_events,
            self.num_key_events,
            self.last_key_event_name,
            self.last_type_name,
            self.last_action,
            self.num_frames
        ))
        .render(area, buf);
    }
}
