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
    /// Snapshot of main help flag at render time (set by App when enabled). Used by --debug to verify help state.
    pub show_help_at_render: bool,
    /// Schema load path taken in DoLoadSchemaBlocking (one-file vs full scan); set when loading Parquet.
    pub schema_load: Option<String>,
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
        let schema = self.schema_load.as_deref().unwrap_or("-");
        Paragraph::new(format!(
            "events={} keys={} last_key={} kind={} last_action={} help={} frames={} schema={}",
            self.num_events,
            self.num_key_events,
            self.last_key_event_name,
            self.last_type_name,
            self.last_action,
            self.show_help_at_render,
            self.num_frames,
            schema
        ))
        .render(area, buf);
    }
}
