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
    /// Snapshot of main help flag at render time (set by App when enabled). Used by the debug overlay (DATUI_DEBUG=1) to verify help state.
    pub show_help_at_render: bool,
    /// Schema load path taken by the open's schema phase (one-file vs full scan); set when loading Parquet.
    pub schema_load: Option<String>,
    /// How long frames and event handlers take.
    pub times: crate::loading::measurements::LoopTimes,
}

impl DebugState {
    /// Name the action a key took, when the overlay is on to show it.
    pub fn action(&mut self, name: impl FnOnce() -> String) {
        if self.enabled {
            self.last_action = name();
        }
    }

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
            "events={} keys={} last_key={} kind={} last_action={} help={} frames={} schema={} frame: {} handler: {}",
            self.num_events,
            self.num_key_events,
            self.last_key_event_name,
            self.last_type_name,
            self.last_action,
            self.show_help_at_render,
            self.num_frames,
            schema,
            self.times.frames.summary(),
            self.times.handlers.summary()
        ))
        .render(area, buf);
    }
}
