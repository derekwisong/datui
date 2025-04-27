use ratatui::{buffer::Buffer, layout::Rect, widgets::{Paragraph, Widget}};


pub struct DebugState {
    pub num_events: usize,
    pub num_frames: usize,
    pub num_key_events: usize,
    pub last_key_event_name: String,
    pub last_type_name: String,
    pub enabled: bool,

}

impl DebugState {
    pub fn on_key(&mut self, event: &crossterm::event::KeyEvent) {
        self.num_key_events += 1;
        self.last_key_event_name = format!("{:?}", event.code);
        self.last_type_name = format!("{:?}", event.kind);

    }
}

impl Default for DebugState {
    fn default() -> Self {
        Self { 
            num_events: 0,
            num_frames: 0,
            num_key_events: 0,
            last_key_event_name: String::new(),
            last_type_name: String::new(),
            enabled: false,
         }
    }
}

impl Widget for &DebugState {
    fn render(self, area: Rect, buf: &mut Buffer) {
        Paragraph::new(format!(
            "num_events={} num_key_events={} last_key_name={} last_type={} num_frames={}",
            self.num_events,
            self.num_key_events,
            self.last_key_event_name,
            self.last_type_name,
            self.num_frames
        )).render(area, buf);
    }
}