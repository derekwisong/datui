use super::*;

/// The lines a frame `height` rows tall shows of four fields, each with a note,
/// focused on `focused`.
fn shown(height: u16, focused: usize) -> String {
    let area = Rect::new(0, 0, 40, height);
    let mut buf = Buffer::empty(area);
    let mut rows = Vec::new();
    for field in 0..4usize {
        rows.push(FormLine::Field(field, "Field:", FormValue::Choice("value")));
        rows.push(FormLine::Note(Line::from(format!("note {field}"))));
    }
    FormView {
        title: "Form",
        screen: datui_cli::keys::Context::Export,
        footer: None,
        label_width: 8,
        rows,
        focused: Some(focused),
        picker: None,
        status: None,
        shields: false,
    }
    .render_unrecorded(area, &mut buf, &RenderContext::for_test());
    crate::tests::buffer_lines(&buf).join("\n")
}

/// Scrolled to the focused field, the frame keeps the note under it in view.
#[test]
fn the_focused_field_s_note_stays_in_view() {
    let out = shown(6, 3);
    assert!(out.contains("note 3"), "{out}");
    assert!(!out.contains("note 0"), "{out}");
    // Room for the field alone: the field, not its note.
    let out = shown(3, 3);
    assert!(out.contains("Field:") && !out.contains("note 3"), "{out}");
}
