use super::*;
use polars::prelude::DataType;

#[test]
fn the_picker_lists_the_types_then_the_formats_with_a_preview() {
    let ctx = RenderContext::for_test();
    let area = Rect::new(0, 0, 80, 24);
    let mut modal = RetypeModal::new(
        "when".into(),
        DataType::String,
        None,
        vec!["03/04/2024".into()],
    );
    let mut buf = Buffer::empty(area);
    render_retype(area, &mut buf, &modal, &ctx);
    let shown = crate::tests::buffer_text(&buf);
    assert!(shown.contains("Type of when, read as str"), "{shown}");
    assert!(shown.contains("as read"), "{shown}");
    assert!(shown.contains("datetime"), "{shown}");
    let date = crate::formats::column_types::TYPE_NAMES
        .iter()
        .position(|n| *n == "date")
        .unwrap();
    modal.picker.select_original(date + 1);
    modal.choose();
    let mut buf = Buffer::empty(area);
    render_retype(area, &mut buf, &modal, &ctx);
    let shown = crate::tests::buffer_text(&buf);
    assert!(shown.contains("Date Format"), "{shown}");
    assert!(shown.contains("%d/%m/%Y  03/04/2024"), "{shown}");
    assert!(shown.contains("2024-04-03"), "{shown}");
    for c in "%d %m".chars() {
        modal.picker.type_char(c);
    }
    let mut buf = Buffer::empty(area);
    render_retype(area, &mut buf, &modal, &ctx);
    let shown = crate::tests::buffer_text(&buf);
    assert!(
        shown.contains("%d %m: does not read the first value"),
        "{shown}"
    );
}

#[test]
fn the_combine_form_echoes_what_it_makes() {
    let ctx = RenderContext::for_test();
    let area = Rect::new(0, 0, 80, 24);
    let columns = vec!["Lcl Date".to_string(), "Lcl Time".to_string()];
    let modal = CombineModal::new("Lcl Date".into(), columns.clone(), &columns);
    let mut buf = Buffer::empty(area);
    render_combine(area, &mut buf, &modal, &ctx);
    let shown = crate::tests::buffer_text(&buf);
    assert!(shown.contains("Combine into Datetime"), "{shown}");
    assert!(shown.contains("UTC offset:"), "{shown}");
    assert!(
        shown.contains("datetime = datetime from Lcl Date, Lcl Time"),
        "{shown}"
    );
}
