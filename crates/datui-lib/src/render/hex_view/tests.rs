use super::*;
use crate::app::hex_view::{HexSource, Origin};
use crate::formats::fixed_records::Bytes;
use std::path::PathBuf;
use std::sync::Arc;

fn view(bytes: Vec<u8>) -> HexView {
    HexView::new(
        HexSource {
            path: PathBuf::from("sample.bin"),
            bytes: Arc::new(Bytes::Owned(bytes)),
        },
        Origin::Launch,
        false,
        1,
    )
}

fn screen(view: &mut HexView, width: u16, height: u16) -> Vec<String> {
    let ctx = RenderContext::for_test();
    let area = Rect::new(0, 0, width, height);
    let mut buf = Buffer::empty(area);
    draw(area, &mut buf, view, &ctx, false);
    (0..height)
        .map(|y| {
            (0..width)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect::<String>()
                .trim_end()
                .to_string()
        })
        .collect()
}

fn sample() -> Vec<u8> {
    let mut bytes = b"PAR1 hello, world\n\0\0\x01\x02\xff\x80".to_vec();
    bytes.extend((0..=255u8).cycle().take(4000));
    bytes
}

#[test]
fn sixty_by_twenty_shows_eight_bytes_a_row_and_the_gutter() {
    let mut v = view(sample());
    let s = screen(&mut v, 60, 20);
    assert!(s[0].starts_with("Hex"), "{s:?}");
    assert!(s[0].contains("8 bytes/row"), "{s:?}");
    assert!(s[1].starts_with("offset"), "{s:?}");
    assert_eq!(
        s[2], "00000000  50 41 52 31  20 68 65 6c  PAR1 hel",
        "{s:?}"
    );
    assert!(
        s[3].starts_with("00000008  6c 6f 2c 20  77 6f 72 6c"),
        "{s:?}"
    );
    assert!(s[19].starts_with("0x0 of 0x"), "{s:?}");
    assert!(
        !s.iter().any(|l| l.contains("LE")),
        "no room for the panel: {s:?}"
    );
}

#[test]
fn eighty_by_twenty_four_shows_sixteen() {
    let mut v = view(sample());
    let s = screen(&mut v, 80, 24);
    let g = crate::glyphs::get();
    assert!(s[0].contains("16 bytes/row"), "{s:?}");
    assert_eq!(
        s[3],
        format!(
            "00000010  64 0a 00 00  01 02 ff 80   00 01 02 03  04 05 06 07  d{}",
            g.hex_dot.repeat(15)
        )
    );
    assert!(s[1].contains("00 01 02 03  04 05 06 07   08"), "{s:?}");
}

/// At 80×24 the inspector under the bytes takes at most half the rows: the
/// bytes keep the rest, and the readings that do not fit are counted.
#[test]
fn the_inspector_under_the_bytes_counts_what_does_not_fit() {
    let mut v = view((0..=255u8).cycle().take(4096).collect());
    v.inspector_open = true;
    let s = screen(&mut v, 80, 24);
    let rows = s.iter().filter(|l| l.starts_with("00000")).count();
    assert!(rows >= 8, "the bytes keep half: {s:#?}");
    let more = format!("{} ", crate::glyphs::get().ellipsis);
    assert!(
        s.iter()
            .any(|l| l.starts_with(&more) && l.contains(" more")),
        "{s:#?}"
    );
}

#[test]
fn wide_screens_carry_the_inspector_beside_the_bytes() {
    let mut v = view(sample());
    let s = screen(&mut v, 140, 40);
    assert!(s[0].contains("16 bytes/row"), "{s:?}");
    assert!(s[1].contains("At 0x0"), "{s:?}");
    assert!(
        s.iter()
            .any(|l| l.contains("u32") && l.contains("827474256")),
        "{s:?}"
    );
    let s = screen(&mut v, 250, 60);
    assert!(s[0].contains("32 bytes/row"), "{s:?}");
    assert!(
        s.iter()
            .any(|l| l.contains("text") && l.contains("PAR1 hello, world")),
        "{s:?}"
    );
    v.inspector = false;
    let s = screen(&mut v, 320, 60);
    assert!(s[0].contains("64 bytes/row"), "{s:?}");
}

#[test]
fn under_fifty_columns_the_gutter_goes() {
    let mut v = view(sample());
    let s = screen(&mut v, 44, 12);
    assert_eq!(s[2], "00000000  50 41 52 31  20 68 65 6c", "{s:?}");
}

#[test]
fn an_empty_file_and_a_one_byte_file_draw() {
    let mut v = view(Vec::new());
    let s = screen(&mut v, 80, 24);
    assert_eq!(s[2], "Empty file");
    let mut v = view(vec![0x41]);
    let s = screen(&mut v, 80, 24);
    assert!(
        s[2].starts_with("00000000  41") && s[2].ends_with("A"),
        "{s:?}"
    );
    assert!(s[23].contains("100.0%"), "{s:?}");
}

#[test]
fn a_wide_record_shows_the_cursor_s_part_of_it() {
    let mut v = view(sample());
    v.record_size = Some(100);
    v.go(99);
    let s = screen(&mut v, 80, 24);
    assert!(s[0].contains("100 bytes/row (fixed)"), "{s:?}");
    assert!(s[0].contains("to 99 shown"), "{s:?}");
}

#[test]
fn the_prompt_and_its_error_sit_at_the_foot() {
    let mut v = view(sample());
    v.prompt = Some(PromptKind::GoTo);
    v.input.set_value("0xzz");
    v.prompt_error = Some("0xzz is not an offset".to_string());
    let s = screen(&mut v, 80, 24);
    assert!(s.iter().any(|l| l.contains("Go to Offset")), "{s:?}");
    assert!(
        s.iter().any(|l| l.contains("0xzz is not an offset")),
        "{s:?}"
    );
    assert!(s[23].starts_with("0x0 of"), "{s:?}");
}

#[test]
fn a_fallback_says_no_spec_matched_and_names_the_key() {
    let mut v = view(sample());
    v.fallback = true;
    let ctx = RenderContext::for_test();
    let area = Rect::new(0, 0, 120, 10);
    let mut buf = Buffer::empty(area);
    draw(area, &mut buf, &mut v, &ctx, true);
    let last: String = (0..120).map(|x| buf[(x, 9)].symbol().to_string()).collect();
    assert!(last.contains("B reads it with a spec"), "{last}");
}
/// The screen at each size the issue names, against the text in `snapshots/`,
/// drawn with the Unicode glyphs; the active set's glyphs stand in for them.
#[test]
fn screens_match_their_snapshots() {
    let (u, g) = (crate::glyphs::unicode(), crate::glyphs::get());
    for (width, height, expected) in [
        (60, 20, include_str!("../snapshots/hex_view_60x20.txt")),
        (80, 24, include_str!("../snapshots/hex_view_80x24.txt")),
        (140, 40, include_str!("../snapshots/hex_view_140x40.txt")),
        (250, 60, include_str!("../snapshots/hex_view_250x60.txt")),
    ] {
        let mut v = view(sample());
        v.go(5);
        let shown = screen(&mut v, width, height);
        let last = expected.lines().count() - 1;
        // The header and status lines separate with a middot; the gutter's dots
        // are the hex view's own glyph.
        let expected: Vec<String> = expected
            .lines()
            .enumerate()
            .map(|(i, line)| {
                let dot = if i == 0 || i == last {
                    g.middot
                } else {
                    g.hex_dot
                };
                line.replace(u.hex_dot, dot)
                    .replace(u.rule_h, g.rule_h)
                    .replace(u.rule, g.rule)
            })
            .collect();
        assert_eq!(shown, expected, "{width}x{height}");
    }
}
