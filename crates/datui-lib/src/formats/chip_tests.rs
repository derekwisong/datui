use super::*;

/// A spec of one header and one record field, with `matches` as its `match`.
fn spec(matches: &str) -> Spec {
    let text = format!(
        r#"name = "acme.chips"
{matches}
[header]
fields = [
  {{ name = "magic", type = "str", size = 4 }},
  {{ name = "version", type = "u2" }},
  {{ name = "kind", type = "str", size = 1 }},
]

[records]
fields = [{{ name = "x", type = "u1" }}]
"#
    );
    Spec::parse(&text, None).unwrap()
}

fn plain(spec: &Spec) -> Vec<String> {
    spec.match_chips().iter().map(|c| c.plain(true)).collect()
}

#[test]
fn printable_magic_is_text_and_where_drops_the_header_prefix() {
    let s = spec(r#"match = { magic = "MKTD", where = { "header.version" = 1 } }"#);
    let chips = s.match_chips();
    assert_eq!(chips[0].kind, ChipKind::Magic);
    assert_eq!(chips[1].kind, ChipKind::Int);
    assert_eq!(plain(&s), ["magic MKTD", "version 1"]);
}

#[test]
fn unprintable_magic_is_hex_and_a_long_one_is_cut() {
    let s = spec("match = { magic = [127, 69, 76, 70] }");
    assert_eq!(s.match_chips()[0].kind, ChipKind::Hex);
    assert_eq!(plain(&s), ["magic 7f 45 4c 46"]);
    let s = spec("match = { magic = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] }");
    let ellipsis = crate::glyphs::get().ellipsis;
    assert_eq!(
        plain(&s),
        [format!("magic 00 01 02 03 04 05 06 07 {ellipsis}")]
    );
    let s = spec(r#"match = { magic = "ABCDEFGHIJKLMNOPQRST" }"#);
    assert_eq!(plain(&s), [format!("magic ABCDEFGHIJKLMNOP{ellipsis}")]);
}

#[test]
fn a_magic_offset_folds_into_its_chip_when_not_zero() {
    let s = spec(r#"match = { magic = "MKTD", magic_offset = 8 }"#);
    assert_eq!(plain(&s), ["magic MKTD @ 8"]);
    let s = spec(r#"match = { magic = "MKTD", magic_offset = 0 }"#);
    assert_eq!(plain(&s), ["magic MKTD"]);
}

#[test]
fn a_glob_list_is_one_chip_of_alternatives() {
    let s = spec(r#"match = { glob = ["*.bin", "*.dat"] }"#);
    assert_eq!(plain(&s), ["glob *.bin *.dat"]);
    assert_eq!(s.match_chips()[0].kind, ChipKind::Glob);
}

#[test]
fn a_text_value_is_quoted_only_in_plain_text() {
    let s = spec(
        r#"match = { magic = "MKTD", where = { "header.kind" = "A", "header.version" = 2 } }"#,
    );
    let chips = s.match_chips();
    let kind = chips.iter().find(|c| c.name == "kind").unwrap();
    assert_eq!(kind.kind, ChipKind::Text);
    assert_eq!(kind.plain(true), "kind \"A\"");
    assert_eq!(kind.plain(false), "kind A");
    let version = chips.iter().find(|c| c.name == "version").unwrap();
    assert_eq!(version.plain(true), "version 2");
}

#[test]
fn a_spec_with_no_match_is_for_format_only() {
    let s = spec("");
    assert!(s.match_chips().is_empty());
    assert_eq!(match_words(&s), FORMAT_ONLY);
}

/// A glob that names the file stands without the magic, and the magic is asked only
/// of a file no glob names, so the pane and the Notes show the one that chose it.
#[test]
fn the_rule_that_chose_the_spec_keeps_its_chips() {
    let s = spec(r#"match = { glob = "*.bin", magic = "MKTD", where = { "header.version" = 1 } }"#);
    let names = |chips: Vec<MatchChip>| chips.into_iter().map(|c| c.name).collect::<Vec<_>>();
    assert_eq!(names(s.match_chips()), ["magic", "version", "glob"]);
    assert_eq!(
        names(s.match_chips_chosen(Chosen::Glob)),
        ["version", "glob"]
    );
    assert_eq!(
        names(s.match_chips_chosen(Chosen::Magic)),
        ["magic", "version"]
    );
    assert_eq!(
        names(s.match_chips_for(Path::new("x/day.bin"))),
        ["glob"],
        "a listing names it by the glob alone, its header unread"
    );
    assert_eq!(
        names(s.match_chips_for(Path::new("x/day"))),
        ["magic", "version"]
    );
    let middot = crate::glyphs::get().middot;
    assert_eq!(
        chips_plain(&s.match_chips()),
        format!("magic MKTD {middot} version 1 {middot} glob *.bin")
    );
    assert_eq!(
        chosen_words(&s, Chosen::Magic),
        format!("matched by magic MKTD {middot} version 1")
    );
    assert_eq!(chosen_words(&s, Chosen::Named), "chosen by its name");
}

/// `datui formats` lists each spec with its conditions, `no match` without.
#[test]
fn the_listing_shows_each_specs_chips() {
    let mut one = spec(r#"match = { magic = "MKTD", where = { "header.version" = 1 } }"#);
    one.name = "acme.mktd".to_string();
    let two = spec("");
    let listing = Registry::of(vec![one, two]).listing(&[]);
    let middot = crate::glyphs::get().middot;
    assert!(
        listing.contains(&format!("acme.mktd  (magic MKTD {middot} version 1)")),
        "{listing}"
    );
    assert!(listing.contains("acme.chips  (no match)"), "{listing}");

    // `datui formats check` says the same.
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("spec-chips-mktd.toml");
    std::fs::write(
        &file,
        "name = \"acme.mktd\"\nmatch = { magic = \"MKTD\", where = { \"header.version\" = 1 } }\n\
             [header]\nfields = [{ name = \"magic\", type = \"str\", size = 4 }, \
             { name = \"version\", type = \"u2\" }]\n\
             [records]\nfields = [{ name = \"x\", type = \"u1\" }]\n",
    )
    .unwrap();
    let text = check(
        &file.to_string_lossy(),
        None,
        &Registry::default(),
        &crate::OpenOptions::default(),
    )
    .unwrap();
    assert!(
        text.contains(&format!("  matches magic MKTD {middot} version 1\n")),
        "{text}"
    );
}
