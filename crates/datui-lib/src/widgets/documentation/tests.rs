use super::*;

fn noaa() -> Arc<Dataset> {
    Arc::new(
        crate::home::catalog::bundled()
            .datasets
            .into_iter()
            .find(|d| d.id == "noaa")
            .unwrap(),
    )
}

fn page(entry: Arc<Dataset>) -> Documented {
    Documented {
        catalog: Some(("Example datasets".into(), entry)),
        ..Documented::default()
    }
}

fn screen(state: &mut DocState, width: u16, height: u16) -> Vec<String> {
    let ctx = RenderContext::for_test();
    let area = Rect::new(0, 0, width, height);
    let mut buf = Buffer::empty(area);
    render_view(state, area, &mut buf, &ctx);
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

#[test]
fn a_link_is_one_line_cut_never_wrapped() {
    let mut state = DocState::default();
    state.open(page(noaa()), None);
    let rows = screen(&mut state, 50, 40);
    let text = rows.join("\n");
    assert!(text.contains("publisher"), "{text}");
    let docs: Vec<&String> = rows
        .iter()
        .filter(|r| r.contains("documentation"))
        .collect();
    assert_eq!(docs.len(), 1, "{text}");
    assert!(docs[0].contains(glyphs::get().ellipsis), "{text}");
    assert!(!text.contains("readme.txt"), "{text}");
    assert!(text.contains("LINKS"), "{text}");
}

#[test]
fn y_copies_the_whole_link_and_enter_opens_a_legend() {
    let mut state = DocState::default();
    state.open(page(noaa()), None);
    while !matches!(
        state.lines()[state.cursor],
        DocLine::Link("documentation", _)
    ) {
        state.move_cursor(1);
    }
    assert_eq!(
        state.copy_text().as_deref(),
        Some("https://www.ncei.noaa.gov/pub/data/ghcn/daily/readme.txt")
    );
    while !matches!(&state.lines()[state.cursor], DocLine::Column { name, .. } if name == "ELEMENT")
    {
        state.move_cursor(1);
    }
    assert!(state.toggle_legend());
    let text = screen(&mut state, 100, 60).join("\n");
    assert!(text.contains("PRCP"), "{text}");
    state.move_cursor(3);
    assert!(state.toggle_legend(), "closes from inside the legend");
    assert!(!screen(&mut state, 100, 60).join("\n").contains("PRCP"));
    assert!(
        matches!(&state.lines()[state.cursor], DocLine::Column { name, .. } if name == "ELEMENT")
    );
}

const ORDERS: &str = r#"
name = "acme.orders"
description = "Order entry capture"
documentation = "https://example.com/orders.pdf"
match = { glob = "*.ord" }

[records]
framing = "length_prefixed"
size = "len"
type = "kind"
fields = [{ name = "len", type = "u2" }, { name = "kind", type = "str", size = 1 }]

[[variants]]
name = "add"
when = "A"
description = "An order added to the book"
fields = [
  { name = "price", type = "u4", scale = 4, description = "Limit price", unit = "USD" },
  { name = "side", type = "u1", enum = { 1 = "BUY", 2 = "SELL" } },
]

[[variants]]
name = "exec"
when = ["E", "C"]
fields = [{ name = "shares", type = "u4", description = "Shares executed" }]
"#;

fn spec_page(text: &str) -> Documented {
    let spec = crate::formats::Spec::parse(text, None).unwrap();
    Documented::new(None, spec.docs().map(Arc::new), "day.ord".into()).unwrap()
}

#[test]
fn a_spec_read_from_a_file_names_its_file() {
    let file = dirs::home_dir().unwrap().join("specs").join("orders.toml");
    let spec = crate::formats::Spec::parse(ORDERS, Some(&file)).unwrap();
    let doc = Documented::new(None, spec.docs().map(Arc::new), "day.ord".into()).unwrap();
    let page = lines(&doc, &HashSet::new(), None);
    let at = |line: &DocLine| page.iter().position(|l| l == line);
    let name = at(&DocLine::Field("format spec", "acme.orders".into())).unwrap();
    let shown = crate::home::display_path(&file);
    assert!(shown.starts_with('~'), "{shown}");
    assert_eq!(at(&DocLine::Field("spec file", shown)), Some(name + 1));
    // Parsed from text, there is no file to name.
    let parsed = lines(&spec_page(ORDERS), &HashSet::new(), None);
    assert!(
        !parsed
            .iter()
            .any(|l| matches!(l, DocLine::Field("spec file", _)))
    );
}

#[test]
fn a_variant_specs_page_lists_record_types_columns_and_legends() {
    let mut state = DocState::default();
    state.open(spec_page(ORDERS), None);
    let m = glyphs::get().middot;
    let lines = state.lines();
    assert_eq!(lines[0], DocLine::About("Order entry capture".into()));
    assert!(lines.contains(&DocLine::Field("format spec", "acme.orders".into())));
    assert!(lines.contains(&DocLine::Link(
        "documentation",
        "https://example.com/orders.pdf".into()
    )));
    assert!(lines.contains(&DocLine::Section("RECORD TYPES", 2)));
    assert!(lines.contains(&DocLine::RecordType(
        "add".into(),
        format!("kind = \"A\" {m} 4 columns {m} An order added to the book")
    )));
    assert!(lines.contains(&DocLine::RecordType(
        "exec".into(),
        format!("kind in (\"E\", \"C\") {m} 3 columns")
    )));
    assert!(lines.contains(&DocLine::Section("COLUMNS", 3)));
    assert!(lines.contains(&DocLine::Column {
        name: "price".into(),
        about: "Limit price (USD)".into(),
        values: 0,
    }));
    // The enum is the column's legend, opened with Enter.
    while !matches!(&state.lines()[state.cursor], DocLine::Column { name, .. } if name == "side") {
        state.move_cursor(1);
    }
    assert!(state.toggle_legend());
    assert!(
        state
            .lines()
            .contains(&DocLine::Legend("2".into(), "SELL".into()))
    );
    let text = screen(&mut state, 100, 40).join("\n");
    assert!(text.contains("Documentation"), "{text}");
    assert!(text.contains("day.ord"), "{text}");
    assert!(text.contains("RECORD TYPES"), "{text}");
    assert!(!text.contains("catalog"), "{text}");
}

#[test]
fn a_delimited_specs_page_lists_its_column_notes() {
    let doc = spec_page(
        r#"
name = "acme.log"
kind = "delimited"
description = "Instrument log"

[columns]
temp = { description = "Air temperature", unit = "deg F" }
"#,
    );
    let lines = lines(&doc, &HashSet::new(), None);
    assert!(
        !lines
            .iter()
            .any(|l| matches!(l, DocLine::Section("RECORD TYPES", _)))
    );
    assert!(lines.contains(&DocLine::Column {
        name: "temp".into(),
        about: "Air temperature (deg F)".into(),
        values: 0,
    }));
}

/// A declared type stands beside the unit, as the type row pairs them.
#[test]
fn a_typed_column_shows_its_type_beside_its_unit() {
    let doc = spec_page(
        r#"
name = "acme.log"
kind = "delimited"

[columns]
Latitude = { type = "f64", unit = "deg", description = "GPS latitude" }
LogIdx = { type = "i64" }
"#,
    );
    let lines = lines(&doc, &HashSet::new(), None);
    assert!(lines.contains(&DocLine::Column {
        name: "Latitude".into(),
        about: "f64 · deg  GPS latitude".into(),
        values: 0,
    }));
    assert!(lines.contains(&DocLine::Column {
        name: "LogIdx".into(),
        about: "i64".into(),
        values: 0,
    }));
}

#[test]
fn a_catalogs_word_stands_over_the_specs() {
    let catalog = crate::home::catalog::parse(
        r#"
label = "Mine"

[orders]
name = "Orders"
path = "/data/day.ord"
description = "Orders from the lab"
documentation = "https://example.com/lab.txt"
columns.price = { description = "Price the lab quotes" }
columns.venue = { description = "Where it traded" }
"#,
        "mine",
        crate::home::catalog::Origin::Mine,
        None,
    )
    .unwrap();
    let entry = Arc::new(catalog.datasets.into_iter().next().unwrap());
    let spec = crate::formats::Spec::parse(ORDERS, None).unwrap();
    let doc = Documented::new(
        Some(("Mine".into(), entry)),
        spec.docs().map(Arc::new),
        "day.ord".into(),
    )
    .unwrap();
    assert_eq!(doc.title(), "Orders");
    let lines = lines(&doc, &HashSet::new(), None);
    assert_eq!(lines[0], DocLine::About("Orders from the lab".into()));
    assert!(lines.contains(&DocLine::Field("catalog", "Mine".into())));
    assert!(lines.contains(&DocLine::Field("format spec", "acme.orders".into())));
    let links: Vec<&DocLine> = lines
        .iter()
        .filter(|l| matches!(l, DocLine::Link("documentation", _)))
        .collect();
    assert_eq!(
        links,
        [&DocLine::Link(
            "documentation",
            "https://example.com/lab.txt".into()
        )]
    );
    assert!(lines.contains(&DocLine::Section("RECORD TYPES", 2)));
    let columns: Vec<(String, String)> = lines
        .iter()
        .filter_map(|l| match l {
            DocLine::Column { name, about, .. } => Some((name.clone(), about.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        columns,
        [
            (
                "price".to_string(),
                "Price the lab quotes (USD)".to_string()
            ),
            ("side".into(), String::new()),
            ("shares".into(), "Shares executed".into()),
            ("venue".into(), "Where it traded".into()),
        ]
    );
}

#[test]
fn a_catalog_note_keeps_the_specs_legend_and_unit() {
    let over = ColumnNote {
        description: "Side of the book".into(),
        ..ColumnNote::default()
    };
    let under = ColumnNote {
        description: "Side".into(),
        unit: "flag".into(),
        values: vec![("1".into(), "BUY".into())],
        ty: String::new(),
    };
    assert_eq!(
        layered(&over, &under),
        ColumnNote {
            description: "Side of the book".into(),
            ..under.clone()
        }
    );
    let legend = ColumnNote {
        values: vec![("B".into(), "Buy".into())],
        ..ColumnNote::default()
    };
    assert_eq!(layered(&legend, &under).values, legend.values);
    assert_eq!(layered(&legend, &under).description, "Side");
}

#[test]
fn header_and_footer_fields_are_documented_in_their_own_sections() {
    let doc = spec_page(
        r#"
name = "acme.tape"
match = { glob = "*.tape" }

[header]
fields = [
  { name = "magic", type = "str", size = 4 },
  { type = "pad", size = 4 },
  { name = "trade_date", type = "u4", description = "Session date" },
  { name = "tick", type = "u4", unit = "ns" },
]

[records]
fields = [{ name = "px", type = "u4", description = "Price" }]

[footer]
fields = [{ name = "rows", type = "u4", description = "Records written" }]
"#,
    );
    let lines = lines(&doc, &HashSet::new(), None);
    let at = |line: &DocLine| lines.iter().position(|l| l == line).unwrap();
    let column = |name: &str, about: &str| DocLine::Column {
        name: name.into(),
        about: about.into(),
        values: 0,
    };
    let header = at(&DocLine::Section("HEADER", 2));
    assert_eq!(
        lines[header + 1..header + 3],
        [column("trade_date", "Session date"), column("tick", "ns")]
    );
    let columns = at(&DocLine::Section("COLUMNS", 1));
    let footer = at(&DocLine::Section("FOOTER", 1));
    assert!(header < columns && columns < footer);
    assert_eq!(lines[footer + 1], column("rows", "Records written"));
    // Nothing documented in the footer, no FOOTER section.
    let bare = spec_page(
        "name = \"a.b\"\n[header]\nfields = [{ name = \"v\", type = \"u1\", description = \"Version\" }]\n[records]\nfields = [{ name = \"x\", type = \"u1\" }]\n[footer]\nfields = [{ name = \"n\", type = \"u4\" }]\n",
    );
    let lines = super::lines(&bare, &HashSet::new(), None);
    assert!(lines.contains(&DocLine::Section("HEADER", 1)));
    assert!(
        !lines
            .iter()
            .any(|l| matches!(l, DocLine::Section("FOOTER", _)))
    );
}

#[test]
fn the_cursor_stays_in_view_and_bookmarks_are_listed() {
    let mut state = DocState::default();
    state.open(page(noaa()), None);
    state.move_cursor(isize::MAX / 2);
    let text = screen(&mut state, 80, 16).join("\n");
    assert!(text.contains("Central Park, NY"), "{text}");
}
