use super::*;

const ORDERS: &str = r#"
name = "acme.orders"
description = "  Order entry capture  "
documentation = "https://example.com/orders.pdf"
match = { glob = "*.ord" }

[records]
framing = "length_prefixed"
size = "len"
size_adjust = 2
type = "kind"
fields = [
  { name = "len", type = "u2" },
  { name = "kind", type = "str", size = 1, description = "Message type" },
]

[[variants]]
name = "add"
when = "A"
description = "An order added to the book"
fields = [
  { name = "ref", type = "u8", description = "Order reference" },
  { name = "price", type = "u4", scale = 4, unit = "USD" },
  { name = "side", type = "u1", enum = { 1 = "BUY", 2 = "SELL" } },
]

[[variants]]
name = "exec"
when = ["E", "C"]
fields = [{ name = "ref", type = "u8" }, { name = "shares", type = "u4", unit = "shares" }]
"#;

fn error_of(text: &str) -> String {
    Spec::parse(text, None).unwrap_err().message
}

#[test]
fn a_spec_takes_the_catalogs_documentation_keys() {
    let spec = Spec::parse(ORDERS, None).unwrap();
    assert_eq!(spec.description.as_deref(), Some("Order entry capture"));
    assert_eq!(
        spec.documentation.as_deref(),
        Some("https://example.com/orders.pdf")
    );
    let add = &spec.records.variants[0];
    assert_eq!(
        add.description.as_deref(),
        Some("An order added to the book")
    );
    assert_eq!(add.fields[1].unit.as_deref(), Some("USD"));
    assert_eq!(
        spec.records.fields[1].description.as_deref(),
        Some("Message type")
    );
    // Documentation only: the same spec without it reads the same columns.
    let plain = Spec::parse(
        &ORDERS
            .replace(", description = \"Message type\"", "")
            .replace(", unit = \"USD\"", ""),
        None,
    )
    .unwrap();
    assert_eq!(
        plain.static_columns(),
        spec.static_columns(),
        "nothing documented changes the columns"
    );
}

#[test]
fn empty_documentation_and_unknown_keys_are_refused() {
    for (from, to, said) in [
        (
            "description = \"  Order entry capture  \"",
            "description = \"  \"",
            "description: must not be empty",
        ),
        (
            "documentation = \"https://example.com/orders.pdf\"",
            "documentation = \"ftp://example.com/x\"",
            "documentation: \"ftp://example.com/x\" is not an https:// link",
        ),
        (
            "description = \"Order reference\"",
            "description = \"\"",
            "description: must not be empty",
        ),
        ("unit = \"USD\"", "unit = \" \"", "unit: must not be empty"),
        (
            "description = \"An order added to the book\"",
            "description = \"\"",
            "description: must not be empty",
        ),
        (
            "unit = \"USD\"",
            "units = \"USD\"",
            "unknown key `units` in a field",
        ),
        (
            "description = \"An order added to the book\"",
            "about = \"x\"",
            "unknown key `about` in a variant",
        ),
        (
            "documentation = \"https://example.com/orders.pdf\"",
            "url = \"https://example.com/orders.pdf\"",
            "unknown key `url` in the spec",
        ),
        (
            "unit = \"USD\"",
            "values = { 1 = \"x\" }",
            "unknown key `values` in a field",
        ),
    ] {
        assert!(ORDERS.contains(from), "{from}");
        let e = error_of(&ORDERS.replacen(from, to, 1));
        assert!(e.contains(said), "{to}: {e}");
    }
    // Skipped bytes have no column to document.
    let e = error_of(&ORDERS.replace(
        "{ name = \"len\", type = \"u2\" },",
        "{ name = \"len\", type = \"u2\" }, { type = \"pad\", size = 1, description = \"x\" },",
    ));
    assert!(e.contains("pad: skipped bytes take only a size"), "{e}");
}

#[test]
fn a_variant_spec_documents_its_record_types_and_columns() {
    let docs = Spec::parse(ORDERS, None).unwrap().docs().unwrap();
    assert_eq!(docs.spec, "acme.orders");
    assert_eq!(docs.description, "Order entry capture");
    assert_eq!(docs.documentation, "https://example.com/orders.pdf");
    assert_eq!(
        docs.record_types,
        [
            RecordType {
                name: "add".into(),
                picked_by: "kind = \"A\"".into(),
                description: "An order added to the book".into(),
                columns: 5,
            },
            RecordType {
                name: "exec".into(),
                picked_by: "kind in (\"E\", \"C\")".into(),
                description: String::new(),
                columns: 4,
            },
        ]
    );
    let names: Vec<&str> = docs.columns.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["kind", "ref", "price", "side", "shares"]);
    let note = |name: &str| {
        docs.columns
            .iter()
            .find(|(n, _)| n == name)
            .unwrap()
            .1
            .clone()
    };
    assert_eq!(note("ref").description, "Order reference");
    assert_eq!(note("price").unit, "USD");
    // The enum is the column's value legend.
    assert_eq!(
        note("side").values,
        [
            ("1".to_string(), "BUY".to_string()),
            ("2".into(), "SELL".into())
        ]
    );
    // A spec that says nothing beyond how to read has no page.
    let bare = "name = \"a.b\"\nmatch = { glob = \"*.b\" }\n[records]\nfields = [{ name = \"x\", type = \"u1\" }]\n";
    assert_eq!(Spec::parse(bare, None).unwrap().docs(), None);
}

#[test]
fn a_flattened_fields_note_is_filed_under_each_of_its_columns() {
    let text = "name = \"a.b\"\n[records]\nfields = [{ name = \"bid\", type = \"u4\", count = 3, flatten = true, description = \"Bid level\", unit = \"USD\" }]\n";
    let spec = Spec::parse(text, None).unwrap();
    let docs = spec.docs().unwrap();
    let names: Vec<&str> = docs.columns.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["bid_0", "bid_1", "bid_2"]);
    assert!(
        docs.columns
            .iter()
            .all(|(_, note)| note.description == "Bid level" && note.unit == "USD")
    );
    let schema: Vec<String> = spec
        .static_columns()
        .unwrap()
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    assert_eq!(schema, names);
}

#[test]
fn a_delimited_specs_columns_take_a_description_and_unit() {
    let text = r#"
name = "acme.log"
kind = "delimited"
documentation = "https://example.com/log"
header_rows = { name = 2, unit = 1 }

[columns]
time = { from = ["date", "clock"], as = "datetime", description = "When it was read" }
temp = { description = "Air temperature", unit = "deg F" }
volts = { unit = "V" }
"#;
    let spec = Spec::parse(text, None).unwrap();
    let delimited = spec.delimited.as_ref().unwrap();
    // A note alone is no derived column; the units line still works.
    assert_eq!(delimited.columns.len(), 1);
    assert_eq!(delimited.header_rows.as_ref().unwrap().unit, Some(1));
    let docs = spec.docs().unwrap();
    assert!(docs.record_types.is_empty());
    assert_eq!(
        docs.columns,
        [
            (
                "time".to_string(),
                ColumnNote {
                    description: "When it was read".into(),
                    ..Default::default()
                }
            ),
            (
                "temp".to_string(),
                ColumnNote {
                    description: "Air temperature".into(),
                    unit: "deg F".into(),
                    values: Vec::new(),
                    ty: String::new(),
                }
            ),
            (
                "volts".to_string(),
                ColumnNote {
                    unit: "V".into(),
                    ..Default::default()
                }
            ),
        ]
    );
    for (entry, said) in [
        ("t = {}", "columns.t: missing `from`"),
        (
            "t = { description = \"\" }",
            "columns.t.description: must not be empty",
        ),
        (
            "t = { values = { a = \"b\" } }",
            "unknown key `values` in columns.t",
        ),
        ("t = { format = \"%Y\" }", "columns.t: missing `from`"),
        (
            "t = { from = \"d\", as = \"date\", type = \"date\" }",
            "columns.t: a derived column takes `as` for its type, not `type`",
        ),
        (
            "t = { type = \"int\" }",
            "columns.t.type: unknown type \"int\"; expected one of str, bool, i8",
        ),
        (
            "t = { type = \"i64\", format = \"%Y\" }",
            "columns.t.type: format is for date, time and datetime, not i64",
        ),
    ] {
        let e = error_of(&format!(
            "name = \"a.b\"\nkind = \"delimited\"\n[columns]\n{entry}\n"
        ));
        assert!(e.contains(said), "{entry}: {e}");
    }
    let e = error_of("name = \"a.b\"\nkind = \"delimited\"\nurl = \"https://x\"\n");
    assert!(e.contains("unknown key `url`"), "{e}");
}
