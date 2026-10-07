use super::*;

fn struct_row() -> Series {
    let df = df!(
        "id" => [7i64],
        "name" => ["ann"],
    )
    .unwrap();
    df.into_struct("customer".into()).into_series()
}

#[test]
fn a_struct_lists_its_fields_as_one_row_slices() {
    let node = Node::Native(struct_row());
    assert_eq!(node.shape(), Shape::Struct);
    assert_eq!(node.len(), 2);
    let kids = node.children(0, 10);
    let labels: Vec<_> = kids.iter().map(|(l, _)| l.as_str()).collect();
    assert_eq!(labels, ["id", "name"]);
    let (_, name) = &kids[1];
    assert_eq!(name.shape(), Shape::Leaf);
    assert_eq!(name.with_text(str::to_string).as_deref(), Some("ann"));
    assert_eq!(node.cell("id").unwrap().type_label(), "i64");
    assert!(node.cell("nope").is_none());
}

/// A list of a million items lists a page of them: only those are made.
#[test]
fn a_long_list_makes_only_the_page_asked_for() {
    let inner = Series::new("".into(), (0..1_000_000i64).collect::<Vec<_>>());
    let list = Series::new("l".into(), [inner]);
    let node = Node::Native(list);
    assert_eq!(node.shape(), Shape::List);
    assert_eq!(node.len(), 1_000_000);
    let page = node.children(999_998, 20);
    assert_eq!(page.len(), 2);
    assert_eq!(page[0].0, "[999998]");
    assert_eq!(page[1].1.type_label(), "i64");
    assert_eq!(node.label_width(), "[999999]".len());
}

#[test]
fn a_null_struct_or_list_is_a_leaf() {
    let list = Series::new_null("l".into(), 1).cast(&DataType::List(Box::new(DataType::Int64)));
    let node = Node::Native(list.unwrap());
    assert_eq!(node.shape(), Shape::Leaf);
    assert!(!node.opens());
}

#[test]
fn json_objects_keep_their_order_and_arrays_their_items() {
    let root = Arc::new(parse_json(r#"{"z": 1, "a": [true, null, {"k": "v"}]}"#).unwrap());
    let node = Node::Json {
        root,
        path: Vec::new(),
    };
    assert_eq!(node.shape(), Shape::Object);
    let kids = node.children(0, 9);
    assert_eq!(kids[0].0, "z", "the document's order, not sorted");
    assert_eq!(kids[0].1.type_label(), "number");
    let (_, a) = &kids[1];
    assert_eq!(a.shape(), Shape::Array);
    assert_eq!(a.len(), 3);
    let (label, obj) = a.child(2).unwrap();
    assert_eq!(label, "[2]");
    assert_eq!(obj.shape(), Shape::Object);
    assert_eq!(obj.cell("k").unwrap().json(), Some(&Value::from("v")));
    assert_eq!(a.child(1).unwrap().1.shape(), Shape::Leaf);
}

/// A step into an object is its key: the items near the end of a large object
/// are found without walking to them, in the document's order.
#[test]
fn a_large_objects_last_keys_resolve_by_key() {
    let n = 200_000;
    let text = format!(
        "{{{}}}",
        (0..n)
            .map(|i| format!("\"k{i}\": {i}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let node = Node::Json {
        root: Arc::new(parse_json(&text).unwrap()),
        path: Vec::new(),
    };
    let tail = node.children(n - 3, 10);
    let labels: Vec<_> = tail.iter().map(|(l, _)| l.as_str()).collect();
    assert_eq!(labels, ["k199997", "k199998", "k199999"]);
    assert_eq!(tail[2].1.json(), Some(&Value::from(n - 1)));
    let Node::Json { path, .. } = &tail[2].1 else {
        panic!("a JSON item");
    };
    assert!(matches!(&path[..], [Step::Key(k)] if &**k == "k199999"));
    let head = node.children(1, 2);
    assert_eq!(head[1].0, "k2");
    assert_eq!(page(0..10, 7, 5), [7, 8, 9]);
    assert_eq!(page(0..10, 2, 3), [2, 3, 4]);
    assert!(page(0..10, 10, 3).is_empty());
}

#[test]
fn text_opens_only_when_it_reads_as_an_object_or_array() {
    assert!(looks_like_json(" {\"a\": 1}\n"));
    assert!(looks_like_json("[1, 2]"));
    assert!(!looks_like_json("{not closed"));
    assert!(!looks_like_json("\"text\""));
    assert!(!looks_like_json("42"));
    let s = Series::new("s".into(), ["[1, 2]"]);
    assert!(Node::Native(s).opens());
    let s = Series::new("s".into(), ["plain"]);
    assert!(!Node::Native(s).opens());
}

/// Depth and size are bounded: a deep document is an error, a huge text is
/// refused before it is parsed.
#[test]
fn parsing_is_bounded() {
    let deep = format!("{}{}", "[".repeat(10_000), "]".repeat(10_000));
    assert!(parse_json(&deep).is_err());
    let huge = format!("[{}0]", "0,".repeat(JSON_MAX_BYTES / 2));
    let err = parse_json(&huge).unwrap_err();
    assert!(err.contains("MiB"), "{err}");
    assert!(parse_json("{oops}").unwrap_err().starts_with("not JSON"));
}

#[test]
fn json_text_stops_at_its_cap() {
    let value: Value = serde_json::from_str(&format!("[{}1]", "1,".repeat(10_000))).unwrap();
    let (text, cut) = json_text(&value, true, 100);
    assert!(cut);
    assert_eq!(text.len(), 100);
    let (whole, cut) = json_text(&Value::from("é"), false, 100);
    assert!(!cut);
    assert_eq!(whole, "\"é\"");
    // Cut inside a character: the part that is whole.
    let (text, cut) = json_text(&Value::from("ééé"), false, 4);
    assert!(cut);
    assert_eq!(text, "\"é");
    assert_eq!(json_copy_text(&value, 100), None);
    assert_eq!(json_copy_text(&Value::from("raw"), 100).unwrap(), "raw");
}

#[test]
fn a_list_of_structs_and_an_array_of_objects_are_tables() {
    let items = df!("sku" => ["A1", "B7"], "qty" => [2i64, 1])
        .unwrap()
        .into_struct("".into())
        .into_series();
    let list = Series::new("items".into(), [items]);
    let node = Node::Native(list);
    assert_eq!(
        node.table_columns(),
        Some(vec![
            ("sku".to_string(), DataType::String),
            ("qty".to_string(), DataType::Int64)
        ])
    );
    let (_, first) = node.child(0).unwrap();
    assert_eq!(first.shape(), Shape::Struct);
    assert_eq!(
        first
            .cell("sku")
            .unwrap()
            .with_text(str::to_string)
            .as_deref(),
        Some("A1")
    );

    let root = Arc::new(parse_json(r#"[{"a": 1, "b": 2}, {"b": 3}]"#).unwrap());
    let arr = Node::Json {
        root,
        path: Vec::new(),
    };
    assert_eq!(
        arr.table_columns(),
        Some(vec![
            ("a".to_string(), DataType::Int64),
            ("b".to_string(), DataType::Int64)
        ])
    );
    assert!(arr.child(1).unwrap().1.cell("a").is_none());
}
