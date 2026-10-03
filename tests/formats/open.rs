//! Opening binary files through format specs: `--spec`, `--format NAME`, a glob, magic,
//! a compressed file, a tie and its picker, and the files specs must leave alone.

use super::*;
use datui::formats::{Registry, Spec};
use std::sync::Arc;

const L2: &str = r#"
name = "acme.l2feed"
match = { glob = ["*.l2"], magic = "L2FD" }
endian = "le"

[header]
fields = [{ name = "magic", type = "str", size = 4 }, { name = "count", type = "u8" }]

[records]
count = "header.count"
fields = [
  { name = "ts",     type = "u8", time = "ns" },
  { name = "symbol", type = "str", size = 8 },
  { name = "side",   type = "u1", enum = { 1 = "BUY", 2 = "SELL" } },
  { name = "price",  type = "u4", scale = 4 },
]
"#;

/// An L2 file of `rows` records: symbol `S{i}`, price `i` ten-thousandths.
fn l2_bytes(rows: u64) -> Vec<u8> {
    let mut out = b"L2FD".to_vec();
    out.extend(rows.to_le_bytes());
    for i in 0..rows {
        out.extend((1_700_000_000_000_000_000u64 + i).to_le_bytes());
        let mut symbol = format!("S{i}").into_bytes();
        symbol.resize(8, 0);
        out.extend(symbol);
        out.push(1 + (i % 2) as u8);
        out.extend((i as u32).to_le_bytes());
    }
    out
}

fn spec(text: &str) -> Spec {
    Spec::parse(text, None).unwrap()
}

fn app_with(specs: Vec<Spec>) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    app.set_formats(Registry::of(specs));
    (app, rx, tx)
}

fn screen(app: &mut App) -> String {
    let area = Rect::new(0, 0, 120, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    rendered_text(&buffer)
}

fn notes(app: &App) -> Vec<String> {
    app.data_table_state
        .as_ref()
        .unwrap()
        .notes()
        .into_iter()
        .map(|n| n.summary)
        .collect()
}

#[test]
fn a_spec_file_opens_a_binary_file_and_scrolls_to_its_last_row() {
    let dir = common::fixture_dir();
    let spec_path = dir.join("spec_file_l2.toml");
    std::fs::write(&spec_path, L2).unwrap();
    // A name no glob matches: `--spec` reads it whatever it is called.
    let data = dir.join("spec_file_feed.dat");
    std::fs::write(&data, l2_bytes(5_000)).unwrap();
    let (mut app, rx, tx) = app_with(Vec::new());
    let options = OpenOptions {
        spec_file: Some(spec_path),
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![data], options);
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows(), 5_000);
    assert_eq!(
        state.format_read().unwrap().spec.name,
        "acme.l2feed",
        "the spec is kept for the notes and the picker"
    );
    assert!(
        notes(&app)
            .iter()
            .any(|n| n == "read as acme.l2feed, chosen by --spec"),
        "{:?}",
        notes(&app)
    );
    assert!(notes(&app).iter().any(|n| n.contains("count = 5000")));
    let first = screen(&mut app);
    assert!(first.contains("S0") && first.contains("BUY"), "{first}");
    assert!(first.contains("0.0001"), "a scaled price: {first}");
    assert!(first.contains("Format"), "the bar offers b: {first}");

    press_and_send(&mut app, &tx, KeyCode::End);
    pump_until_idle(&mut app, &rx, &tx);
    let last = screen(&mut app);
    assert!(last.contains("S4999"), "the last row is on screen: {last}");
    assert!(last.contains("0.4999"), "{last}");
    assert!(!last.contains(" S0 "), "{last}");
}

#[test]
fn a_glob_match_reads_the_file_and_a_tie_is_said_and_picked_from() {
    let dir = common::fixture_dir();
    let data = dir.join("tie_day.l2");
    std::fs::write(&data, l2_bytes(3)).unwrap();
    let other = L2.replace("acme.l2feed", "acme.other");
    let (mut app, rx, tx) = app_with(vec![spec(L2), spec(&other)]);
    pump_open_until_loaded(&mut app, &rx, vec![data], OpenOptions::default());
    let read = app
        .data_table_state
        .as_ref()
        .unwrap()
        .format_read()
        .unwrap()
        .clone();
    assert_eq!(read.spec.name, "acme.l2feed", "the first on the path wins");
    assert_eq!(read.also, ["acme.other"]);
    assert!(
        notes(&app)
            .iter()
            .any(|n| n.contains("acme.other also matches this file; press b")),
        "{:?}",
        notes(&app)
    );
    assert!(screen(&mut app).contains(" 2 formats match "));

    press(&mut app, KeyCode::Char('b'));
    assert_eq!(app.input_mode, InputMode::PickFormat);
    assert!(screen(&mut app).contains("acme.other"));
    press(&mut app, KeyCode::Down);
    let reopen = press(&mut app, KeyCode::Enter).expect("Enter reads the file again");
    let mut next = Some(reopen);
    while let Some(event) = next.take() {
        next = app.event(&event);
    }
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.format_read().unwrap().spec.name, "acme.other");
    assert_eq!(state.num_rows(), 3);
}

#[test]
fn magic_matches_a_file_no_glob_names() {
    let dir = common::fixture_dir();
    let data = dir.join("magic_only.bin");
    std::fs::write(&data, l2_bytes(4)).unwrap();
    let (mut app, rx, _tx) = app_with(vec![spec(L2)]);
    pump_open_until_loaded(&mut app, &rx, vec![data], OpenOptions::default());
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows(), 4);
    assert!(
        notes(&app)
            .iter()
            .any(|n| n.ends_with("chosen by its magic"))
    );
}

#[test]
fn a_compressed_file_is_decompressed_then_read() {
    let dir = common::fixture_dir();
    let data = dir.join("compressed_day.l2.zst");
    let packed = zstd::encode_all(l2_bytes(25).as_slice(), 3).unwrap();
    std::fs::write(&data, packed).unwrap();
    let (mut app, rx, _tx) = app_with(vec![spec(L2)]);
    pump_open_until_loaded(&mut app, &rx, vec![data], OpenOptions::default());
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows(), 25);
    assert_eq!(state.format_read().unwrap().spec.name, "acme.l2feed");
    let shown = screen(&mut app);
    assert!(shown.contains("S6"), "{shown}");
}

#[test]
fn a_bad_magic_fails_the_open_saying_what_it_found() {
    let dir = common::fixture_dir();
    let data = dir.join("bad_magic.l2");
    let mut bytes = l2_bytes(2);
    bytes[..4].copy_from_slice(b"ZZZZ");
    std::fs::write(&data, bytes).unwrap();
    let (mut app, rx, _tx) = app_with(vec![spec(L2)]);
    let options = OpenOptions {
        spec_name: Some("acme.l2feed".into()),
        ..OpenOptions::default()
    };
    let message = pump_open_until_error(&mut app, &rx, vec![data], options).unwrap();
    assert!(
        message.contains("expected magic 4c 32 46 44 at byte 0, found 5a 5a 5a 5a"),
        "{message}"
    );
}

#[test]
fn a_format_name_not_on_the_path_says_so() {
    let dir = common::fixture_dir();
    let data = dir.join("unknown_name.l2");
    std::fs::write(&data, l2_bytes(1)).unwrap();
    let (mut app, rx, _tx) = app_with(vec![spec(L2)]);
    let options = OpenOptions {
        spec_name: Some("acme.missing".into()),
        ..OpenOptions::default()
    };
    let message = pump_open_until_error(&mut app, &rx, vec![data], options).unwrap();
    assert!(
        message.contains("no format named acme.missing"),
        "{message}"
    );
}

/// spec and no reader takes opens in the hex view.
/// spec matches opens in the hex view, as one did before specs.
#[test]
fn files_that_open_today_open_the_same_way() {
    let dir = common::fixture_dir();
    let greedy = r#"name = "acme.greedy"
match = { glob = ["*.csv", "*.parquet"], magic = "a" }
[records]
fields = [{ name = "x", type = "u1" }]"#;
    let csv = dir.join("greedy_today.csv");
    std::fs::write(&csv, "a,b\n1,2\n").unwrap();
    let (mut app, rx, _tx) = app_with(vec![spec(greedy)]);
    pump_open_until_loaded(&mut app, &rx, vec![csv], OpenOptions::default());
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.format_read().is_none());
    assert_eq!(state.num_rows(), 1);

    let (mut app, rx, _tx) = app_with(vec![spec(L2)]);
    let unmatched = dir.join("unmatched_today.bin");
    std::fs::write(&unmatched, [0u8, 1, 2, 3]).unwrap();
    // No reader and no spec: its bytes, in the hex view (#588), not an error.
    pump_open_until_loaded(&mut app, &rx, vec![unmatched], OpenOptions::default());
    drain_events(&mut app, &rx);
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    assert_eq!(app.input_mode, InputMode::Hex);
    assert!(app.hex_view().unwrap().fallback);
}

#[test]
fn a_directory_of_column_files_opens_by_name() {
    let dir = common::fixture_dir().join("splayed_trades");
    std::fs::create_dir_all(&dir).unwrap();
    let mut price = Vec::new();
    let mut size = Vec::new();
    for i in 0..100i64 {
        price.extend((i as f64 / 2.0).to_be_bytes());
        size.extend((i * 10).to_be_bytes());
    }
    std::fs::write(dir.join("price"), price).unwrap();
    std::fs::write(dir.join("size"), size).unwrap();
    let splayed = r#"name = "kdb.trades"
layout = "columns"
endian = "be"
[records]
fields = [{ name = "price", type = "f8" }, { name = "size", type = "s8" }]"#;
    let registry = Registry::of(vec![spec(splayed)]);
    assert!(
        matches!(
            App::route_named_paths_with(vec![dir.clone()], OpenOptions::default(), &registry),
            AppEvent::LookThenOpenDirectory(..)
        ),
        "a directory no glob names is looked at, as before"
    );
    let globbed =
        spec(&splayed.replace("[records]", "match = { glob = \"splayed_*\" }\n[records]"));
    assert!(matches!(
        App::route_named_paths_with(
            vec![dir.clone()],
            OpenOptions::default(),
            &Registry::of(vec![globbed])
        ),
        AppEvent::Open(..)
    ));
    let (mut app, rx, tx) = app_with(vec![spec(splayed)]);
    let options = OpenOptions {
        spec_name: Some("kdb.trades".into()),
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![dir], options);
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), 100);
    press_and_send(&mut app, &tx, KeyCode::End);
    pump_until_idle(&mut app, &rx, &tx);
    let last = screen(&mut app);
    assert!(last.contains("49.5") && last.contains("990"), "{last}");
}

/// Times the first page and a jump to the last row of a large file, for the numbers in
/// the pull request. Run on a generated file:
/// `DATUI_BENCH_L2=~/tmp/big.l2 cargo test --release --test integration_test
/// formats_open::time_a_large_file -- --ignored --nocapture`
#[test]
#[ignore = "needs a large generated file"]
fn time_a_large_file() {
    let Some(path) = std::env::var_os("DATUI_BENCH_L2").map(PathBuf::from) else {
        return;
    };
    let (mut app, rx, tx) = app_with(vec![spec(L2)]);
    let started = std::time::Instant::now();
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    let opened = started.elapsed();
    let rows = app.data_table_state.as_ref().unwrap().num_rows();
    let first = screen(&mut app);
    assert!(first.contains("S0"), "{first}");
    let started = std::time::Instant::now();
    press_and_send(&mut app, &tx, KeyCode::End);
    pump_until_idle(&mut app, &rx, &tx);
    let to_end = started.elapsed();
    let last = screen(&mut app);
    // The price is the row number in ten-thousandths.
    let price = format!("{}.{:04}", (rows - 1) / 10_000, (rows - 1) % 10_000);
    assert!(last.contains(&price), "{last}");
    let records = app
        .data_table_state
        .as_ref()
        .unwrap()
        .format_read()
        .unwrap()
        .records
        .clone();
    let started = std::time::Instant::now();
    let window = records.window(rows - 50, 50).unwrap().collect().unwrap();
    let windowed = started.elapsed();
    let started = std::time::Instant::now();
    let sliced = Arc::clone(&records)
        .into_lazy()
        .unwrap()
        .slice((rows - 50) as i64, 50)
        .collect()
        .unwrap();
    let full = started.elapsed();
    assert!(window.equals_missing(&sliced));
    let started = std::time::Instant::now();
    let price = Arc::clone(&records)
        .into_lazy()
        .unwrap()
        .select([col("price").cast(DataType::Float64).sum()])
        .collect()
        .unwrap();
    let column_pass = started.elapsed();
    println!(
        "rows {rows}: open to first page {opened:?}, End to last row {to_end:?}, \
         window of the last 50 {windowed:?}, slice of the last 50 without the window \
         {full:?}, sum of one column {column_pass:?} ({price:?})"
    );
    // Queries as the app runs them, streaming asked for: time and the most anonymous
    // memory held while each ran (the map's pages are the file's, not counted).
    let lf = Arc::clone(&records).into_lazy().unwrap();
    let queries: [(&str, LazyFrame); 5] = [
        (
            "filter and count",
            lf.clone()
                .filter(col("side").eq(lit("SELL")))
                .select([len()]),
        ),
        (
            "group by side",
            lf.clone()
                .group_by([col("side")])
                .agg([col("price").cast(DataType::Float64).mean()]),
        ),
        (
            "sort by price, top 10",
            lf.clone()
                .sort(
                    ["price"],
                    SortMultipleOptions::default().with_order_descending(true),
                )
                .limit(10),
        ),
        (
            "sort by time, top 10",
            lf.clone()
                .sort(
                    ["ts"],
                    SortMultipleOptions::default().with_order_descending(true),
                )
                .limit(10),
        ),
        (
            "slice of the last 50",
            lf.clone().slice((rows - 50) as i64, 50),
        ),
    ];
    for (name, query) in queries {
        let (took, peak) = with_peak_anon(|| {
            datui::statistics::collect_lazy(query, true).unwrap();
        });
        println!("{name}: {took:?}, peak anonymous memory {} MiB", peak >> 20);
    }
}

/// How long `f` takes and the most anonymous resident memory the process held while it
/// ran, sampled every few milliseconds.
fn with_peak_anon(f: impl FnOnce()) -> (std::time::Duration, u64) {
    fn anon() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("RssAnon:"))
                    .and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok())
            })
            .map_or(0, |kb| kb * 1024)
    }
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop = done.clone();
    let sampler = std::thread::spawn(move || {
        let mut peak = anon();
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
            peak = peak.max(anon());
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        peak.max(anon())
    });
    let before = anon();
    let started = std::time::Instant::now();
    f();
    let took = started.elapsed();
    done.store(true, std::sync::atomic::Ordering::Relaxed);
    (took, sampler.join().unwrap().saturating_sub(before))
}
