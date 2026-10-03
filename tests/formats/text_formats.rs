//! VCD value change dumps, FIX logs and SDF compound files, each read into a table of
//! its own.
//!
//! Fixtures are small files written here, under `fixture_dir()`.

use crate::common::{self, next_event, pump_open_until_loaded};
use datui::widgets::info::InfoTab;
use datui::{App, AppEvent, OpenOptions};
use polars::prelude::*;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::mpsc;

/// A directory of the test's own, for its fixtures and converted files.
fn scratch(name: &str) -> PathBuf {
    let dir = common::fixture_dir().join(format!(
        "text-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn options_in(dir: &Path) -> OpenOptions {
    OpenOptions {
        temp_dir: Some(dir.join("tmp")),
        ..OpenOptions::default()
    }
}

fn open_with(path: PathBuf, options: OpenOptions) -> (App, mpsc::Receiver<AppEvent>) {
    if let Some(dir) = &options.temp_dir {
        std::fs::create_dir_all(dir).unwrap();
    }
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], options);
    (app, rx)
}

fn frame(app: &App) -> DataFrame {
    app.data_table_state
        .as_ref()
        .expect("a dataset is open")
        .lf()
        .clone()
        .collect()
        .expect("collect")
}

fn names(df: &DataFrame) -> Vec<String> {
    df.get_column_names()
        .iter()
        .map(|n| n.to_string())
        .collect()
}

fn strings(df: &DataFrame, name: &str) -> Vec<Option<String>> {
    df.column(name)
        .unwrap_or_else(|_| panic!("{name} in {:?}", names(df)))
        .str()
        .unwrap()
        .iter()
        .map(|s| s.map(String::from))
        .collect()
}

fn gzip(path: &Path, bytes: &[u8]) {
    let mut gz = flate2::write::GzEncoder::new(
        std::fs::File::create(path).unwrap(),
        flate2::Compression::default(),
    );
    gz.write_all(bytes).unwrap();
    gz.finish().unwrap();
}

fn files_in(dir: &Path) -> usize {
    std::fs::read_dir(dir).map_or(0, |d| d.count())
}

/// `i` opens the Info panel on `tab`.
fn info_opens_on(app: &mut App, tab: InfoTab) {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    // Notes take the panel first while unread.
    app.data_table_state.as_mut().unwrap().mark_notes_seen();
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.info_modal.active_tab, tab);
}

const VCD: &str = "$date
  Fri Oct  2 12:00:00 2026
$end
$version Icarus Verilog $end
$timescale 1ns $end
$scope module tb $end
$var wire 1 ! clk $end
$var reg 4 \" count [3:0] $end
$scope module dut $end
$var wire 1 # ready $end
$upscope $end
$upscope $end
$enddefinitions $end
#0
$dumpvars
0!
b0 \"
x#
$end
#5
1!
#10
0!
b1 \"
1#
#15
1!
#20
0!
b10 \"
";

#[test]
fn a_vcd_dump_opens_as_its_value_changes() {
    let dir = scratch("vcd");
    let path = dir.join("counter.vcd");
    std::fs::write(&path, VCD).unwrap();
    let (mut app, _rx) = open_with(path, options_in(&dir));
    assert_eq!(app.error_message(), None);
    let df = frame(&app);
    assert_eq!(names(&df), ["time", "signal", "value", "int", "width"]);
    assert_eq!(df.height(), 10);
    assert_eq!(
        df.column("time").unwrap().dtype(),
        &DataType::Duration(TimeUnit::Nanoseconds)
    );
    assert_eq!(
        strings(&df, "signal")[..3],
        [
            Some("tb.clk".into()),
            Some("tb.count[3:0]".into()),
            Some("tb.dut.ready".into())
        ]
    );
    assert_eq!(strings(&df, "value")[9], Some("0010".into()));
    let int: Vec<_> = df.column("int").unwrap().u64().unwrap().iter().collect();
    assert_eq!(int[2], None, "x has no integer");
    assert_eq!(int[9], Some(2));
    let state = app.data_table_state.as_ref().unwrap();
    let detail = state.format_detail().expect("a VCD tab");
    assert_eq!(detail.tab, "VCD");
    assert_eq!(detail.list.len(), 3);
    assert!(detail.lines.iter().any(|l| l == "Version: Icarus Verilog"));
    info_opens_on(&mut app, InfoTab::Format);
    drop(app);
    assert_eq!(files_in(&dir.join("tmp")), 0, "removed with the dataset");
}

/// The wide table of the docs: one row per time, one column per signal, each carried
/// forward from its last change.
#[cfg(feature = "sql")]
#[test]
fn the_docs_pivot_gives_the_wide_table() {
    let dir = scratch("vcd-wide");
    let path = dir.join("counter.vcd");
    std::fs::write(&path, VCD).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    std::fs::create_dir_all(dir.join("tmp")).unwrap();
    pump_open_until_loaded(&mut app, &rx, vec![path], options_in(&dir));
    let sql = "SELECT time,
       MAX(clk) OVER (PARTITION BY clk_n) AS clk,
       MAX(count) OVER (PARTITION BY count_n) AS count
FROM (
  SELECT *, COUNT(clk) OVER (ORDER BY time) AS clk_n,
            COUNT(count) OVER (ORDER BY time) AS count_n
  FROM (
    SELECT time,
           MAX(CASE WHEN signal = 'tb.clk' THEN value END) AS clk,
           MAX(CASE WHEN signal = 'tb.count[3:0]' THEN value END) AS count
    FROM df GROUP BY time
  )
)
ORDER BY time";
    app.event(&AppEvent::SqlSearch(sql.to_string()));
    super::pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error().is_none(), "{:?}", state.error());
    let df = frame(&app);
    assert_eq!(df.height(), 5);
    assert_eq!(
        strings(&df, "clk"),
        ["0", "1", "0", "1", "0"].map(|s| Some(s.to_string()))
    );
    assert_eq!(
        strings(&df, "count"),
        ["0000", "0000", "0001", "0001", "0010"].map(|s| Some(s.to_string())),
        "carried forward between changes"
    );
}

#[test]
fn a_compressed_vcd_and_one_named_otherwise_open() {
    let dir = scratch("vcd-gz");
    let gz = dir.join("counter.vcd.gz");
    gzip(&gz, VCD.as_bytes());
    let (app, _rx) = open_with(gz, options_in(&dir));
    assert_eq!(app.error_message(), None);
    assert_eq!(frame(&app).height(), 10);

    let dump = dir.join("waves.dump");
    std::fs::write(&dump, VCD).unwrap();
    let (app, _rx) = open_with(dump, options_in(&dir));
    assert_eq!(app.error_message(), None, "known by its header");
    assert_eq!(frame(&app).height(), 10);
}

/// A FIX message of `body` (the fields after 9 and before 10) with its BodyLength and
/// CheckSum right, its fields joined by `delim`.
fn fix(begin: &str, body: &[(u32, &str)], delim: &str) -> String {
    let body: String = body.iter().map(|(t, v)| format!("{t}={v}\x01")).collect();
    let head = format!("8={begin}\x019={}\x01", body.len());
    let sum: u32 = head.bytes().chain(body.bytes()).map(u32::from).sum();
    format!("{head}{body}10={:03}\x01", sum % 256).replace('\x01', delim)
}

fn fix_log() -> String {
    let order = fix(
        "FIX.4.4",
        &[
            (35, "D"),
            (34, "2"),
            (49, "BUYSIDE"),
            (56, "BROKERX"),
            (52, "20261002-13:30:00.250"),
            (11, "ord-1"),
            (55, "ACME"),
            (54, "1"),
            (38, "500"),
            (40, "2"),
            (44, "101.25"),
            (9001, "VWAP"),
        ],
        "\x01",
    );
    let fill = fix(
        "FIX.4.4",
        &[
            (35, "8"),
            (34, "3"),
            (49, "BROKERX"),
            (56, "BUYSIDE"),
            (52, "20261002-13:30:01"),
            (11, "ord-1"),
            (17, "exec-1"),
            (150, "F"),
            (39, "2"),
            (55, "ACME"),
            (54, "1"),
            (32, "500"),
            (31, "101.2"),
            (453, "2"),
            (448, "BUYER"),
            (452, "3"),
            (448, "SELLER"),
            (452, "1"),
            (60, "20261002-13:30:00.900"),
        ],
        "|",
    );
    let bad = fix("FIX.4.2", &[(35, "0"), (49, "BROKERX")], "|").replace("10=", "10=1");
    format!(
        "20261002-13:30:00.251 OUT FIX.4.4:BUYSIDE->BROKERX : {order}\n\
         20261002-13:30:01.002 IN FIX.4.4:BUYSIDE->BROKERX : {fill}\n\
         a line that is not FIX\n\
         20261002-13:30:02.000 IN FIX.4.4:BUYSIDE->BROKERX : {bad}\n"
    )
}

#[test]
fn a_fix_log_opens_as_its_messages_by_its_content() {
    let dir = scratch("fix");
    let path = dir.join("session.log");
    std::fs::write(&path, fix_log()).unwrap();
    let (mut app, _rx) = open_with(path, options_in(&dir));
    assert_eq!(app.error_message(), None);
    let df = frame(&app);
    assert_eq!(df.height(), 3);
    let columns = names(&df);
    assert_eq!(columns[..3], ["prefix", "direction", "session"]);
    assert_eq!(
        columns[columns.len() - 2..],
        ["body_length_ok", "checksum_ok"]
    );
    assert_eq!(
        strings(&df, "MsgType"),
        [
            Some("NewOrderSingle".into()),
            Some("ExecutionReport".into()),
            Some("Heartbeat".into())
        ]
    );
    assert_eq!(strings(&df, "MsgType_code")[0], Some("D".into()));
    assert_eq!(strings(&df, "Side")[1], Some("Buy".into()));
    assert_eq!(strings(&df, "OrdStatus")[1], Some("Filled".into()));
    assert_eq!(
        strings(&df, "direction"),
        [Some("out".into()), Some("in".into()), Some("in".into())]
    );
    assert_eq!(
        strings(&df, "session")[0],
        Some("FIX.4.4:BUYSIDE->BROKERX".into())
    );
    assert_eq!(
        strings(&df, "prefix")[0],
        Some("20261002-13:30:00.251 OUT FIX.4.4:BUYSIDE->BROKERX".into())
    );
    assert_eq!(df.column("Price").unwrap().dtype(), &DataType::Float64);
    assert_eq!(
        df.column("LastPx").unwrap().f64().unwrap().get(1),
        Some(101.2)
    );
    assert_eq!(df.column("MsgSeqNum").unwrap().dtype(), &DataType::Int64);
    for time in ["SendingTime", "TransactTime"] {
        assert_eq!(
            df.column(time).unwrap().dtype(),
            &DataType::Datetime(TimeUnit::Nanoseconds, Some(TimeZone::UTC)),
            "{time}"
        );
    }
    assert_eq!(strings(&df, "PartyID")[1], Some("BUYER".into()));
    let rest = df.column("PartyID_rest").unwrap().list().unwrap();
    let rest = rest.get_as_series(1).unwrap();
    assert_eq!(rest.str().unwrap().get(0), Some("SELLER"));
    assert_eq!(
        strings(&df, "9001")[0],
        Some("VWAP".into()),
        "a tag no dictionary names"
    );
    let ok: Vec<_> = df
        .column("checksum_ok")
        .unwrap()
        .bool()
        .unwrap()
        .iter()
        .collect();
    assert_eq!(ok, [Some(true), Some(true), Some(false)]);

    let state = app.data_table_state.as_ref().unwrap();
    let notes: Vec<String> = state.notes().into_iter().map(|n| n.summary).collect();
    assert!(
        notes.iter().any(|n| n.contains("no FIX message")),
        "{notes:?}"
    );
    assert!(notes.iter().any(|n| n.contains("CheckSum")), "{notes:?}");
    let detail = state.format_detail().expect("a FIX tab");
    assert_eq!(detail.tab, "FIX");
    assert!(
        detail.lines[0].contains("FIX.4.4 (2)"),
        "{:?}",
        detail.lines
    );
    assert!(
        detail.list.iter().any(|(k, _)| k == "MsgType"),
        "each column's tag is listed"
    );
    info_opens_on(&mut app, InfoTab::Schema);
}

#[test]
fn a_fix_dictionary_names_a_counterpartys_tags() {
    let dir = scratch("fix-dict");
    let path = dir.join("session.log");
    std::fs::write(&path, fix_log()).unwrap();
    let dict = dir.join("broker.toml");
    std::fs::write(
        &dict,
        "name = \"acme.fix.buyside\"\nkind = \"fix\"\nmatch = { sender = \"BUYSIDE\" }\n\
         tags = { 9001 = { name = \"AlgoName\", enum = { VWAP = \"VolumeWeighted\" } } }\n",
    )
    .unwrap();
    let (app, _rx) = open_with(
        path.clone(),
        OpenOptions {
            fix_dict: Some(dict),
            ..options_in(&dir)
        },
    );
    assert_eq!(app.error_message(), None);
    let df = frame(&app);
    assert_eq!(strings(&df, "AlgoName")[0], Some("VolumeWeighted".into()));
    assert_eq!(strings(&df, "AlgoName_code")[0], Some("VWAP".into()));

    let wrong = dir.join("wrong.toml");
    std::fs::write(&wrong, "name = \"a.b\"\nkind = \"fix\"\ntags = { x = 1 }\n").unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let message = super::pump_open_until_error(
        &mut app,
        &rx,
        vec![path],
        OpenOptions {
            fix_dict: Some(wrong),
            ..options_in(&dir)
        },
    )
    .expect("a dictionary that does not parse is an error");
    assert!(message.contains("tags.x"), "{message}");
}

#[test]
fn a_fix_log_piped_in_or_named_with_format() {
    let dir = scratch("fix-named");
    let path = dir.join("capture.bin");
    // SOH-delimited with no newlines between messages, as a raw capture is.
    let text = [
        fix("FIX.4.2", &[(35, "A"), (98, "0"), (108, "30")], "\x01"),
        fix("FIX.4.2", &[(35, "0")], "\x01"),
    ]
    .concat();
    std::fs::write(&path, text).unwrap();
    let (app, _rx) = open_with(
        path,
        OpenOptions {
            format: Some(datui::FileFormat::Fix),
            ..options_in(&dir)
        },
    );
    assert_eq!(app.error_message(), None);
    let df = frame(&app);
    assert_eq!(
        strings(&df, "MsgType"),
        [Some("Logon".into()), Some("Heartbeat".into())]
    );
    assert!(
        !names(&df).contains(&"prefix".to_string()),
        "no prefix to show"
    );
}

const SDF: &str = "aspirin
  RDKit          2D

 13 13  0  0  0  0  0  0  0  0999 V2000
    1.2990   -0.7500    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0
M  END
> <ID>
CHEMBL25

> <MW>
180.16

> <Solubility> (1)
-1.72

$$$$
caffeine
  RDKit          2D

 14 15  0  0  0  0  0  0  0  0999 V2000
M  END
>  <ID>
CHEMBL113

> <MW>
194.19

> <Class>
high

> <Notes>
a stimulant
of the methylxanthine class

$$$$
";

#[test]
fn an_sdf_file_opens_as_a_row_per_record() {
    let dir = scratch("sdf");
    let path = dir.join("drugs.sdf");
    std::fs::write(&path, SDF).unwrap();
    let (mut app, _rx) = open_with(path, options_in(&dir));
    assert_eq!(app.error_message(), None);
    let df = frame(&app);
    assert_eq!(
        names(&df),
        [
            "name",
            "atoms",
            "bonds",
            "ID",
            "MW",
            "Solubility",
            "Class",
            "Notes"
        ]
    );
    assert_eq!(
        strings(&df, "name"),
        [Some("aspirin".into()), Some("caffeine".into())]
    );
    assert_eq!(df.column("atoms").unwrap().u32().unwrap().get(1), Some(14));
    assert_eq!(df.column("MW").unwrap().dtype(), &DataType::Float64);
    assert_eq!(
        df.column("Solubility").unwrap().f64().unwrap().get(1),
        None,
        "sparse"
    );
    assert_eq!(
        strings(&df, "Notes")[1],
        Some("a stimulant\nof the methylxanthine class".into())
    );
    let detail = app
        .data_table_state
        .as_ref()
        .unwrap()
        .format_detail()
        .unwrap()
        .clone();
    assert_eq!(detail.tab, "SDF");
    assert!(detail.lines[0].contains("2 records"));
    info_opens_on(&mut app, InfoTab::Schema);
}

#[test]
fn a_gzipped_sdf_file_streams() {
    let dir = scratch("sdf-gz");
    let path = dir.join("drugs.sdf.gz");
    gzip(&path, SDF.repeat(50).as_bytes());
    let (app, _rx) = open_with(path, options_in(&dir));
    assert_eq!(app.error_message(), None);
    assert_eq!(frame(&app).height(), 100);
}

/// An SDF file at an HTTP URL is downloaded and read like a local one.
#[cfg(feature = "http")]
#[test]
fn an_sdf_file_opens_from_a_url() {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use std::io::Read;
    let mut body = Vec::new();
    {
        let mut gz = flate2::write::GzEncoder::new(&mut body, flate2::Compression::default());
        gz.write_all(SDF.as_bytes()).unwrap();
        gz.finish().unwrap();
    }
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/drugs.sdf.gz", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") && stream.read(&mut byte).unwrap_or(0) == 1 {
                head.push(byte[0]);
            }
            let get = head.starts_with(b"GET");
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/gzip\r\nContent-Length: {}\r\n\
                 Connection: close\r\n\r\n",
                body.len()
            );
            if get {
                let _ = stream.write_all(&body);
            }
        }
    });
    let dir = scratch("sdf-url");
    std::fs::create_dir_all(dir.join("tmp")).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let mut next = Some(AppEvent::Open(vec![PathBuf::from(&url)], options_in(&dir)));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while app.data_table_state.is_none() && app.error_message().is_none() {
        assert!(std::time::Instant::now() < deadline, "the open finishes");
        if app.awaiting_download_confirmation() {
            next = Some(AppEvent::Key(KeyEvent::new(
                KeyCode::Enter,
                KeyModifiers::NONE,
            )));
        }
        let Some(event) = next.take().or_else(|| next_event(&app, &rx)) else {
            continue;
        };
        next = app.event(&event);
    }
    while let Some(event) = next.take().or_else(|| next_event(&app, &rx)) {
        next = app.event(&event);
    }
    assert_eq!(app.error_message(), None);
    assert_eq!(frame(&app).height(), 2);
}
