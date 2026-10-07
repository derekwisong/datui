use super::*;
use std::sync::atomic::Ordering;

/// Every way a GPS log is refused names the file, in the one shape.
#[test]
fn errors_name_the_file() {
    use crate::formats::readers::bad_input::{assert_shape, each_names_its_file, opening};
    each_names_its_file(
        FileFormat::Gpx,
        &[
            (
                "kml.gpx",
                b"<?xml version=\"1.0\"?><kml></kml>",
                "first element is <kml>",
            ),
            ("empty.gpx", b"<?xml version=\"1.0\"?>", "no <gpx> element"),
        ],
    );
    each_names_its_file(
        FileFormat::Nmea,
        &[("words.nmea", b"hello\nthere\n", "No NMEA sentences")],
    );
    let dir = tempfile::tempdir().unwrap();
    let options = OpenOptions {
        table: Some("nope".into()),
        ..Default::default()
    };
    let message = opening(
        dir.path(),
        "a.nmea",
        b"$GPGGA,1,2\n",
        FileFormat::Nmea,
        &options,
    )
    .expect("no such table");
    eprintln!("{message}");
    assert_shape(&message, &dir.path().join("a.nmea"));
    assert!(message.contains("--table names one of"), "{message}");
}

fn options(dir: &Path) -> OpenOptions {
    OpenOptions {
        temp_dir: Some(dir.to_path_buf()),
        ..Default::default()
    }
}

/// Other tables are offered only when one holds rows this one does not show.
#[test]
fn other_tables_only_when_worth_opening() {
    let stats = |types: &[(&str, u64)]| nmea::Stats {
        types: types.iter().map(|(t, n)| (t.to_string(), *n)).collect(),
        ..Default::default()
    };
    let fixes_only = stats(&[("GGA", 3), ("RMC", 3), ("VTG", 3)]);
    assert!(nmea_other_tables(&fixes_only, nmea::Table::Fixes).is_empty());
    assert_eq!(
        nmea_other_tables(&fixes_only, nmea::Table::Gga),
        ["fixes", "RMC 3", "VTG 3", "sentences"]
    );
    let gga_only = stats(&[("GGA", 3)]);
    assert!(nmea_other_tables(&gga_only, nmea::Table::Gga).is_empty());
    let with_gsv = stats(&[("GGA", 3), ("GSV", 9)]);
    assert_eq!(
        nmea_other_tables(&with_gsv, nmea::Table::Fixes),
        ["GGA 3", "GSV 9", "sentences"]
    );
}

/// A GPX file whose second batch brings a new field is two segments, joined with
/// the field null where the first lacks it; the files go when the frame's holder
/// lets them go.
#[test]
fn a_field_found_late_starts_a_segment() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.gpx");
    // The new field comes a few chunks after the first batch is full.
    let mut text = String::from("<gpx><trk><trkseg>");
    for i in 0..gpx::BATCH_ROWS + 5000 {
        text.push_str(&format!("<trkpt lat=\"1\" lon=\"{}\"/>", i % 100));
    }
    text.push_str("<trkpt lat=\"1\" lon=\"2\"><extensions><hr>99</hr></extensions></trkpt>");
    text.push_str("</trkseg></trk></gpx>");
    std::fs::write(&path, text).unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let writer = Writer::default();
    let (converted, _) = convert(
        std::slice::from_ref(&path),
        &path,
        FileFormat::Gpx,
        &options(out_dir.path()),
        &writer,
        &AtomicU64::default(),
    )
    .unwrap();
    assert_eq!(converted.files.len(), 2);
    let df = converted.lf.clone().collect().unwrap();
    assert_eq!(df.height(), gpx::BATCH_ROWS + 5001);
    let hr = df.column("hr").unwrap();
    assert_eq!(hr.dtype(), &DataType::Int64);
    assert_eq!(hr.null_count(), gpx::BATCH_ROWS + 5000);
    drop(converted);
    assert_eq!(std::fs::read_dir(out_dir.path()).unwrap().count(), 0);
}

/// A temp directory named like a glob holds the segments, and they are read
/// from there, not from what its name would match (#625).
#[test]
fn segments_in_a_temp_directory_named_like_a_glob() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.nmea");
    std::fs::write(
        &path,
        "$GPRMC,120000,A,4807.038,N,01131.000,E,1.0,0.0,010124,,,A\n",
    )
    .unwrap();
    let temp = dir.path().join("t[1]");
    std::fs::create_dir(&temp).unwrap();
    std::fs::create_dir(dir.path().join("t1")).unwrap();
    let (converted, _) = convert(
        std::slice::from_ref(&path),
        &path,
        FileFormat::Nmea,
        &options(&temp),
        &Writer::default(),
        &AtomicU64::default(),
    )
    .unwrap();
    assert_eq!(converted.lf.clone().collect().unwrap().height(), 1);
}

#[test]
fn an_nmea_log_compressed_and_its_tables() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.nmea.gz");
    let log = "$GPRMC,120000,A,4807.038,N,01131.000,E,1.0,0.0,010124,,,A\n\
                   junk\n$GPGSV,1,1,01,07,40,083,46\n";
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    std::io::Write::write_all(&mut enc, log.as_bytes()).unwrap();
    std::fs::write(&path, enc.finish().unwrap()).unwrap();
    let writer = Writer::default();
    let read = AtomicU64::default();
    let (converted, _) = convert(
        std::slice::from_ref(&path),
        &path,
        FileFormat::Nmea,
        &options(dir.path()),
        &writer,
        &read,
    )
    .unwrap();
    assert_eq!(converted.lf.clone().collect().unwrap().height(), 1);
    assert_eq!(
        read.load(Ordering::Relaxed),
        std::fs::metadata(&path).unwrap().len(),
        "the bytes read are counted as stored, compressed"
    );
    let summaries: Vec<_> = converted.notes.iter().map(|n| n.summary.as_str()).collect();
    assert!(
        summaries[0].starts_with("1 line left out: not NMEA"),
        "{summaries:?}"
    );
    assert_eq!(summaries.len(), 1, "{summaries:?}");
    assert_eq!(
        converted.other_tables,
        ["RMC 1", "GSV 1", "sentences"],
        "a GSV the fixes do not show"
    );
    let (gsv, _) = convert(
        std::slice::from_ref(&path),
        &path,
        FileFormat::Nmea,
        &OpenOptions {
            table: Some("gsv".into()),
            ..options(dir.path())
        },
        &writer,
        &AtomicU64::default(),
    )
    .unwrap();
    assert_eq!(gsv.lf.collect().unwrap().height(), 1);
    assert_eq!(gsv.other_tables, ["fixes", "RMC 1", "sentences"]);
    let none = convert(
        std::slice::from_ref(&path),
        &path,
        FileFormat::Nmea,
        &OpenOptions {
            table: Some("nope".into()),
            ..options(dir.path())
        },
        &writer,
        &AtomicU64::default(),
    );
    assert!(none.err().unwrap().to_string().contains("GSV"));
    let text = dir.path().join("t.txt");
    std::fs::write(&text, "hello\nworld\n").unwrap();
    assert!(
        convert(
            std::slice::from_ref(&text),
            &text,
            FileFormat::Nmea,
            &options(dir.path()),
            &writer,
            &AtomicU64::default()
        )
        .is_err()
    );
}

/// Several logs are one table with a `file` column first; a field one GPX file has
/// and another lacks is null in the other, typed where the files agree and stacked
/// as text where they do not; notes count across the files; the files go with the
/// frame.
#[test]
fn several_logs_are_one_table_with_a_file_column() {
    let dir = tempfile::tempdir().unwrap();
    let gpx = |name: &str, points: &str| {
        let path = dir.path().join(name);
        std::fs::write(
            &path,
            format!("<gpx><trk><trkseg>{points}</trkseg></trk></gpx>"),
        )
        .unwrap();
        path
    };
    let a = gpx(
        "a.gpx",
        "<trkpt lat=\"1\" lon=\"2\"><extensions><hr>90</hr><cad>7</cad></extensions></trkpt>\
             <trkpt lat=\"1\" lon=\"3\"><time>yesterday</time></trkpt>",
    );
    let b = gpx(
        "b.gpx",
        "<trkpt lat=\"5\" lon=\"6\"><extensions><hr>91</hr><cad>fast</cad><pwr>200</pwr></extensions></trkpt>",
    );
    let out = tempfile::tempdir().unwrap();
    let read = AtomicU64::default();
    let (converted, _) = convert(
        &[a.clone(), b.clone()],
        dir.path(),
        FileFormat::Gpx,
        &options(out.path()),
        &Writer::default(),
        &read,
    )
    .unwrap();
    let df = converted.lf.clone().collect().unwrap();
    assert_eq!(df.get_column_names()[0].as_str(), "file");
    let files: Vec<Option<&str>> = df.column("file").unwrap().str().unwrap().iter().collect();
    assert_eq!(files, [Some("a.gpx"), Some("a.gpx"), Some("b.gpx")]);
    let hr = df.column("hr").unwrap();
    assert_eq!(hr.dtype(), &DataType::Int64, "a number in both files");
    assert_eq!(hr.null_count(), 1);
    assert_eq!(df.column("cad").unwrap().dtype(), &DataType::String);
    assert_eq!(df.column("pwr").unwrap().null_count(), 2, "only b has it");
    assert_eq!(
        read.load(Ordering::Relaxed),
        std::fs::metadata(&a).unwrap().len() + std::fs::metadata(&b).unwrap().len()
    );
    let notes: Vec<_> = converted
        .notes
        .iter()
        .map(|n| (n.summary.as_str(), n.scope.as_str()))
        .collect();
    assert_eq!(
        notes,
        [("1 time left empty: not ISO 8601", "of 3 points in 2 files")]
    );
    assert!(converted.files.len() >= 2);
    drop(converted);
    assert_eq!(std::fs::read_dir(out.path()).unwrap().count(), 0);
}

/// NMEA logs stack with their counts added: one without a date says so of itself
/// alone, and a table the fixes do not show is offered from either.
#[test]
fn nmea_logs_stack_and_count_together() {
    let dir = tempfile::tempdir().unwrap();
    let dated = dir.path().join("a.nmea");
    std::fs::write(
        &dated,
        "$GPRMC,120000,A,4807.038,N,01131.000,E,1.0,0.0,010124,,,A\n",
    )
    .unwrap();
    let undated = dir.path().join("b.nmea");
    std::fs::write(
            &undated,
            "$GPGGA,120001,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,\n$GPGSV,1,1,01,07,40,083,46\n",
        )
        .unwrap();
    let (converted, _) = convert(
        &[dated, undated],
        dir.path(),
        FileFormat::Nmea,
        &options(dir.path()),
        &Writer::default(),
        &AtomicU64::default(),
    )
    .unwrap();
    let df = converted.lf.clone().collect().unwrap();
    assert_eq!(df.height(), 2);
    assert_eq!(df.get_column_names()[0].as_str(), "file");
    let summaries: Vec<_> = converted.notes.iter().map(|n| n.summary.as_str()).collect();
    assert_eq!(
        summaries,
        [format!(
            "1 of 2 logs without an RMC or ZDA date {} time empty",
            crate::glyphs::get().middot
        )]
    );
    assert_eq!(
        converted.other_tables,
        ["GGA 1", "RMC 1", "GSV 1", "sentences"]
    );
}

/// The GPS tab: what was read, when and where, from the rows as they were written;
/// several files' extents are one.
#[test]
fn the_tab_says_when_and_where() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.nmea");
    std::fs::write(
        &path,
        "$GPRMC,120000,A,4807.038,N,01131.000,E,1.0,0.0,010124,,,A\n\
             $GPRMC,121005,A,4808.000,N,01130.000,E,1.0,0.0,010124,,,A\n\
             $GPGSV,1,1,01,07,40,083,46\n",
    )
    .unwrap();
    let (_, detail) = convert(
        std::slice::from_ref(&path),
        &path,
        FileFormat::Nmea,
        &options(dir.path()),
        &Writer::default(),
        &AtomicU64::default(),
    )
    .unwrap();
    assert_eq!(detail.tab, "GPS");
    assert_eq!(
        detail.lines[0],
        format!(
            "2 rows of fixes {} 3 sentences of 3 lines",
            crate::glyphs::get().middot
        )
    );
    assert_eq!(
        detail.lines[1],
        "Time: 2024-01-01 12:00:00 to 12:10:05 UTC (10:05.000)"
    );
    assert!(
        detail.lines[2]
            .starts_with("Bounds: latitude 48.11730 to 48.13333, longitude 11.50000 to 11.51667"),
        "{:?}",
        detail.lines
    );
    assert_eq!(detail.list_title, "Sentences");
    assert_eq!(detail.list.len(), 2);

    let gpx = dir.path().join("t.gpx");
    std::fs::write(
            &gpx,
            "<gpx><wpt lat=\"1\" lon=\"2\"/><trk><trkseg><trkpt lat=\"-3\" lon=\"4\"><time>2024-01-01T00:00:00Z</time></trkpt></trkseg></trk></gpx>",
        )
        .unwrap();
    let (_, detail) = convert(
        &[gpx.clone(), gpx],
        dir.path(),
        FileFormat::Gpx,
        &options(dir.path()),
        &Writer::default(),
        &AtomicU64::default(),
    )
    .unwrap();
    let middot = crate::glyphs::get().middot;
    assert_eq!(
        detail.lines[0],
        format!("4 points {middot} 2 tracks {middot} 0 routes {middot} 2 waypoints")
    );
    assert_eq!(detail.lines[1], "2 files");
    assert!(
        detail
            .lines
            .iter()
            .any(|l| l == "Bounds: latitude -3.00000 to 1.00000, longitude 2.00000 to 4.00000"),
        "{:?}",
        detail.lines
    );
}
