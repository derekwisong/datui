use super::*;
use polars::prelude::IntoLazy;

/// An open routes a remote file as `FileFormat::bucket_object` and `http_file` say,
/// which the format table in `docs/formats/index.md` and the home screen's marker
/// read: read in place, or downloaded first. A bucket's Arrow is listed first, and read in place
/// unless the listing finds a stream; a compressed object is downloaded.
#[cfg(all(feature = "http", feature = "cloud"))]
#[test]
fn remote_files_are_routed_as_their_format_says() {
    use crate::{FileFormat, RemoteRead, Stored};
    let in_place = |url: &str, stream: bool| {
        let path = Path::new(url);
        crate::cloud::remote_model::model_format(path, None).is_some()
            || match remote_download(&source::input_source(path), &OpenOptions::default()) {
                None => true,
                Some(PendingDownload::Arrow { .. }) => {
                    let object = crate::cloud::cloud_arrow::Object {
                        url: url.to_string(),
                        size: 10,
                        stream,
                    };
                    crate::cloud::cloud_arrow::in_place(&[object]).is_some()
                }
                Some(_) => false,
            }
    };
    let mut seen = Vec::new();
    for ext in [
        "parquet",
        "csv",
        "tsv",
        "psv",
        "json",
        "jsonl",
        "arrow",
        "avro",
        "orc",
        "xlsx",
        "safetensors",
        "gguf",
        "nmea",
        "gpx",
        "wav",
        "mid",
        "db",
        "vcd",
        "sdf",
        "log",
        "npy",
        "elf",
        "ulg",
    ] {
        let format = FileFormat::from_extension(ext).expect(ext);
        seen.push(format);
        let https = format!("https://example.com/d/x.{ext}");
        assert_eq!(
            in_place(&https, false),
            format.http_file() == RemoteRead::InPlace,
            "{https}"
        );
        for url in [format!("s3://b/d/x.{ext}"), format!("gs://b/d/x.{ext}")] {
            let said = |stored| format.bucket_object(stored) == RemoteRead::InPlace;
            assert_eq!(in_place(&url, false), said(Stored::Plain), "{url}");
            if format == FileFormat::Arrow {
                assert_eq!(in_place(&url, true), said(Stored::Stream), "{url}: stream");
            }
            let gz = format!("{url}.gz");
            let compressed = Stored::Compressed { in_memory: false };
            assert_eq!(in_place(&gz, false), said(compressed), "{gz}");
        }
    }
    // A FIX log has no extension, so no URL names it: it is found by its content
    // once downloaded, as `http_file` and `bucket_object` say for it.
    assert_eq!(FileFormat::Fix.http_file(), RemoteRead::Downloaded);
    assert_eq!(
        FileFormat::Fix.bucket_object(Stored::Plain),
        RemoteRead::Downloaded
    );
    seen.push(FileFormat::Fix);
    // A DataFlash log is a `.bin`, found by its first bytes, as a FIX log is.
    assert_eq!(FileFormat::Dataflash.http_file(), RemoteRead::Downloaded);
    assert_eq!(
        FileFormat::Dataflash.bucket_object(Stored::Plain),
        RemoteRead::Downloaded
    );
    seen.push(FileFormat::Dataflash);
    // So is a candump log, whatever its name.
    assert_eq!(FileFormat::Candump.http_file(), RemoteRead::Downloaded);
    assert_eq!(
        FileFormat::Candump.bucket_object(Stored::Plain),
        RemoteRead::Downloaded
    );
    seen.push(FileFormat::Candump);
    // And journal JSON, from a file or a pipe.
    assert_eq!(FileFormat::Journal.http_file(), RemoteRead::Downloaded);
    assert_eq!(
        FileFormat::Journal.bucket_object(Stored::Plain),
        RemoteRead::Downloaded
    );
    seen.push(FileFormat::Journal);
    for f in FileFormat::ALL {
        assert!(seen.contains(&f), "{} is checked", f.name());
    }
}

fn frame() -> LazyFrame {
    polars::df!("a" => [1i32, 2, 3]).unwrap().lazy()
}

fn state() -> Box<DataTableState> {
    Box::new(DataTableState::from_lazyframe(frame(), &OpenOptions::default()).unwrap())
}

fn request(path: &str) -> OpenRequest {
    OpenRequest {
        paths: vec![PathBuf::from(path)],
        options: OpenOptions::default(),
        size: 7,
        recent: Some(PathBuf::from(path)),
        shown: None,
        warn_in_memory_above: None,
    }
}

#[cfg(any(feature = "http", feature = "cloud"))]
fn jobs() -> Jobs {
    Jobs::new(std::sync::mpsc::channel().0)
}

/// `answered`, with the job owner the download question needs.
fn answer(loader: &mut Loader, id: LoadId, answer: LoadAnswer) -> Step {
    loader.answered(
        id,
        answer,
        #[cfg(any(feature = "http", feature = "cloud"))]
        &jobs(),
    )
}

fn scanned(path: &str) -> LoadAnswer {
    LoadAnswer::Scanned {
        lf: Box::new(frame()),
        path: Some(PathBuf::from(path)),
        options: OpenOptions::default(),
    }
}

fn schema_read(path: &str) -> LoadAnswer {
    LoadAnswer::SchemaRead {
        state: state(),
        path: Some(PathBuf::from(path)),
        options: OpenOptions::default(),
        debug_label: None,
    }
}

/// A local file goes scan, schema, install, first rows, done; each step says what to
/// run, and the screen says each phase in turn.
#[test]
fn a_local_open_runs_its_phases_in_order() {
    let mut loader = Loader::default();
    let step = loader.open(request("data.parquet"));
    let id = loader.id().expect("a load");
    assert!(matches!(
        step,
        Step::Scan { ref paths, display: None, status: "Scanning input...", .. }
            if paths == &[PathBuf::from("data.parquet")]
    ));
    let load = loader.current().unwrap();
    assert_eq!(load.phase().label(), ("Scanning input", 10));
    assert_eq!(load.path(), Some(Path::new("data.parquet")));
    assert_eq!(load.size(), 7);
    assert!(loader.awaiting_dataset() && loader.waits());

    let Step::ReadSchema { progress, .. } = answer(&mut loader, id, scanned("data.parquet")) else {
        panic!("the scan goes on to the schema");
    };
    assert!(Arc::ptr_eq(&progress, loader.progress().unwrap()));
    assert_eq!(
        loader.current().unwrap().phase().label(),
        ("Reading schema", 40)
    );

    let Step::Install(loaded) = answer(&mut loader, id, schema_read("data.parquet")) else {
        panic!("the schema goes on to the install");
    };
    assert_eq!(loaded.path.as_deref(), Some(Path::new("data.parquet")));
    assert_eq!(
        loaded.paths.as_deref(),
        Some(&[PathBuf::from("data.parquet")][..])
    );
    assert_eq!(loaded.recent.as_deref(), Some(Path::new("data.parquet")));
    assert!(!loaded.from_home);
    assert!(
        Arc::ptr_eq(&loaded.footers, &progress),
        "the dataset takes the load's footer counter"
    );
    assert!(!loader.awaiting_dataset(), "the dataset is up");
    assert!(!loader.waits(), "the read of its rows holds the keys");
    assert!(loader.progress().is_none(), "the counter is the dataset's");
    assert_eq!(
        loader.current().unwrap().phase().label(),
        ("Loading buffer", 70)
    );

    loader.first_rows_settled();
    assert!(loader.current().is_none(), "the open is done");
}

/// Arrow IPC streams found by the scan are converted, counting their bytes, and the
/// IPC file they become is scanned and held by the dataset, named by what was asked
/// for; a failure names that too, and putting the load down lets the file go.
#[test]
fn streams_are_converted_then_scanned() {
    let dir = tempfile::tempdir().unwrap();
    let mut loader = Loader::default();
    let _ = loader.open(request("cache.arrow"));
    let id = loader.id().unwrap();
    let Step::Convert {
        files, path, read, ..
    } = answer(
        &mut loader,
        id,
        LoadAnswer::Convert {
            what: Conversion::Streams,
            files: vec![PathBuf::from("cache.arrow")],
            bytes: 200,
            path: Some(PathBuf::from("cache.arrow")),
            options: OpenOptions::default(),
        },
    )
    else {
        panic!("the streams are converted");
    };
    assert_eq!(files, [PathBuf::from("cache.arrow")]);
    assert_eq!(path.as_deref(), Some(Path::new("cache.arrow")));
    assert_eq!(
        loader.current().unwrap().phase().label(),
        ("Converting Arrow stream", 10)
    );
    read.store(100, Ordering::Relaxed);
    assert_eq!(
        loader.current().unwrap().phase().label(),
        ("Converting Arrow stream", 20),
        "the bar moves with the bytes read"
    );
    assert!(loader.waits());

    let converted =
        TempDownload::keep(TempDownload::create(Some(dir.path()), Some("arrow")).unwrap());
    let temp = converted.path().to_path_buf();
    let Step::Scan {
        paths,
        options,
        display,
        ..
    } = answer(
        &mut loader,
        id,
        LoadAnswer::Converted {
            converted: Converted::Streams {
                file: converted,
                parts: Vec::new(),
            },
            path: Some(PathBuf::from("cache.arrow")),
            options: OpenOptions::default(),
        },
    )
    else {
        panic!("the IPC file is scanned");
    };
    assert_eq!(paths, std::slice::from_ref(&temp));
    assert_eq!(options.format, Some(FileFormat::Arrow));
    assert_eq!(display.as_deref(), Some(Path::new("cache.arrow")));
    assert!(
        matches!(
            answer(
                &mut loader,
                id,
                LoadAnswer::Convert {
                    what: Conversion::Streams,
                    files: vec![temp.clone()],
                    bytes: 1,
                    path: None,
                    options: OpenOptions::default(),
                },
            ),
            Step::Nothing
        ),
        "converted once"
    );
    let Step::ReadSchema { made, .. } = answer(&mut loader, id, scanned("cache.arrow")) else {
        panic!("the schema is read");
    };
    assert!(made.download.is_none(), "nothing was downloaded");
    assert_eq!(
        made.converted
            .iter()
            .map(|d| d.path().to_path_buf())
            .collect::<Vec<_>>(),
        std::slice::from_ref(&temp),
        "the dataset holds the converted file"
    );
    drop(made);
    let Step::Failed(failed) = loader.failed(id, &format!("could not read {}", temp.display()))
    else {
        panic!("the open fails");
    };
    assert_eq!(failed.message, "could not read cache.arrow");
    assert!(!temp.exists(), "the retired load let the file go");
}

/// An answer from a load that is no longer in flight, or from a phase it has left,
/// changes nothing: no install, no new phase, no title.
#[test]
fn an_old_load_s_answers_change_nothing() {
    let mut loader = Loader::default();
    let _ = loader.open(request("first.csv"));
    let first = loader.id().unwrap();
    assert!(loader.make_way().is_some(), "the first is doing work");
    let _ = loader.open(request("second.csv"));
    let second = loader.id().unwrap();
    assert_ne!(first, second);

    assert!(matches!(
        answer(&mut loader, first, scanned("first.csv")),
        Step::Nothing
    ));
    assert!(matches!(
        answer(&mut loader, first, schema_read("first.csv")),
        Step::Nothing
    ));
    assert!(matches!(loader.failed(first, "gone"), Step::Nothing));
    assert_eq!(loader.id(), Some(second));
    let load = loader.current().unwrap();
    assert_eq!(load.path(), Some(Path::new("second.csv")));
    assert_eq!(load.phase().label(), ("Scanning input", 10));

    // An answer for a phase the load is not in is dropped too: a schema before the
    // scan has answered.
    assert!(matches!(
        answer(&mut loader, second, schema_read("second.csv")),
        Step::Nothing
    ));
    assert!(loader.awaiting_dataset());
}

/// A conversion put down mid-way is told to stop through its writer, and its file,
/// answered late or for a phase the load is not in, is dropped and so removed.
#[test]
fn a_converted_file_nobody_wants_is_removed() {
    let dir = tempfile::tempdir().unwrap();
    let converted = || {
        let file =
            TempDownload::keep(TempDownload::create(Some(dir.path()), Some("arrow")).unwrap());
        let path = file.path().to_path_buf();
        (
            LoadAnswer::Converted {
                converted: Converted::Streams {
                    file,
                    parts: Vec::new(),
                },
                path: Some(PathBuf::from("cache.arrow")),
                options: OpenOptions::default(),
            },
            path,
        )
    };
    let mut loader = Loader::default();
    let _ = loader.open(request("cache.arrow"));
    let first = loader.id().unwrap();

    let (early, early_path) = converted();
    assert!(matches!(answer(&mut loader, first, early), Step::Nothing));
    assert!(!early_path.exists(), "not converting yet");

    let Step::Convert { writer, .. } = answer(
        &mut loader,
        first,
        LoadAnswer::Convert {
            what: Conversion::Streams,
            files: vec![PathBuf::from("cache.arrow")],
            bytes: 1,
            path: Some(PathBuf::from("cache.arrow")),
            options: OpenOptions::default(),
        },
    ) else {
        panic!("the streams are converted");
    };
    assert!(!writer.stopped());
    assert!(loader.make_way().is_some(), "the conversion is work");
    assert!(writer.stopped(), "Ctrl+O or another open stops it");

    let _ = loader.open(request("other.csv"));
    let (late, late_path) = converted();
    assert!(matches!(answer(&mut loader, first, late), Step::Nothing));
    assert!(!late_path.exists(), "the replaced load's file");
    assert_eq!(
        loader.current().unwrap().path(),
        Some(Path::new("other.csv"))
    );
}

/// Opening something new replaces a load doing work, and stops it: its footer pass
/// and its download read the stop flag.
#[test]
fn a_superseding_open_stops_the_one_it_replaces() {
    let mut loader = Loader::default();
    let _ = loader.open(request("big"));
    let id = loader.id().unwrap();
    let Step::ReadSchema { progress, .. } = answer(&mut loader, id, scanned("big")) else {
        panic!("reading the schema");
    };
    let retired = loader.make_way().expect("the load is replaced");
    assert!(!retired.asking);
    assert!(progress.is_cancelled(), "its pass stops issuing reads");
    assert!(loader.current().is_none());
}

/// The frame between a key and its open, and the look at named paths, are carried on
/// by the open they lead to: one load, one identity, and the origin it was asked from.
#[test]
fn an_open_carries_on_the_load_that_announced_it() {
    let mut loader = Loader::default();
    let announced = loader.announce(true, "Scanning input".to_string(), 10);
    loader.name(PathBuf::from("chosen.csv"));
    assert_eq!(
        loader.current().unwrap().path(),
        Some(Path::new("chosen.csv"))
    );
    assert!(loader.waits(), "the keys wait from the key that asked");
    assert!(loader.make_way().is_none(), "nothing to replace yet");
    let _ = loader.open(request("chosen.csv"));
    assert_eq!(loader.id(), Some(announced));

    let Step::Install(loaded) = ({
        let _ = answer(&mut loader, announced, scanned("chosen.csv"));
        answer(&mut loader, announced, schema_read("chosen.csv"))
    }) else {
        panic!("installs");
    };
    assert!(loaded.from_home, "a failure would be reported at home");

    // A look at named paths, then the directory it found, then the open of it.
    let mut loader = Loader::default();
    let look = loader.look_at_paths();
    assert!(loader.looking_at_paths(look));
    assert!(loader.make_way().is_none());
    let directory = loader.look_at_directory(PathBuf::from("dir"));
    assert_eq!(directory, look);
    assert!(!loader.looking_at_paths(look), "that look is answered");
    assert!(loader.looking_at_directory(look));
    assert_eq!(
        loader.current().unwrap().phase().label(),
        (crate::App::LOOKING_AT_A_DIRECTORY, 5)
    );
    let _ = loader.open(request("dir"));
    assert_eq!(loader.id(), Some(look));
    assert!(
        !loader.looking_at_directory(look),
        "nor is a late look taken"
    );
}

/// A worker's failure ends its load with the reason and where it was asked from, and
/// retires it; one from a load already installed, or gone, is not the open's.
#[test]
fn a_failed_phase_retires_its_load() {
    let mut loader = Loader::default();
    let id = loader.announce(true, "Scanning input".to_string(), 10);
    let _ = loader.open(request("broken.parquet"));
    let Step::ReadSchema { progress, .. } = answer(&mut loader, id, scanned("broken.parquet"))
    else {
        panic!("reading the schema");
    };
    let Step::Failed(failed) = loader.failed(id, "not parquet") else {
        panic!("the open fails");
    };
    assert_eq!(failed.message, "not parquet");
    assert!(failed.from_home);
    assert!(loader.current().is_none());
    assert!(progress.is_cancelled());
    assert!(matches!(loader.failed(id, "again"), Step::Nothing));

    let id = {
        let _ = loader.open(request("good.parquet"));
        loader.id().unwrap()
    };
    let _ = answer(&mut loader, id, scanned("good.parquet"));
    let Step::Install(loaded) = answer(&mut loader, id, schema_read("good.parquet")) else {
        panic!("installs");
    };
    assert!(
        matches!(loader.failed(id, "the rows"), Step::Nothing),
        "the rows' failure is the table's"
    );
    assert!(!loaded.footers.is_cancelled());
    assert!(
        loader.retire().is_some(),
        "going home puts down the read of the first rows"
    );
    assert!(
        !loaded.footers.is_cancelled(),
        "but not the dataset's footer pass"
    );
}

/// A file read whole into memory past the size that asks is put to the user before
/// its scan; agreeing scans it, and one in memory under the size, or read lazily,
/// is scanned without asking.
#[test]
fn a_large_read_into_memory_is_asked_about_first() {
    let dir = tempfile::tempdir().unwrap();
    let json = dir.path().join("big.json");
    std::fs::write(&json, "[{\"a\": 1}]").unwrap();
    let csv = dir.path().join("big.csv");
    std::fs::write(&csv, "a\n1\n").unwrap();
    let open = |loader: &mut Loader, path: &Path, limit: u64| {
        loader.open(OpenRequest {
            warn_in_memory_above: Some(limit),
            ..request(&path.to_string_lossy())
        })
    };
    let mut loader = Loader::default();
    let Step::AskRead(read) = open(&mut loader, &json, 4) else {
        panic!("a JSON file past the size is asked about");
    };
    assert_eq!(read.format, FileFormat::Json);
    assert_eq!((read.bytes, read.files), (10, 1));
    assert!(loader.asking() && loader.awaiting_dataset() && !loader.waits());
    assert!(matches!(loader.confirmed(), Step::Scan { ref paths, .. } if paths[0] == json));
    assert!(!loader.asking());
    let mut loader = Loader::default();
    assert!(matches!(open(&mut loader, &json, 10), Step::Scan { .. }));
    let mut loader = Loader::default();
    assert!(matches!(open(&mut loader, &csv, 0), Step::Scan { .. }));
    let mut loader = Loader::default();
    let _ = open(&mut loader, &json, 0);
    assert!(loader.retire().is_some_and(|retired| retired.asking));
}

/// Journal JSON is read whole too, and asked about by what its bytes say, named or
/// not.
#[test]
fn a_large_journal_is_asked_about_first() {
    let dir = tempfile::tempdir().unwrap();
    let entry = "{\"__CURSOR\":\"s=1\",\"__REALTIME_TIMESTAMP\":\"1\",\"MESSAGE\":\"m\"}\n";
    for name in ["journal", "journal.json"] {
        let path = dir.path().join(name);
        std::fs::write(&path, entry).unwrap();
        let mut loader = Loader::default();
        let step = loader.open(OpenRequest {
            warn_in_memory_above: Some(4),
            ..request(&path.to_string_lossy())
        });
        let Step::AskRead(read) = step else {
            panic!("{name}: a journal past the size is asked about");
        };
        assert_eq!(read.format, FileFormat::Journal, "{name}");
    }
}

/// A CSV whose footer rows are dropped counts the file in its scan, and says so;
/// a file of another format, or with no footer to drop, scans as ever.
#[test]
fn a_footer_to_drop_says_the_file_is_counted() {
    let footer = |n| OpenOptions {
        skip_tail_rows: Some(n),
        parse_strings: Some(crate::ParseStringsTarget::All),
        ..OpenOptions::default()
    };
    let mut loader = Loader::default();
    let step = loader.open(OpenRequest {
        options: footer(2),
        ..request("vendor.csv")
    });
    assert!(
        matches!(step, Step::Scan { status, .. } if status == COUNTING_FOOTER_STATUS),
        "{:?}",
        loader.current().map(|l| l.phase().label())
    );
    assert_eq!(
        loader.current().unwrap().phase().label(),
        (COUNTING_FOOTER, 10)
    );
    for (path, n) in [("vendor.csv", 0), ("vendor.parquet", 2)] {
        let mut loader = Loader::default();
        let step = loader.open(OpenRequest {
            options: footer(n),
            ..request(path)
        });
        assert!(
            matches!(step, Step::Scan { status, .. } if status != COUNTING_FOOTER_STATUS),
            "{path}"
        );
    }
}

/// Several URLs at once cannot be read, and say so.
#[test]
fn several_urls_end_the_session() {
    let mut loader = Loader::default();
    let step = loader.open(OpenRequest {
        paths: vec![PathBuf::from("s3://a/x.csv"), PathBuf::from("s3://a/y.csv")],
        options: OpenOptions::default(),
        size: 0,
        recent: None,
        shown: None,
        warn_in_memory_above: None,
    });
    assert!(matches!(step, Step::Crash(message) if message.contains("S3")));
    assert!(loader.current().is_none());
}

/// A compressed TSV or PSV is decompressed as a CSV is, told its format so it is
/// split on its own separator (#567); the format comes from `--format`, else the
/// name under the compression suffix.
#[test]
fn a_compressed_delimited_file_is_decompressed_as_its_format() {
    let opts = |format: Option<FileFormat>| OpenOptions {
        format,
        ..OpenOptions::default()
    };
    for (name, format, expected) in [
        ("x.tsv.gz", None, Some(FileFormat::Tsv)),
        ("x.PSV.xz", None, Some(FileFormat::Psv)),
        ("x.csv.zst", None, Some(FileFormat::Csv)),
        ("x.tsv", None, Some(FileFormat::Tsv)),
        ("x.gz", Some(FileFormat::Tsv), Some(FileFormat::Tsv)),
        ("x.csv.gz", Some(FileFormat::Psv), Some(FileFormat::Psv)),
        ("x.json.gz", None, None),
        ("x.gz", None, None),
        ("x.tsv.gz", Some(FileFormat::Parquet), None),
    ] {
        assert_eq!(
            delimited_format(Path::new(name), &opts(format)),
            expected,
            "{name} {format:?}"
        );
    }

    let mut loader = Loader::default();
    assert!(matches!(
        loader.open(request("logs.tsv.gz")),
        Step::Decompress { ref options, .. } if options.format == Some(FileFormat::Tsv)
    ));
}

/// A scan that finds one compressed delimited file, as a directory of one does, hands
/// it to the decompress step under the name the open was asked for (#576); the
/// answer of a load in another phase changes nothing.
#[test]
fn a_compressed_file_the_scan_found_is_decompressed() {
    let compressed = || LoadAnswer::Compressed {
        file: PathBuf::from("dir/data.tsv.gz"),
        path: Some(PathBuf::from("dir")),
        options: OpenOptions {
            format: Some(FileFormat::Tsv),
            ..OpenOptions::default()
        },
    };
    let mut loader = Loader::default();
    assert!(matches!(loader.open(request("dir")), Step::Scan { .. }));
    let id = loader.id().unwrap();
    assert!(matches!(
        answer(&mut loader, id, compressed()),
        Step::Decompress { ref file, ref path, ref options, download: None, .. }
            if file == Path::new("dir/data.tsv.gz")
                && path == Path::new("dir")
                && options.format == Some(FileFormat::Tsv)
    ));
    assert_eq!(
        loader.current().unwrap().phase().label(),
        ("Decompressing", 30)
    );
    assert!(matches!(
        answer(&mut loader, id, compressed()),
        Step::Nothing
    ));
    assert!(matches!(
        answer(&mut loader, id, schema_read("dir")),
        Step::Install(_)
    ));
}

/// A format spec reads local bytes: one object it reads is downloaded first, whatever
/// its name says, and a prefix or a glob goes on to the scan, which refuses it.
#[cfg(feature = "cloud")]
#[test]
fn a_remote_object_a_spec_reads_is_downloaded_first() {
    let asked = [
        OpenOptions {
            spec_name: Some("vendor.feed".into()),
            ..OpenOptions::default()
        },
        OpenOptions {
            spec_file: Some(PathBuf::from("feed.toml")),
            ..OpenOptions::default()
        },
    ];
    let open = |url: &str, options: &OpenOptions| {
        Loader::default().open(OpenRequest {
            options: options.clone(),
            ..request(url)
        })
    };
    for options in &asked {
        for url in [
            "s3://b/day",
            "s3://b/day.parquet",
            "gs://b/day.arrow",
            "abfss://c@acct.dfs.core.windows.net/day.l2",
        ] {
            assert!(
                matches!(open(url, options), Step::Probe(_)),
                "{url} is downloaded"
            );
        }
        for url in ["s3://b/days/", "gs://b/days/*.l2"] {
            assert!(
                matches!(open(url, options), Step::Scan { .. }),
                "{url} goes on to the scan"
            );
        }
    }
    // Without a spec, Parquet is still read in place.
    assert!(matches!(
        open("s3://b/day.parquet", &OpenOptions::default()),
        Step::Scan { .. }
    ));
}

/// `--format` naming a URL: the spec is fetched first, in a phase of its own, then the
/// open goes on as it would have, carrying the spec; a second answer does nothing.
#[test]
fn a_remote_spec_is_fetched_before_the_open_goes_on() {
    let spec = Arc::new(
        crate::formats::Spec::parse(
            "name = \"t.rec\"\n[records]\nfields = [{ name = \"v\", type = \"u1\" }]\n",
            None,
        )
        .unwrap(),
    );
    let options = OpenOptions {
        spec_file: Some(PathBuf::from("https://example.com/t.toml")),
        ..OpenOptions::default()
    };
    let mut loader = Loader::default();
    let Step::FetchSpec { url, options, .. } = loader.open(OpenRequest {
        options,
        ..request("day.rec")
    }) else {
        panic!("the spec is fetched first");
    };
    assert_eq!(url, Path::new("https://example.com/t.toml"));
    assert_eq!(
        loader.current().unwrap().phase().label(),
        ("Reading spec", 5)
    );
    let id = loader.id().unwrap();
    let fetched = || LoadAnswer::SpecFetched {
        spec: spec.clone(),
        options: options.clone(),
    };
    let Step::Scan { paths, options, .. } = answer(&mut loader, id, fetched()) else {
        panic!("the open goes on to its scan");
    };
    assert_eq!(paths, [PathBuf::from("day.rec")]);
    assert!(options.spec_fetched.is_some_and(|s| s.name == "t.rec"));
    assert!(matches!(answer(&mut loader, id, fetched()), Step::Nothing));
}

/// A compressed file a spec reads is decompressed in a phase of its own, which a
/// retired load stops, then its copy's records are read in another; the copy goes
/// to the dataset with the frame, and a second answer for a phase left does nothing.
#[test]
fn a_compressed_file_a_spec_reads_is_decompressed_then_its_records_read() {
    let spec = crate::formats::Spec::parse(
        "name = \"t.rec\"\n[records]\nfields = [{ name = \"v\", type = \"u1\" }]\n",
        None,
    )
    .unwrap();
    let choice = crate::formats::Choice {
        spec: Arc::new(spec),
        by: crate::formats::Chosen::Glob,
        also: Vec::new(),
    };
    let compressed = || LoadAnswer::CompressedRecords {
        file: PathBuf::from("day.rec.zst"),
        path: None,
        choice: choice.clone(),
        options: OpenOptions::default(),
    };
    let mut loader = Loader::default();
    let _ = loader.open(request("day.rec.zst"));
    let id = loader.id().unwrap();
    let Step::DecompressRecords {
        file, path, writer, ..
    } = answer(&mut loader, id, compressed())
    else {
        panic!("the file is decompressed");
    };
    assert_eq!(
        (file.as_path(), path.as_path()),
        (Path::new("day.rec.zst"), Path::new("day.rec.zst"))
    );
    assert_eq!(
        loader.current().unwrap().phase().label(),
        ("Decompressing", 30)
    );
    assert!(loader.waits());
    assert!(matches!(
        answer(&mut loader, id, compressed()),
        Step::Nothing
    ));
    loader.retire();
    assert!(writer.stopped(), "Esc or another open stops the copy");

    let dir = tempfile::tempdir().unwrap();
    let mut loader = Loader::default();
    let _ = loader.open(request("day.rec.zst"));
    let id = loader.id().unwrap();
    let _ = answer(&mut loader, id, compressed());
    let copy = TempDownload::keep(TempDownload::create(Some(dir.path()), None).unwrap());
    let temp = copy.path().to_path_buf();
    let decompressed = || LoadAnswer::DecompressedRecords {
        copy: copy.clone(),
        path: PathBuf::from("day.rec.zst"),
        choice: choice.clone(),
        options: OpenOptions::default(),
    };
    let Step::ReadRecords { copy: read, .. } = answer(&mut loader, id, decompressed()) else {
        panic!("the copy's records are read");
    };
    assert_eq!(read, temp);
    assert_eq!(
        loader.current().unwrap().phase().label(),
        ("Reading records", 35)
    );
    assert!(matches!(
        answer(&mut loader, id, decompressed()),
        Step::Nothing
    ));
    let Step::ReadSchema { made, .. } = answer(&mut loader, id, scanned("day.rec.zst")) else {
        panic!("the frame's schema is read");
    };
    assert_eq!(made.converted.len(), 1, "the dataset holds the copy");
    drop((made, copy));
    loader.retire();
    assert!(!temp.exists(), "the retired load let the copy go");
}

/// GPS logs the scan found are converted under the name the open was asked for;
/// a second answer for the phase it left changes nothing; the frame the conversion
/// built has its schema read holding its files and notes, and installs; putting the
/// load down mid-conversion stops the writer, so its files go.
#[test]
fn gps_logs_the_scan_found_are_converted() {
    let convert = || LoadAnswer::Convert {
        what: Conversion::Text(FileFormat::Nmea),
        files: vec![PathBuf::from("logs/a.nmea"), PathBuf::from("logs/b.nmea")],
        bytes: 200,
        path: Some(PathBuf::from("logs")),
        options: OpenOptions {
            format: Some(FileFormat::Nmea),
            ..OpenOptions::default()
        },
    };
    let mut loader = Loader::default();
    let _ = loader.open(request("logs"));
    let id = loader.id().unwrap();
    let Step::Convert {
        what,
        files,
        writer,
        read,
        ..
    } = answer(&mut loader, id, convert())
    else {
        panic!("the logs are converted");
    };
    assert_eq!(what, Conversion::Text(FileFormat::Nmea));
    assert_eq!(what.status(), "Reading NMEA...");
    assert_eq!(files.len(), 2);
    assert_eq!(
        loader.current().unwrap().phase().label(),
        ("Reading NMEA log", 10)
    );
    read.store(100, Ordering::Relaxed);
    assert_eq!(
        loader.current().unwrap().phase().label(),
        ("Reading NMEA log", 20),
        "the bar moves with the bytes read"
    );
    assert!(loader.waits());
    assert!(matches!(answer(&mut loader, id, convert()), Step::Nothing));
    assert!(!writer.stopped());
    loader.retire();
    assert!(
        writer.stopped(),
        "Ctrl+O or another open stops the conversion"
    );

    let dir = tempfile::tempdir().unwrap();
    let mut loader = Loader::default();
    let _ = loader.open(request("logs"));
    let id = loader.id().unwrap();
    assert!(matches!(
        answer(&mut loader, id, convert()),
        Step::Convert { .. }
    ));
    let file = TempDownload::keep(TempDownload::create(Some(dir.path()), Some("arrow")).unwrap());
    let temp = file.path().to_path_buf();
    let note = crate::notes::Note {
        summary: "1 line left out: not NMEA".to_string(),
        scope: "of 9 lines in 2 logs".to_string(),
        read_as_text: None,
        passed_over: None,
    };
    let Step::ReadSchema { made, path, .. } = answer(
        &mut loader,
        id,
        LoadAnswer::Converted {
            converted: Converted::Frame {
                files: vec![file],
                lf: Box::new(frame()),
                notes: vec![note],
                other_tables: vec!["GSV 3".to_string()],
                detail: None,
            },
            path: Some(PathBuf::from("logs")),
            options: OpenOptions::default(),
        },
    ) else {
        panic!("the frame's schema is read");
    };
    assert_eq!(path.as_deref(), Some(Path::new("logs")));
    assert_eq!(
        loader.current().unwrap().phase().label(),
        ("Reading schema", 40)
    );
    assert_eq!(made.converted.len(), 1);
    assert_eq!(made.notes.len(), 1);
    assert_eq!(made.other_tables, ["GSV 3"]);
    drop(made);
    let Step::Failed(failed) = loader.failed(id, &format!("could not read {}", temp.display()))
    else {
        panic!("the open fails");
    };
    assert_eq!(failed.message, "could not read logs", "named as asked for");
    assert!(!temp.exists(), "the retired load let the files go");
}

/// A database of several tables, opened without `--table`, puts the load down and
/// lands on its tables, where it was asked from; a second answer changes nothing.
/// Piped in, there is no place to list them, and the open fails naming them.
#[test]
fn a_database_of_several_tables_lands_on_them() {
    let tables = |file: &str| LoadAnswer::Tables {
        file: PathBuf::from(file),
        tables: vec!["users".to_string(), "orders".to_string()],
        path: Some(PathBuf::from(file)),
    };
    let mut loader = Loader::default();
    let _ = loader.open(request("shop.db"));
    let id = loader.id().unwrap();
    let Step::Tables(landed) = answer(&mut loader, id, tables("shop.db")) else {
        panic!("the tables are listed");
    };
    assert_eq!(landed.database, Path::new("shop.db"));
    assert!(!landed.from_home);
    assert!(loader.current().is_none(), "the load is put down");
    assert!(matches!(
        answer(&mut loader, id, tables("shop.db")),
        Step::Nothing
    ));

    let dir = tempfile::tempdir().unwrap();
    let mut loader = Loader::default();
    let Step::Spool { .. } = loader.open(OpenRequest::named(
        vec![PathBuf::from("-")],
        OpenOptions::default(),
        &Default::default(),
    )) else {
        panic!("standard input is read first");
    };
    let id = loader.id().unwrap();
    let spool = TempDownload::keep(TempDownload::create(Some(dir.path()), None).unwrap());
    let options = OpenOptions {
        format: Some(FileFormat::Sqlite),
        ..Default::default()
    };
    let Step::Scan { paths, .. } = answer(
        &mut loader,
        id,
        LoadAnswer::Spooled {
            download: spool,
            options,
        },
    ) else {
        panic!("the spool is scanned");
    };
    let Step::Failed(failed) = answer(
        &mut loader,
        id,
        LoadAnswer::Tables {
            file: paths[0].clone(),
            tables: vec!["users".to_string(), "orders".to_string()],
            path: Some(PathBuf::from("stdin")),
        },
    ) else {
        panic!("piped in, the table has to be named");
    };
    assert_eq!(
        failed.message,
        "stdin holds 2 tables: users, orders. Open one with --table NAME."
    );
}

/// A table named by its path inside a database is the database opened with
/// `--table`, recorded and shown as the table.
#[test]
fn a_path_inside_a_database_opens_the_database() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("shop.db");
    let mut header = crate::formats::sqlite::MAGIC.to_vec();
    header.resize(512, 0);
    std::fs::write(&db, header).unwrap();
    let request = OpenRequest::named(
        vec![db.join("orders")],
        OpenOptions::default(),
        &Default::default(),
    );
    assert_eq!(request.paths, std::slice::from_ref(&db));
    assert_eq!(request.options.table.as_deref(), Some("orders"));
    assert_eq!(request.recent, Some(db.join("orders")));
    assert_eq!(request.shown, Some(db.join("orders")));

    let named = OpenRequest::named(
        vec![db.clone()],
        OpenOptions {
            table: Some("users".to_string()),
            ..Default::default()
        },
        &Default::default(),
    );
    assert_eq!(named.paths, std::slice::from_ref(&db));
    assert_eq!(
        named.recent,
        Some(db.join("users")),
        "--table is recorded too"
    );

    let plain = OpenRequest::named(
        vec![db.clone()],
        OpenOptions::default(),
        &Default::default(),
    );
    assert_eq!(plain.recent, Some(db));
    assert_eq!(plain.shown, None);
}

/// A local compressed CSV is decompressed rather than scanned; a CSV read with its
/// strings parsed scans saying so; a frame handed over goes straight to its schema
/// and has no path to open again.
#[test]
fn the_first_step_follows_what_was_asked_for() {
    let mut loader = Loader::default();
    assert!(matches!(
        loader.open(request("logs.csv.gz")),
        Step::Decompress { ref file, ref path, .. }
            if file == Path::new("logs.csv.gz") && path == Path::new("logs.csv.gz")
    ));
    assert_eq!(
        loader.current().unwrap().phase().label(),
        ("Decompressing", 30)
    );
    let id = loader.id().unwrap();
    assert!(matches!(
        answer(&mut loader, id, schema_read("logs.csv.gz")),
        Step::Install(_)
    ));

    let mut loader = Loader::default();
    let mut parsed = request("table.csv");
    parsed.options.parse_strings = Some(crate::ParseStringsTarget::All);
    assert!(matches!(
        loader.open(parsed),
        Step::Scan {
            status: "Scanning string columns...",
            ..
        }
    ));

    let mut loader = Loader::default();
    let Step::ReadSchema { path: None, .. } = loader.open_frame(frame(), OpenOptions::default())
    else {
        panic!("a frame's schema is read");
    };
    let id = loader.id().unwrap();
    let Step::Install(loaded) = answer(
        &mut loader,
        id,
        LoadAnswer::SchemaRead {
            state: state(),
            path: None,
            options: OpenOptions::default(),
            debug_label: None,
        },
    ) else {
        panic!("installs");
    };
    assert!(loaded.paths.is_none() && loaded.recent.is_none());
}

fn downloaded(dir: &Path, body: &str, extension: &str) -> TempDownload {
    use std::io::Write;
    let mut file = TempDownload::create(Some(dir), Some(extension)).unwrap();
    file.write_all(body.as_bytes()).unwrap();
    TempDownload::keep(file)
}

/// A remote model's headers are read by range, and that worker's dataset installs
/// under the URL. A server without byte ranges sends it to the download question,
/// which says why.
#[cfg(feature = "http")]
#[test]
fn a_remote_model_reads_its_headers_or_falls_back_to_the_download() {
    let url = "https://example.com/m/model.safetensors";
    let jobs = jobs();
    let mut loader = Loader::default();
    let Step::ReadHeaders {
        url: read, format, ..
    } = loader.open(request(url))
    else {
        panic!("the headers are read, not downloaded");
    };
    assert_eq!(
        (read.as_path(), format),
        (Path::new(url), FileFormat::Safetensors)
    );
    let id = loader.id().unwrap();
    assert_eq!(
        loader.current().unwrap().phase().label(),
        ("Reading headers", 20)
    );
    assert!(loader.waits());
    let Step::Install(loaded) = loader.answered(id, schema_read(url), &jobs) else {
        panic!("the headers are the dataset");
    };
    assert_eq!(loaded.recent.as_deref(), Some(Path::new(url)));

    let mut loader = Loader::default();
    let _ = loader.open(request(url));
    let id = loader.id().unwrap();
    let Step::Probe(pending) = loader.answered(
        id,
        LoadAnswer::NoRanges {
            options: OpenOptions::default(),
        },
        &jobs,
    ) else {
        panic!("no ranges: the file is sized for its download");
    };
    assert_eq!(pending.parts().0, url);
    assert_eq!(loader.download_note(), None, "not asked yet");
    assert!(matches!(
        loader.answered(id, LoadAnswer::Sized(pending), &jobs),
        Step::Ask(_)
    ));
    assert_eq!(loader.download_note(), Some(NO_RANGES));
    // An answer for a phase it has left changes nothing.
    assert!(matches!(
        loader.answered(id, schema_read(url), &jobs),
        Step::Nothing
    ));
}

/// Arrow in a bucket, a prefix the listing said holds it or one object, is listed
/// first, not sent to the Parquet scan; a prefix of another format, or a glob, is
/// scanned in place. A listing of IPC files only is scanned in place, unasked.
#[cfg(feature = "cloud")]
#[test]
fn arrow_in_a_bucket_is_listed_first() {
    let open = |url: &str, format: Option<FileFormat>| {
        let mut loader = Loader::default();
        let mut request = request(url);
        request.options.format = format;
        request.options.hive = url.ends_with('/');
        let step = loader.open(request);
        (loader, step)
    };
    for url in ["s3://lake/hf/", "gs://lake/hf/", "s3://lake/one.arrow"] {
        let (_, step) = open(url, url.ends_with('/').then_some(FileFormat::Arrow));
        let Step::Probe(PendingDownload::Arrow { url: listed, .. }) = step else {
            panic!("{url}: listed first");
        };
        assert_eq!(listed, url);
    }
    assert!(matches!(
        open("s3://lake/hf/", Some(FileFormat::Csv)).1,
        Step::Scan { .. }
    ));
    assert!(matches!(
        open("s3://lake/hf/*.arrow", Some(FileFormat::Arrow)).1,
        Step::Scan { .. }
    ));

    let object = |name: &str, stream: bool| crate::cloud::cloud_arrow::Object {
        url: format!("s3://lake/hf/{name}"),
        size: 10,
        stream,
    };
    let jobs = jobs();
    let (mut loader, _) = open("s3://lake/hf/", Some(FileFormat::Arrow));
    let id = loader.id().unwrap();
    let listed = |objects| {
        LoadAnswer::Sized(PendingDownload::Arrow {
            url: "s3://lake/hf/".to_string(),
            objects,
            size: Some(0),
            options: OpenOptions::default(),
        })
    };
    let Step::Scan { paths, options, .. } = loader.answered(
        id,
        listed(vec![object("a.arrow", false), object("b.arrow", false)]),
        &jobs,
    ) else {
        panic!("IPC files only: scanned in place");
    };
    assert_eq!(paths, [PathBuf::from("s3://lake/hf/")]);
    assert_eq!(
        options.arrow_parts.as_deref(),
        Some(&vec![
            crate::formats::ipc_stream::Part::InPlace(PathBuf::from("s3://lake/hf/a.arrow")),
            crate::formats::ipc_stream::Part::InPlace(PathBuf::from("s3://lake/hf/b.arrow")),
        ])
    );
    let (mut loader, _) = open("s3://lake/hf/", Some(FileFormat::Arrow));
    let id = loader.id().unwrap();
    assert!(matches!(
        loader.answered(
            id,
            listed(vec![object("a.arrow", false), object("s.arrow", true)]),
            &jobs
        ),
        Step::Ask(PendingDownload::Arrow { .. })
    ));
}

/// A small file of the built-in catalog is downloaded as soon as its size is known,
/// with no question; a large one, one the server and the catalog say nothing of,
/// and any other URL are asked about (#547 M5).
#[cfg(feature = "http")]
#[test]
fn a_small_catalog_file_downloads_without_asking() {
    let url = "https://example.com/penguins.csv";
    let jobs = jobs();
    let unasked = |listed| crate::UnaskedDownload {
        limit: 1_000,
        listed,
    };
    let ask = |options: Option<crate::UnaskedDownload>, size: Option<u64>| {
        let mut loader = Loader::default();
        let mut request = request(url);
        request.options.download_unasked = options;
        let Step::Probe(pending) = loader.open(request) else {
            panic!("the size is asked first");
        };
        let id = loader.id().unwrap();
        let step = loader.answered(id, LoadAnswer::Sized(pending.with_size(size)), &jobs);
        (matches!(step, Step::Download { .. }), loader)
    };
    let (downloads, loader) = ask(Some(unasked(None)), Some(800));
    assert!(downloads, "under the limit: no question");
    assert!(!loader.asking());
    assert_eq!(
        loader.current().unwrap().phase().label(),
        ("Downloading", 20)
    );
    assert!(
        ask(Some(unasked(Some(800))), None).0,
        "the catalog's size stands in"
    );
    assert!(
        !ask(Some(unasked(Some(800))), Some(5_000)).0,
        "the server's size wins"
    );
    assert!(!ask(Some(unasked(None)), None).0, "nothing says how big");
    assert!(!ask(None, Some(10)).0, "a URL from anywhere else asks");
}

/// A catalog file fetched without a question that runs past its limit is asked
/// about once, its size unknown; agreed to, it downloads with no limit, and a
/// large catalog file asked about up front is not asked about again either.
#[cfg(feature = "http")]
#[test]
fn an_unasked_download_past_its_limit_asks_once() {
    let url = "https://example.com/penguins.csv";
    let jobs = jobs();
    let unasked = Some(crate::UnaskedDownload {
        limit: 1_000,
        listed: Some(800),
    });
    let mut loader = Loader::default();
    let mut asked_for = request(url);
    asked_for.options.download_unasked = unasked;
    let Step::Probe(pending) = loader.open(asked_for) else {
        panic!("the size is asked first");
    };
    let id = loader.id().unwrap();
    let Step::Download { pending, .. } =
        loader.answered(id, LoadAnswer::Sized(pending.with_size(None)), &jobs)
    else {
        panic!("small by the catalog: no question");
    };
    assert!(
        pending.parts().2.download_unasked.is_some(),
        "it has a limit"
    );

    let Step::Ask(asked) = loader.answered(id, LoadAnswer::PastLimit(pending), &jobs) else {
        panic!("past the limit, it asks");
    };
    assert_eq!(asked.parts().1, None, "its size is not what anyone said");
    assert_eq!(loader.download_note(), Some(PAST_LIMIT));
    assert!(jobs.would_strand(), "the generation is held while it asks");
    let Step::Download { pending, .. } = loader.confirmed() else {
        panic!("agreed to, it downloads");
    };
    assert!(pending.parts().2.download_unasked.is_none(), "asked once");

    // Large by the server: asked up front, and not again.
    let mut loader = Loader::default();
    let mut asked_for = request(url);
    asked_for.options.download_unasked = unasked;
    let Step::Probe(pending) = loader.open(asked_for) else {
        panic!("the size is asked first");
    };
    let id = loader.id().unwrap();
    let step = loader.answered(id, LoadAnswer::Sized(pending.with_size(Some(5_000))), &jobs);
    assert!(matches!(step, Step::Ask(_)));
    let Step::Download { pending, .. } = loader.confirmed() else {
        panic!("agreed to, it downloads");
    };
    assert!(pending.parts().2.download_unasked.is_none());
}

#[cfg(any(feature = "http", feature = "cloud"))]
#[test]
fn the_past_limit_note_names_the_limit() {
    assert_eq!(crate::UnaskedDownload::LIMIT, 50 * 1024 * 1024);
    assert!(PAST_LIMIT.contains("50 MiB"));
}

/// A remote file is sized, put to the user, downloaded, then scanned under its URL;
/// the installed dataset holds the file, and opening the URL again reads that copy
/// rather than asking again.
#[cfg(feature = "http")]
#[test]
fn a_remote_file_is_asked_about_then_downloaded_and_kept() {
    let url = "https://example.com/data.csv";
    let dir = tempfile::tempdir().unwrap();
    let jobs = jobs();
    let mut loader = Loader::default();
    let Step::Probe(pending) = loader.open(request(url)) else {
        panic!("the size is asked first");
    };
    let id = loader.id().unwrap();
    assert_eq!(
        loader.current().unwrap().phase().label(),
        ("Checking size", 0)
    );

    let sized = pending.with_size(Some(42));
    assert!(matches!(
        loader.answered(id, LoadAnswer::Sized(sized), &jobs),
        Step::Ask(ref p) if p.parts().1 == Some(42)
    ));
    assert!(loader.asking() && loader.awaiting_dataset());
    assert!(!loader.waits(), "the question has the keys");
    assert!(jobs.would_strand(), "the generation is held while it asks");

    let Step::Download { .. } = loader.confirmed() else {
        panic!("agreed to, it downloads");
    };
    assert!(!jobs.would_strand(), "the hold goes with the question");
    assert!(matches!(loader.confirmed(), Step::Nothing), "asked once");
    assert_eq!(
        loader.current().unwrap().phase().label(),
        ("Downloading", 20)
    );

    let file = downloaded(dir.path(), "a\n1\n", "csv");
    let at = file.path().to_path_buf();
    let Step::Scan {
        paths,
        display,
        status,
        ..
    } = loader.answered(
        id,
        LoadAnswer::Downloaded {
            download: file,
            options: OpenOptions::default(),
        },
        &jobs,
    )
    else {
        panic!("the download is scanned");
    };
    assert_eq!(paths, vec![at.clone()]);
    assert_eq!(display.as_deref(), Some(Path::new(url)), "named by its URL");
    assert_eq!(status, "Scanning...");
    let Step::ReadSchema {
        made: Made {
            download: Some(download),
            ..
        },
        ..
    } = loader.answered(id, scanned(url), &jobs)
    else {
        panic!("the schema read is handed the download");
    };
    assert_eq!(download.path(), at, "the file the scan reads");
    // As the worker builds it: the dataset holds its file from the start.
    let state = DataTableState::from_lazyframe(frame(), &OpenOptions::default())
        .unwrap()
        .with_open(crate::table::OpenFacts {
            download: Some(download),
            ..Default::default()
        });
    let read = LoadAnswer::SchemaRead {
        state: Box::new(state),
        path: Some(PathBuf::from(url)),
        options: OpenOptions::default(),
        debug_label: None,
    };
    let Step::Install(loaded) = loader.answered(id, read, &jobs) else {
        panic!("installs");
    };
    assert!(
        loaded.state.scans_a_download(),
        "the dataset holds its file"
    );
    loader.first_rows_settled();
    drop(loaded);
    assert!(at.exists(), "and the loader keeps it to read again");

    let Step::Scan { paths, display, .. } = loader.open(request(url)) else {
        panic!("the copy on hand is scanned, with no question");
    };
    assert_eq!(paths, vec![at.clone()]);
    assert_eq!(display.as_deref(), Some(Path::new(url)));

    // Opening something else lets the copy go.
    assert!(loader.make_way().is_some());
    let _ = loader.open(request("local.csv"));
    assert!(!at.exists(), "nothing else held it");
}

/// The download handed to the schema read is claimed like any file the open wrote:
/// quitting while that read is out removes it, though the worker still holds it.
#[cfg(feature = "http")]
#[test]
fn quitting_mid_schema_read_sweeps_the_download_the_worker_holds() {
    use std::time::{Duration, Instant};
    let url = "https://example.com/data.csv";
    let dir = tempfile::tempdir().unwrap();
    let jobs = jobs();
    let mut loader = Loader::default();
    let Step::Probe(pending) = loader.open(request(url)) else {
        panic!("the size is asked first");
    };
    let id = loader.id().unwrap();
    let _ = loader.answered(id, LoadAnswer::Sized(pending.with_size(Some(4))), &jobs);
    let Step::Download { writer, .. } = loader.confirmed() else {
        panic!("agreed to, it downloads");
    };
    let file = crate::cloud::download::read_to_temp(
        Some(dir.path()),
        Some("csv"),
        || Ok((std::io::Cursor::new(b"a\n1\n".to_vec()), Some(4))),
        &writer,
        None,
    )
    .unwrap();
    let at = file.path().to_path_buf();
    let downloaded = LoadAnswer::Downloaded {
        download: file,
        options: OpenOptions::default(),
    };
    let _ = loader.answered(id, downloaded, &jobs);
    let Step::ReadSchema {
        made: Made {
            download: Some(held),
            ..
        },
        ..
    } = loader.answered(id, scanned(url), &jobs)
    else {
        panic!("the schema read is handed the download");
    };

    // Quitting drops the app, and the loader with it; the worker is still reading.
    let unfinished = loader.unfinished().clone();
    drop(loader);
    assert!(at.exists(), "the worker's copy keeps it");
    unfinished.sweep(Instant::now() + Duration::from_millis(50));
    assert!(!at.exists(), "the sweep removes it");
    // The worker ending later finds nothing to remove.
    drop(held);
}

/// Declining the download, a superseding open and a stale download each retire what
/// the load held: the question's hold, and the file.
#[cfg(feature = "http")]
#[test]
fn a_remote_open_put_down_leaves_nothing_behind() {
    let url = "https://example.com/data.csv";
    let dir = tempfile::tempdir().unwrap();
    let jobs = jobs();
    let mut loader = Loader::default();
    let Step::Probe(pending) = loader.open(request(url)) else {
        panic!("probe");
    };
    let id = loader.id().unwrap();
    let _ = loader.answered(id, LoadAnswer::Sized(pending), &jobs);
    let retired = loader.retire().expect("declined");
    assert!(retired.asking, "the question goes with it");
    assert!(!jobs.would_strand(), "and so does its hold");

    // A download answering for a load replaced meanwhile is dropped, and its file
    // with it.
    let Step::Probe(pending) = loader.open(request(url)) else {
        panic!("probe");
    };
    let old = loader.id().unwrap();
    let _ = loader.answered(old, LoadAnswer::Sized(pending), &jobs);
    let Step::Download { writer, .. } = loader.confirmed() else {
        panic!("download");
    };
    assert!(loader.make_way().is_some());
    assert!(writer.stopped(), "the download in flight is told to stop");
    let _ = loader.open(request("local.csv"));
    let file = downloaded(dir.path(), "a\n1\n", "csv");
    let at = file.path().to_path_buf();
    assert!(matches!(
        loader.answered(
            old,
            LoadAnswer::Downloaded {
                download: file,
                options: OpenOptions::default()
            },
            &jobs
        ),
        Step::Nothing
    ));
    assert!(!at.exists(), "the stale download's file went with it");

    // A download whose scan fails is let go, not kept.
    assert!(loader.make_way().is_some());
    let Step::Probe(pending) = loader.open(request(url)) else {
        panic!("probe");
    };
    let id = loader.id().unwrap();
    let _ = loader.answered(id, LoadAnswer::Sized(pending), &jobs);
    let _ = loader.confirmed();
    let file = downloaded(dir.path(), "a\n1\n", "csv");
    let at = file.path().to_path_buf();
    let _ = loader.answered(
        id,
        LoadAnswer::Downloaded {
            download: file,
            options: OpenOptions::default(),
        },
        &jobs,
    );
    let reason = format!(
        "\"{}\": Bad csv.\nIt stopped at {}.",
        at.display(),
        at.display()
    );
    let Step::Failed(failed) = loader.failed(id, &reason) else {
        panic!("the open fails");
    };
    assert_eq!(
        failed.message,
        format!("\"{url}\": Bad csv.\nIt stopped at {url}."),
        "named by the URL opened, never the temp file (#511)"
    );
    assert!(!at.exists(), "a failed open keeps no download");
}

/// A compressed CSV, TSV or PSV downloaded is decompressed under its URL as its
/// format, as one opened from disk is (#567).
#[cfg(feature = "http")]
#[test]
fn a_downloaded_compressed_csv_is_decompressed() {
    for (ext, format) in [
        ("csv.gz", FileFormat::Csv),
        ("tsv.zst", FileFormat::Tsv),
        ("psv.xz", FileFormat::Psv),
    ] {
        let url = format!("https://example.com/data.{ext}");
        let dir = tempfile::tempdir().unwrap();
        let jobs = jobs();
        let mut loader = Loader::default();
        let Step::Probe(pending) = loader.open(request(&url)) else {
            panic!("probe");
        };
        let id = loader.id().unwrap();
        let _ = loader.answered(id, LoadAnswer::Sized(pending), &jobs);
        let _ = loader.confirmed();
        let file = downloaded(dir.path(), "", ext);
        let step = loader.answered(
            id,
            LoadAnswer::Downloaded {
                download: file,
                options: OpenOptions::default(),
            },
            &jobs,
        );
        assert!(
            matches!(
                step,
                Step::Decompress { ref path, ref options, .. }
                    if path == Path::new(&url) && options.format == Some(format)
            ),
            "{ext}"
        );
    }
}

/// Standard input is read to a file first, its bytes counted on the loading
/// screen, then scanned or decompressed as `stdin`; it is no recent, and opened
/// again the copy on hand is read rather than the spent pipe.
#[test]
fn stdin_is_spooled_then_read_as_stdin() {
    let dir = tempfile::tempdir().unwrap();
    let mut loader = Loader::default();
    let request = OpenRequest::named(
        vec![PathBuf::from("-")],
        OpenOptions::default(),
        &Default::default(),
    );
    assert_eq!(request.recent, None, "not recorded in recents");
    let Step::Spool { writer, read, .. } = loader.open(request) else {
        panic!("standard input is read first");
    };
    let id = loader.id().unwrap();
    let load = loader.current().unwrap();
    assert_eq!(load.phase().label(), ("Reading stdin", 5));
    assert_eq!(load.path(), Some(Path::new("stdin")));
    read.store(1234, Ordering::Relaxed);
    assert_eq!(loader.current().unwrap().size(), 1234, "what has come in");
    assert!(loader.waits());

    let file = {
        use std::io::Write;
        let mut file = TempDownload::create(Some(dir.path()), None).unwrap();
        file.write_all(b"a\n1\n").unwrap();
        TempDownload::keep(file)
    };
    let at = file.path().to_path_buf();
    let options = OpenOptions {
        format: Some(FileFormat::Csv),
        ..Default::default()
    };
    let Step::Scan { paths, display, .. } = answer(
        &mut loader,
        id,
        LoadAnswer::Spooled {
            download: file,
            options: options.clone(),
        },
    ) else {
        panic!("the file is scanned");
    };
    assert_eq!(paths, vec![at.clone()]);
    assert_eq!(display.as_deref(), Some(Path::new("stdin")));
    let Step::ReadSchema {
        made: Made {
            download: Some(_), ..
        },
        ..
    } = answer(&mut loader, id, scanned("stdin"))
    else {
        panic!("the dataset holds the file");
    };
    let Step::Install(loaded) = answer(&mut loader, id, schema_read("stdin")) else {
        panic!("installs");
    };
    assert_eq!(loaded.recent, None);
    assert_eq!(loaded.paths.as_deref(), Some(&[PathBuf::from("-")][..]));
    loader.first_rows_settled();
    drop(writer);

    let Step::Scan { paths, display, .. } = loader.open(OpenRequest::named(
        vec![PathBuf::from("-")],
        options,
        &Default::default(),
    )) else {
        panic!("the copy on hand is read again");
    };
    assert_eq!(paths, vec![at]);
    assert_eq!(display.as_deref(), Some(Path::new("stdin")));

    // Standard input with another path cannot be read.
    assert!(loader.make_way().is_some());
    let step = loader.open(OpenRequest::named(
        vec![PathBuf::from("-"), PathBuf::from("a.csv")],
        OpenOptions::default(),
        &Default::default(),
    ));
    assert!(matches!(step, Step::Crash(_)));
}

/// A stream piped in is converted, and the copy, not the spool, is what is kept:
/// the spool goes once converted, and opened again the copy is scanned as it is.
#[test]
fn a_converted_pipe_keeps_the_copy_not_the_stream() {
    let dir = tempfile::tempdir().unwrap();
    let mut loader = Loader::default();
    let options = OpenOptions {
        format: Some(FileFormat::Arrow),
        ..Default::default()
    };
    let Step::Spool { .. } = loader.open(OpenRequest::named(
        vec![PathBuf::from("-")],
        OpenOptions::default(),
        &Default::default(),
    )) else {
        panic!("standard input is read first");
    };
    let id = loader.id().unwrap();
    let spool = downloaded(dir.path(), "stream", "tmp");
    let spooled = spool.path().to_path_buf();
    let Step::Scan { paths, .. } = answer(
        &mut loader,
        id,
        LoadAnswer::Spooled {
            download: spool,
            options: options.clone(),
        },
    ) else {
        panic!("the spool is scanned");
    };
    let Step::Convert { .. } = answer(
        &mut loader,
        id,
        LoadAnswer::Convert {
            what: Conversion::Streams,
            files: paths,
            bytes: 6,
            path: Some(PathBuf::from("stdin")),
            options: options.clone(),
        },
    ) else {
        panic!("the stream is converted");
    };
    let copy = downloaded(dir.path(), "ipc file", "arrow");
    let converted = copy.path().to_path_buf();
    let Step::Scan { paths, display, .. } = answer(
        &mut loader,
        id,
        LoadAnswer::Converted {
            converted: Converted::Streams {
                file: copy,
                parts: Vec::new(),
            },
            path: Some(PathBuf::from("stdin")),
            options: options.clone(),
        },
    ) else {
        panic!("the copy is scanned");
    };
    assert_eq!(paths, std::slice::from_ref(&converted));
    assert_eq!(display.as_deref(), Some(Path::new("stdin")));
    assert!(!spooled.exists(), "the spool goes once converted");

    let Step::ReadSchema {
        made: Made {
            download: Some(_), ..
        },
        ..
    } = answer(&mut loader, id, scanned("stdin"))
    else {
        panic!("the dataset holds the copy");
    };
    let Step::Install(loaded) = answer(&mut loader, id, schema_read("stdin")) else {
        panic!("installs");
    };
    loader.first_rows_settled();
    drop(loaded);
    assert!(converted.exists(), "kept to read again");
    let Step::Scan { paths, .. } = loader.open(OpenRequest::named(
        vec![PathBuf::from("-")],
        options,
        &Default::default(),
    )) else {
        panic!("the copy on hand is read again");
    };
    assert_eq!(paths, [converted]);
}

/// A compressed CSV piped in is decompressed as `stdin`; putting the read down
/// stops its writer, which removes the partial file.
#[test]
fn piped_compressed_csv_is_decompressed_and_a_stop_reaches_the_spool() {
    let dir = tempfile::tempdir().unwrap();
    let mut loader = Loader::default();
    let Step::Spool { writer, .. } = loader.open(OpenRequest::named(
        vec![PathBuf::from("-")],
        OpenOptions::default(),
        &Default::default(),
    )) else {
        panic!("spool");
    };
    assert!(loader.make_way().is_some(), "Ctrl+O puts it down");
    assert!(writer.stopped(), "and the spool stops at its next chunk");

    let Step::Spool { .. } = loader.open(OpenRequest::named(
        vec![PathBuf::from("-")],
        OpenOptions::default(),
        &Default::default(),
    )) else {
        panic!("spool");
    };
    let id = loader.id().unwrap();
    let file = TempDownload::keep(TempDownload::create(Some(dir.path()), None).unwrap());
    let step = answer(
        &mut loader,
        id,
        LoadAnswer::Spooled {
            download: file,
            options: OpenOptions {
                format: Some(FileFormat::Csv),
                compression: Some(CompressionFormat::Gzip),
                ..Default::default()
            },
        },
    );
    assert!(matches!(
        step,
        Step::Decompress { ref path, download: Some(_), .. } if path == Path::new("stdin")
    ));
}
