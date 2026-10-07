use super::*;
use polars::prelude::*;

/// A file row's read follows its format and how it is stored, and a remote file
/// other than a Parquet object or a model file is downloaded first. Directories say nothing.
#[test]
fn how_a_row_is_read() {
    use crate::ReadMode::*;
    let how = |path: &str| how_read(&Entry::for_test(Path::new(path), path));
    let at = |path: &str, mode, download| {
        assert_eq!(how(path), Some(HowRead { mode, download }), "{path}");
    };
    at("/d/a.parquet", Lazy, false);
    at("/d/a.csv", Lazy, false);
    at("/d/a.csv.gz", Decompressed, false);
    at("/d/a.json", InMemory, false);
    at("/d/a.gpx", Converted, false);
    at("/d/a.arrow", Lazy, false);
    at("s3://b/a.parquet", Lazy, false);
    at("s3://b/a.csv", Lazy, true);
    at("gs://b/a.json", InMemory, true);
    at("https://example.com/a.parquet", Lazy, true);
    at("s3://b/m.safetensors", InMemory, false);
    at("https://example.com/m.gguf", InMemory, false);
    assert_eq!(how("/d/a.parquet.gz"), None, "does not open");
    assert_eq!(how("/d/README"), None);

    let mut stream = Entry::for_test(Path::new("/d/x.arrow"), "x.arrow");
    stream.cost.ipc_stream = true;
    assert_eq!(how_read(&stream).map(|h| h.mode), Some(Converted));
    let mut spec = Entry::for_test(Path::new("/d/day.l2.zst"), "day.l2.zst");
    spec.format_spec = Some("acme.l2feed".into());
    assert_eq!(how_read(&spec).map(|h| h.mode), Some(Decompressed));
    at("/d/shop.db", Lazy, false);
    at("s3://b/shop.sqlite", Lazy, true);
    let mut table = Entry::for_test(Path::new("/d/shop.db/orders"), "orders");
    table.table = Some(TableOf {
        format: Some(crate::FileFormat::Sqlite),
        kind: "table".into(),
        internal: false,
    });
    assert_eq!(how_read(&table).map(|h| h.mode), Some(Lazy));
    assert_eq!(how_read(&Entry::directory(Path::new("/d/x"))), None);
}

/// An Arrow file is told a stream by its first bytes when it is measured.
#[test]
fn measuring_an_arrow_file_tells_a_stream() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("file.arrow");
    std::fs::write(&file, b"ARROW1\0\0rest").unwrap();
    let stream = dir.path().join("stream.arrow");
    std::fs::write(&stream, b"\xff\xff\xff\xff\x10\x01\0\0").unwrap();
    for (path, is_stream) in [(file, false), (stream, true)] {
        let mut entry = Entry::for_test(&path, "x.arrow");
        enrich(&mut entry);
        assert_eq!(entry.cost.ipc_stream, is_stream, "{}", path.display());
    }
}

/// Every extension the home screen offers has a reader behind it, and every
/// extension a reader knows is offered. The two lists had drifted: `.psv` and
/// `.xlsb` opened but were invisible, and `.txt` was listed and then refused.
#[test]
fn what_is_offered_and_what_opens_are_one_list() {
    for ext in [
        "parquet", "csv", "tsv", "psv", "json", "jsonl", "ndjson", "arrow", "arrows", "ipc",
        "feather", "avro", "orc", "xls", "xlsx", "xlsm", "xlsb",
    ] {
        let named = PathBuf::from(format!("sales.{ext}"));
        assert!(
            data_format(&named).is_some(),
            ".{ext} opens, so the home screen must offer it"
        );
    }
    // A README is text, read as lines; beside data it is not the directory's
    // table (`a_file_datui_does_not_read_does_not_disqualify_a_directory`).
    assert_eq!(
        data_format(Path::new("README.txt")),
        Some(crate::FileFormat::Text)
    );
    assert!(data_format(Path::new("notes")).is_none());
}

/// A format's name is not an extension, and the one place that stores a name has
/// to read it back with the inverse of what wrote it. `excel` is a name no
/// extension spells, so parsing it as one answers `None` — and `None` there means
/// "not Parquet", which leaves a directory's counts off rather than filling them from
/// whatever Parquet is under it.
#[test]
fn a_format_name_round_trips_only_through_from_name() {
    use crate::FileFormat;
    for format in FileFormat::ALL {
        assert_eq!(
            FileFormat::from_name(format.name()),
            Some(format),
            "{} is a name",
            format.name()
        );
    }
    assert_eq!(FileFormat::from_extension("excel"), None);
    assert_eq!(FileFormat::from_name("xlsx"), None);
}

/// A compression suffix is how a file is stored, not what it holds, on both routes.
#[test]
fn a_compressed_name_reads_as_the_format_under_it() {
    assert_eq!(
        data_format(Path::new("sales.csv.gz")),
        Some(crate::FileFormat::Csv)
    );
    assert_eq!(
        data_format(Path::new("events.json.zst")),
        Some(crate::FileFormat::Json)
    );
}

/// `.ipc`, `.arrow` and `.feather` are one format under three names, so a directory
/// holding two of them is one kind of thing rather than a mixture.
#[test]
fn one_format_under_several_names_is_not_a_mixture() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.arrow"), b"x").unwrap();
    std::fs::write(dir.path().join("b.ipc"), b"x").unwrap();
    assert_eq!(classify_directory(dir.path()), EntryKind::MultiFile);
}

/// A README is neither a marker nor data. Locally it counts toward the majority
/// and does not disqualify the directory; the cloud route counted it as data and
/// answered `dir` where the local one said `multi`.
#[test]
fn a_file_datui_does_not_read_does_not_disqualify_a_directory() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.parquet", &["id"]);
    write(dir.path(), "b.parquet", &["id"]);
    std::fs::write(dir.path().join("README.txt"), b"notes").unwrap();

    #[cfg(feature = "cloud")]
    let objects: Vec<(String, u64)> = [
        ("out/a.parquet", 100u64),
        ("out/b.parquet", 100),
        ("out/README.txt", 12),
    ]
    .iter()
    .map(|(k, s)| ((*k).to_string(), *s))
    .collect();

    #[cfg(feature = "cloud")]
    assert_eq!(
        classify_directory(dir.path()),
        crate::cloud::cloud_browse::look_at_listing("out/", &[], &objects).0,
        "the two routes answer the same directory alike"
    );
    assert_eq!(classify_directory(dir.path()), EntryKind::MultiFile);
}

/// A label says what is inside, so it is true whatever `Enter` then does. The same
/// directory of three tables reads `3 parquet` and is one to look inside.
#[test]
fn a_label_counts_what_is_there_rather_than_naming_a_decision() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["a.parquet", "b.parquet", "c.parquet"] {
        write(dir.path(), name, &["id"]);
    }
    std::fs::create_dir_all(dir.path().join("archive")).unwrap();
    std::fs::write(dir.path().join("notes.csv"), b"x").unwrap();
    std::fs::write(dir.path().join("_SUCCESS"), b"").unwrap();
    std::fs::write(dir.path().join(".part.crc"), b"").unwrap();

    let entry = measured(dir.path());
    assert_eq!(entry.label(), "mixed", "two formats is two formats");
    assert_eq!(
        entry.holds.line(true).as_deref(),
        Some("3 parquet · 1 csv · 1 directory"),
        "and the pane says what the label boiled down"
    );
    assert_eq!(entry.holds.data_files(), 4);
    assert_eq!(entry.holds.directories, 1);
}

/// One format, and the count is the files.
#[test]
fn a_directory_of_one_format_is_labelled_by_it() {
    let dir = tempfile::tempdir().unwrap();
    for i in 0..12 {
        write(dir.path(), &format!("part-{i:05}.parquet"), &["id", "ts"]);
    }
    let entry = measured(dir.path());
    assert_eq!(entry.label(), "12 parquet");
    assert_eq!(entry.holds.line(true).as_deref(), Some("12 parquet"));

    // A directory with nothing in it datui reads is a place to look inside. The
    // files are counted, but the pane's line is about what can be opened, and
    // "20 not read" read as a fault in a directory with nothing wrong in it.
    let plain = tempfile::tempdir().unwrap();
    for i in 0..20 {
        std::fs::write(plain.path().join(format!("note{i}.md")), b"x").unwrap();
    }
    let plain = measured(plain.path());
    assert_eq!(plain.label(), "dir");
    assert_eq!(plain.holds.not_read, 20);
    assert_eq!(plain.holds.line(true), None);
}

/// A row nothing has looked into has only its kind to go on, and a hive root or a
/// lake table is named by the thing it is rather than counted.
#[test]
fn a_kind_that_names_itself_keeps_its_name() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("year=2024")).unwrap();
    std::fs::create_dir_all(dir.path().join("year=2025")).unwrap();
    assert_eq!(measured(dir.path()).label(), "hive");

    let lake = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(lake.path().join("_delta_log")).unwrap();
    assert_eq!(measured(lake.path()).label(), "delta");

    let unlooked = Entry::new(PathBuf::from("/nowhere"), EntryKind::Unknown);
    assert_eq!(unlooked.label(), "");
}

/// A directory of exactly the cap is a total, not a floor. `5000+` claims there is
/// more; saying so about a directory that was read whole is a lie in the direction
/// nobody can check.
#[test]
fn a_directory_read_whole_does_not_claim_there_is_more() {
    let dir = tempfile::tempdir().unwrap();
    for i in 0..MAX_ENTRIES_PER_DIR {
        std::fs::write(dir.path().join(format!("f{i:05}.csv")), b"x").unwrap();
    }
    let holds = look_at_directory(dir.path()).1;
    assert!(!holds.truncated, "every entry was read");
    assert_eq!(holds.label(), format!("{MAX_ENTRIES_PER_DIR} csv"));

    std::fs::write(dir.path().join("one-more.csv"), b"x").unwrap();
    let holds = look_at_directory(dir.path()).1;
    assert!(holds.truncated, "and now there is more than was read");
    assert!(holds.label().contains('+'));
}

/// A lake table is answered by three `join` tests and costs no listing. Counting
/// one would walk every table in a warehouse on every pass, for a line beside a
/// table whose files `enrich` then refuses to read anyway.
#[test]
fn a_lake_table_is_not_counted() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("_delta_log")).unwrap();
    write(dir.path(), "part-00000.parquet", &["id"]);
    write(dir.path(), "part-00001.parquet", &["id"]);

    let (kind, holds) = look_at_directory(dir.path());
    assert_eq!(kind, EntryKind::Delta);
    assert!(holds.is_empty(), "and its label is the format's own name");
    let entry = measured(dir.path());
    assert_eq!(entry.label(), "delta");
}

/// A listing cut short cannot say there is no data in a directory, only that it found
/// none among the entries it read. `mixed` needs no such qualifier — more files
/// cannot unmake it — and `dir` does, because they can.
#[test]
fn a_cut_short_listing_does_not_claim_a_directory_is_empty() {
    let seen = Holds {
        skipped: 5000,
        truncated: true,
        ..Default::default()
    };
    assert_eq!(seen.label(), "dir+");

    let whole = Holds {
        skipped: 3,
        ..Default::default()
    };
    assert_eq!(whole.label(), "dir");

    // And a listing cut short before it found anything at all still says so: it is
    // not an empty tally, or the row falls back to its kind and reads `dir`.
    let nothing_yet = Holds {
        truncated: true,
        ..Default::default()
    };
    assert!(!nothing_yet.is_empty());
    assert_eq!(nothing_yet.label(), "dir+");
    assert!(Holds::default().is_empty());

    let mixed = Holds {
        formats: vec![("parquet".to_string(), 3), ("csv".to_string(), 2)],
        truncated: true,
        ..Default::default()
    };
    assert_eq!(mixed.label(), "mixed", "more files cannot unmake it");
}

/// A name cut to fit keeps both ends. A Hadoop output directory's `.crc` files are
/// named for the file they check, and the head and the tail are what say so.
#[test]
fn a_long_name_keeps_both_ends() {
    let name = ".part-00000-8f3a91c2-7b4d-4e19-a6f0-c1d2e3f4a5b6-c000.snappy.parquet.crc";
    let line = crate::glyphs::fit_middle(name, 24);
    assert!(line.starts_with(".part-00000"), "the head: {line}");
    // The tail, as much of it as the ellipsis leaves: it takes three characters of
    // the twenty-four in the ASCII glyph set and one in the Unicode one.
    assert!(line.ends_with(".crc"), "and the tail: {line}");
    assert!(!line.contains("8f3a91c2"), "the middle goes: {line}");
    assert!(line.chars().count() <= 24, "{line}");
}

/// The same directory read twice reads the same. Skipped names come back in whatever
/// order the filesystem holds them, so the pane takes the first few *by name*.
#[test]
fn what_a_directory_holds_reads_the_same_twice() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.parquet", &["id"]);
    for marker in [
        "_SUCCESS",
        "_committed_9",
        "_committed_1",
        ".crc",
        "_started_4",
    ] {
        std::fs::write(dir.path().join(marker), b"").unwrap();
    }
    let first = look_at_directory(dir.path()).1;
    for _ in 0..8 {
        assert_eq!(look_at_directory(dir.path()).1, first);
    }
    assert_eq!(
        first.skipped_names,
        vec![".crc", "_SUCCESS", "_committed_1", "_committed_9"],
        "the first four by name, of five"
    );
    assert_eq!(first.skipped, 5);
}

/// The label counts what is directly inside; the numbers beside it are a promise
/// about what `Enter` gives, and `Enter` reads the subtree. Measuring only the top
/// would promise three files and open twenty-three — and would ask `is_one_table`
/// about three files while unioning all twenty-three, which is the union the
/// downgrade exists to prevent. The `holds` line names the directory that explains
/// it.
#[test]
fn a_directories_numbers_are_what_opening_it_gives() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["a.parquet", "b.parquet", "c.parquet"] {
        write(dir.path(), name, &["id", "legacy"]);
    }
    let archive = dir.path().join("archive");
    std::fs::create_dir_all(&archive).unwrap();
    for i in 0..20 {
        write(&archive, &format!("old-{i}.parquet"), &["id", "legacy"]);
    }

    let entry = measured(dir.path());
    assert_eq!(
        entry.label(),
        "3 parquet",
        "three files are directly inside"
    );
    assert_eq!(
        entry.holds.line(true).as_deref(),
        Some("3 parquet · 1 directory")
    );
    assert_eq!(entry.rows, Some(23), "and opening it reads all of them");
}

/// A directory past the counting budget whose *own* files were all read says an exact
/// width. The budget is about the subtree; three files at the top are three
/// footers, and `5+ cols` claims a sample that did not happen.
#[test]
fn a_width_is_a_floor_only_when_a_footer_went_unread() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["a.parquet", "b.parquet", "c.parquet"] {
        write(dir.path(), name, &["id", "ts"]);
    }
    let archive = dir.path().join("archive");
    std::fs::create_dir_all(&archive).unwrap();
    for i in 0..MAX_FOOTERS_PER_DATASET + 6 {
        write(
            &archive,
            &format!("old-{i:03}.parquet"),
            &["wholly", "different"],
        );
    }

    let entry = measured(dir.path());
    assert_eq!(entry.kind, EntryKind::Directory, "not one table");
    assert_eq!(entry.label(), "3 parquet");
    assert_eq!(entry.cols, Some(2), "id and ts");
    assert!(
        !entry.cols_sampled,
        "all three of its own footers were read"
    );
}

#[test]
fn a_big_directory_is_still_found_by_a_column_one_level_down() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["a.parquet", "b.parquet", "c.parquet"] {
        write(dir.path(), name, &["id", "ts"]);
    }
    let archive = dir.path().join("archive");
    std::fs::create_dir_all(&archive).unwrap();
    for i in 0..MAX_FOOTERS_PER_DATASET + 6 {
        write(
            &archive,
            &format!("old-{i:03}.parquet"),
            &["wholly", "different"],
        );
    }

    let entry = measured(dir.path());
    assert_eq!(entry.kind, EntryKind::Directory);
    // The label and the width are the three files directly inside.
    assert_eq!(entry.label(), "3 parquet");
    assert_eq!(entry.cols, Some(2), "id and ts");
    // The names are not: they are the home screen's search index, and `wholly` has
    // to reach the directory that holds one whether the directory was small enough to
    // read every footer or, as here, too big and sampled instead. Narrowing these
    // to the directory's own files made the answer depend on the directory's size.
    assert!(
        entry.columns.contains(&"wholly".to_string()),
        "{:?}",
        entry.columns
    );
    assert!(entry.columns.contains(&"id".to_string()));
}

#[test]
fn a_width_over_a_directories_own_files_is_a_floor_when_there_are_too_many() {
    let dir = tempfile::tempdir().unwrap();
    // Past the footer budget with the directory's *own* files, and no two of them one
    // table, so the downgrade samples its own files as well and says so. Every file
    // gets its own column, because which three get sampled is `read_dir` order.
    for i in 0..MAX_FOOTERS_PER_DATASET + 6 {
        write(
            dir.path(),
            &format!("f-{i:03}.parquet"),
            &[&format!("c{i}")],
        );
    }

    let entry = measured(dir.path());
    assert_eq!(entry.kind, EntryKind::Directory, "not one table");
    assert_eq!(entry.label(), "70 parquet");
    assert!(
        entry.cols_sampled,
        "three of seventy footers were read, so the width is a floor"
    );
}

#[test]
fn a_directory_read_as_one_table_is_sized_by_everything_under_it() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.parquet", &["id", "ts"]);
    write(dir.path(), "b.parquet", &["id", "ts"]);
    let more = dir.path().join("more");
    std::fs::create_dir_all(&more).unwrap();
    write(&more, "c.parquet", &["id", "ts"]);

    let entry = measured(dir.path());
    assert_eq!(entry.kind, EntryKind::MultiFile, "one table");
    // `Enter` unions the subtree, so the size and the rows beside it are the
    // subtree's — the opposite of a downgraded directory, whose numbers are its own
    // files because it is never opened as one table.
    let all: u64 = [
        dir.path().join("a.parquet"),
        dir.path().join("b.parquet"),
        more.join("c.parquet"),
    ]
    .iter()
    .map(|p| std::fs::metadata(p).unwrap().len())
    .sum();
    assert_eq!(entry.size, Some(all));
    assert_eq!(entry.rows, Some(3));
}

#[test]
fn nothing_counted_is_the_only_thing_holds_calls_empty() {
    // `is_empty` stops a peek's answer reaching a row and keeps a `Holds` out of
    // the cache, so anything it calls empty is thrown away. Asked of each field on
    // its own, because the contract is the function's and not its callers': both
    // routes happen to set `directories` beside `partitions` and `skipped` beside
    // `skipped_names` today, which is exactly the kind of agreement that stops
    // holding one refactor later.
    assert!(Holds::default().is_empty());
    let one = |f: fn(&mut Holds)| {
        let mut h = Holds::default();
        f(&mut h);
        h
    };
    for (what, holds) in [
        ("a data file", one(|h| h.formats.push(("csv".into(), 1)))),
        ("a directory", one(|h| h.directories = 1)),
        ("a partition", one(|h| h.partitions = 1)),
        ("a file it cannot read", one(|h| h.not_read = 1)),
        ("a writer's own file", one(|h| h.skipped = 1)),
        (
            "the name of one",
            one(|h| h.skipped_names.push("_SUCCESS".into())),
        ),
        ("a listing cut short", one(|h| h.truncated = true)),
    ] {
        assert!(!holds.is_empty(), "{what} is something to say");
    }
}

#[test]
fn formats_that_tie_are_ordered_by_name_whatever_order_they_arrived_in() {
    use crate::FileFormat;
    // Given in the order that is wrong on both counts, so neither clause of the
    // comparison can be the one doing nothing. Without the tie-break a directory of
    // two CSV and two JSON reads `2 csv · 2 json` on one pass and `2 json · 2 csv`
    // on the next, which is the `read_dir` order this release exists to remove.
    let mut counts = vec![
        (FileFormat::Json, 2),
        (FileFormat::Csv, 2),
        (FileFormat::Parquet, 5),
    ];
    order_formats(&mut counts);
    assert_eq!(
        counts,
        vec![
            (FileFormat::Parquet, 5),
            (FileFormat::Csv, 2),
            (FileFormat::Json, 2)
        ]
    );
}

/// The label and the read name the same format, including on a tie.
///
/// They agreed on the common case and not on a tie: the label sorted equal counts
/// by name and the read put Parquet first, so a directory of two CSV and two Parquet
/// was labelled `2 csv · 2 parquet` and opened as Parquet. One order now, and this
/// is the case that tells the two orders apart.
#[test]
fn the_label_and_the_read_pick_the_same_format_on_a_tie() {
    use crate::FileFormat;
    let tmp = tempfile::TempDir::new().unwrap();
    for name in ["a.csv", "b.csv", "c.parquet", "d.parquet"] {
        std::fs::write(tmp.path().join(name), b"x").unwrap();
    }

    let (_, holds) = look_at_directory(tmp.path());
    assert_eq!(
        holds.formats.first().map(|(f, n)| (f.as_str(), *n)),
        Some(("parquet", 2)),
        "the label names Parquet first: {:?}",
        holds.formats
    );

    match directory_format(tmp.path()) {
        DirectoryFormat::Mixed { format, .. } => assert_eq!(
            format,
            FileFormat::Parquet,
            "and so does the reader the open picks"
        ),
        other => panic!("a directory of two formats is mixed, got {other:?}"),
    }
}

/// A Hugging Face dataset saved to disk is one shard and two JSON files that
/// describe it: a dataset of Arrow, labelled and read as one, the JSON its writer's
/// own. Beside no Arrow, the same names are data.
#[test]
fn a_hugging_face_dataset_is_its_shards() {
    use crate::FileFormat;
    let tmp = tempfile::TempDir::new().unwrap();
    for name in [
        "data-00000-of-00002.arrow",
        "data-00001-of-00002.arrow",
        "dataset_info.json",
        "state.json",
    ] {
        std::fs::write(tmp.path().join(name), b"x").unwrap();
    }
    let (kind, holds) = look_at_directory(tmp.path());
    assert_eq!(kind, EntryKind::MultiFile);
    assert_eq!(holds.formats, [("arrow".to_string(), 2)]);
    assert_eq!(holds.skipped, 2);
    assert_eq!(holds.skipped_names, ["dataset_info.json", "state.json"]);
    match directory_format(tmp.path()) {
        DirectoryFormat::One(FileFormat::Arrow, files) => assert_eq!(files.len(), 2),
        other => panic!("the shards are the dataset, got {other:?}"),
    }

    std::fs::remove_file(tmp.path().join("data-00001-of-00002.arrow")).unwrap();
    assert!(matches!(
        directory_format(tmp.path()),
        DirectoryFormat::One(FileFormat::Arrow, _)
    ));

    let json = tempfile::TempDir::new().unwrap();
    for name in ["state.json", "other.json"] {
        std::fs::write(json.path().join(name), b"{}").unwrap();
    }
    assert_eq!(
        look_at_directory(json.path()).1.formats,
        [("json".to_string(), 2)]
    );
}

#[test]
fn partitions_carry_a_directory_only_while_they_are_the_most_of_it() {
    // The boundary the local rule turns on, and the twin of the cloud route's
    // `partitions_carry_a_prefix_only_while_they_are_the_most_of_it`. Every directory
    // on disk goes through this one.
    let laid_out = |strays: usize| {
        let dir = tempfile::tempdir().unwrap();
        for year in ["year=2024", "year=2025"] {
            let part = dir.path().join(year);
            std::fs::create_dir_all(&part).unwrap();
            write(&part, "data.parquet", &["id"]);
        }
        for i in 0..strays {
            write(dir.path(), &format!("stray-{i}.parquet"), &["id"]);
        }
        classify_directory(dir.path())
    };
    assert_eq!(
        laid_out(2),
        EntryKind::Hive,
        "two partitions against two files beside them"
    );
    assert_ne!(
        laid_out(3),
        EntryKind::Hive,
        "one more file than partitions is a directory that holds a key=value"
    );
}

#[test]
fn a_folder_marker_is_bookkeeping_even_beside_a_partition() {
    // Legacy s3n and EMR write a zero-byte `<name>_$folder$` object beside every
    // prefix. Where the prefix is a partition the marker carries the `=` too, so a
    // partition test that only looks for one calls the marker data and the pane
    // reports one unreadable file per partition.
    assert!(is_bookkeeping("year=2024_$folder$"));
    assert!(is_bookkeeping("alpha_$folder$"));
    assert!(!is_bookkeeping("year=2024"), "the partition itself is data");
    assert!(
        !is_bookkeeping("_date=2024-01-01"),
        "Spark partitions on internal columns"
    );
}

/// And the files under it are what the one-table test is asked about, since they
/// are what the union would hold.
#[test]
fn a_table_hidden_under_a_directory_still_downgrades_it() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["a.parquet", "b.parquet"] {
        write(dir.path(), name, &["id", "ts"]);
    }
    let archive = dir.path().join("archive");
    std::fs::create_dir_all(&archive).unwrap();
    write(
        &archive,
        "other.parquet",
        &["wholly", "different", "columns"],
    );

    let entry = measured(dir.path());
    assert_eq!(
        entry.kind,
        EntryKind::Directory,
        "a union over these is not one table"
    );
    assert_eq!(entry.rows, None);
    // And the width beside `2 parquet` is those two files. The check is asked of
    // everything under the directory, because that is what opening it would union;
    // a downgraded row is never opened as one, so reporting the subtree's union
    // would be a set of columns nothing produces.
    assert_eq!(entry.label(), "2 parquet");
    assert_eq!(entry.cols, Some(2), "id and ts");
    // The names are every column under the directory, because they are what the home
    // screen searches: the directory does hold a `wholly`, one level down.
    assert!(entry.columns.contains(&"wholly".to_string()));
    assert!(entry.columns.contains(&"id".to_string()));

    // And the size is those two files, not the subtree's: three numbers on one row
    // measured over three different sets of files is no row at all.
    let own: u64 = ["a.parquet", "b.parquet"]
        .iter()
        .map(|n| std::fs::metadata(dir.path().join(n)).unwrap().len())
        .sum();
    assert_eq!(entry.size, Some(own));
}

/// A hive tree of CSV is still laid out, whatever its rows cannot say. The layout
/// is directory names — no footers, no opens — and it is the thing you most want
/// before opening a dataset too large to count.
#[test]
fn a_hive_tree_of_another_format_is_still_laid_out() {
    let dir = tempfile::tempdir().unwrap();
    for year in ["year=2024", "year=2025"] {
        let part = dir.path().join(year);
        std::fs::create_dir_all(&part).unwrap();
        std::fs::write(part.join("data.csv"), b"id\n1\n").unwrap();
    }
    // A stray data file at the root, which is a hive root's ordinary furniture.
    std::fs::write(dir.path().join("summary.csv"), b"id\n1\n").unwrap();

    let entry = measured(dir.path());
    assert_eq!(entry.kind, EntryKind::Hive);
    assert!(entry.cost.partitions.is_some(), "the layout is named");
    assert_eq!(entry.rows, None, "and nothing is invented about its rows");
}

/// A hive root's own files are strays beside the partitions — a `schema.json` or a
/// `manifest.csv` left at the top — so its counted format is not its data's, and
/// asking it would blank the whole dataset for one such file.
#[test]
fn a_hive_dataset_is_described_despite_a_stray_file_at_its_root() {
    let dir = tempfile::tempdir().unwrap();
    for year in ["year=2024", "year=2025"] {
        let part = dir.path().join(year);
        std::fs::create_dir_all(&part).unwrap();
        write(&part, "data.parquet", &["id"]);
    }
    std::fs::write(dir.path().join("schema.json"), b"{}").unwrap();

    let entry = measured(dir.path());
    assert_eq!(entry.kind, EntryKind::Hive);
    assert_eq!(entry.holds.one_format(), Some("json"), "its own only file");
    assert_eq!(entry.rows, Some(2), "and the dataset is still counted");
    assert_eq!(entry.cols, Some(2), "`id` and the partition column `year`");
}

/// A hive dataset is described whatever odd file is lying in a partition. One
/// spine cannot tell a Parquet tree with a stray CSV in it from a CSV tree with a
/// stray Parquet, and blanking a dataset that opens perfectly is the worse of the
/// two mistakes.
#[test]
fn a_hive_dataset_is_described_despite_a_stray_file() {
    let dir = tempfile::tempdir().unwrap();
    for year in ["year=2024", "year=2025"] {
        let part = dir.path().join(year);
        std::fs::create_dir_all(&part).unwrap();
        write(&part, "data.parquet", &["id"]);
    }
    // Somebody's notes, dropped in beside the data.
    std::fs::write(dir.path().join("year=2024/notes.csv"), b"x").unwrap();

    let entry = measured(dir.path());
    assert_eq!(entry.kind, EntryKind::Hive);
    assert_eq!(entry.rows, Some(2), "the dataset is still counted");
    assert!(
        entry.cost.partitions.is_some(),
        "and its layout still named"
    );
}

/// A directory is described by its own files, not by what is under them. The footer
/// walk recurses, which is right for a hive root and wrong for a directory of JSON
/// that happens to have Parquet in a subdirectory.
#[test]
fn a_directory_is_not_described_by_files_it_does_not_name() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["a.json", "b.json", "c.json"] {
        std::fs::write(dir.path().join(name), b"{}").unwrap();
    }
    let under = dir.path().join("derived");
    std::fs::create_dir_all(&under).unwrap();
    write(&under, "one.parquet", &["id", "ts", "amount"]);
    write(&under, "two.parquet", &["id", "ts", "amount"]);

    let entry = measured(dir.path());
    assert_eq!(entry.label(), "3 json");
    assert_eq!(
        entry.cols, None,
        "the Parquet below it is not this directory's shape"
    );
    assert_eq!(entry.rows, None);
    assert!(entry.columns.is_empty());
}

/// The gate's default, for a dataset row that counted nothing.
///
/// **Nothing produces this row today.** `enrich` only reaches the gate for `Hive`
/// and `MultiFile`; `look_at_directory` cannot answer `MultiFile` without counting
/// a format, a hive root skips the gate outright, `CLASSIFIER_VERSION` 4 refuses a
/// cached kind that arrives without a tally, and the cloud `(all files)` row is
/// built from a listing and never measured. So this constructs the row by hand, and
/// it pins a default rather than a path.
///
/// It is worth pinning because the default is the arguable one. Turning it away
/// would blank the size, the width and the row count of any such row the moment one
/// appeared, and leaving the counts off a directory is a mistake opening it undoes —
/// giving it another format's numbers is not.
#[test]
fn a_dataset_row_that_counted_nothing_is_still_described() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.parquet", &["id", "ts"]);
    write(dir.path(), "b.parquet", &["id", "ts"]);

    let mut entry = Entry {
        kind: EntryKind::MultiFile,
        ..Entry::for_test(dir.path(), "data")
    };
    assert!(entry.holds.one_format().is_none(), "nothing counted");

    enrich(&mut entry);
    assert!(entry.size.is_some(), "the footers were read");
    assert_eq!(entry.rows, Some(2));
    assert_eq!(entry.cols, Some(2), "id and ts");
}

/// A Parquet file whose name begins with `_` is still a Parquet file. It does not
/// count toward what the directory around it holds — that is what `is_bookkeeping` is
/// for — but the listing shows it, `Enter` opens it, and the row beside it must say
/// how many rows it has rather than nothing at all.
#[test]
fn a_parquet_file_named_like_a_writers_file_is_still_measured() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "_2024_sales.parquet", &["id", "amount"]);

    let mut entry = Entry::new(dir.path().join("_2024_sales.parquet"), EntryKind::File);
    enrich(&mut entry);
    assert_eq!(entry.rows, Some(1), "its footer was read");
    assert_eq!(entry.cols, Some(2));
    assert!(schema_preview(&entry).is_some(), "and the pane shows it");

    // And it still does not make the directory around it a dataset.
    assert!(is_bookkeeping("_2024_sales.parquet"));
    assert_eq!(classify_directory(dir.path()), EntryKind::Directory);
}

/// A hive table's pane lists its partition keys typed the way the scan types them:
/// a date is a date and `true` a boolean, not text.
#[test]
fn a_hive_preview_types_its_keys_as_the_scan_does() {
    use polars::prelude::DataType;
    let dir = tempfile::tempdir().unwrap();
    let leaf = dir.path().join("day=2024-01-02/flag=true/n=3/x=1.5");
    std::fs::create_dir_all(&leaf).unwrap();
    write(&leaf, "part.parquet", &["id"]);
    let mut entry = Entry::directory(dir.path());
    entry.kind = EntryKind::Hive;
    let preview = schema_preview(&entry).expect("a footer to read");
    let types: Vec<(&str, &DataType)> = preview.iter().map(|(n, t)| (n.as_str(), t)).collect();
    assert_eq!(
        types[..4],
        [
            ("day", &DataType::Date),
            ("flag", &DataType::Boolean),
            ("n", &DataType::Int64),
            ("x", &DataType::Float64),
        ]
    );
    assert_eq!(types[4].0, "id");
}

/// Part files with no extension inside a `.parquet` directory are data by where they
/// sit. The cloud route has always counted them; the local one said `dir`.
#[cfg(feature = "cloud")]
#[test]
fn extensionless_part_files_are_data_on_both_routes() {
    let dir = tempfile::tempdir().unwrap();
    let table = dir.path().join("occurrence.parquet");
    std::fs::create_dir_all(&table).unwrap();
    std::fs::write(table.join("000001"), b"PAR1").unwrap();
    std::fs::write(table.join("000002"), b"PAR1").unwrap();

    let objects: Vec<(String, u64)> = [
        "gbif/occurrence.parquet/000001",
        "gbif/occurrence.parquet/000002",
    ]
    .iter()
    .map(|k| ((*k).to_string(), 10u64))
    .collect();

    assert_eq!(
        classify_directory(&table),
        crate::cloud::cloud_browse::look_at_listing("gbif/occurrence.parquet/", &[], &objects).0,
        "the two routes answer the same directory alike"
    );
    assert_eq!(classify_directory(&table), EntryKind::MultiFile);
}

/// And a directory offered as a dataset is one whose files can be counted. The same
/// name test decides both, or the row promises a dataset and shows `?` rows and an
/// empty schema for the rest of the session.
#[test]
fn extensionless_part_files_are_measured_not_just_offered() {
    let dir = tempfile::tempdir().unwrap();
    let table = dir.path().join("occurrence.parquet");
    std::fs::create_dir_all(&table).unwrap();
    // Named as GBIF and Spark leave them: no extension, inside a `.parquet`
    // directory.
    write(&table, "000001", &["id", "species"]);
    write(&table, "000002", &["id", "species"]);

    let entry = measured(&table);
    assert_eq!(entry.kind, EntryKind::MultiFile);
    assert_eq!(entry.rows, Some(2), "both footers were read");
    assert_eq!(entry.cols, Some(2));
    assert!(
        schema_preview(&entry).is_some(),
        "and the schema pane shows what those footers said, rather than asking \
         for a full read of files already read"
    );

    // → goes inside a directory labelled `multi`, so the listing has to show the
    // files the label was counted from — and each is a Parquet file in its own right.
    let mut listed = scan_dir_progressive(&table, |_| {}).entries;
    assert_eq!(
        listed.iter().map(|e| e.name.as_str()).collect::<Vec<_>>(),
        vec!["000001", "000002"],
        "the directory the label promises is not an empty listing"
    );
    let part = listed.first_mut().expect("a part file is listed");
    enrich(part);
    assert_eq!(part.rows, Some(1), "a part file counts its own rows");
    assert_eq!(part.cols, Some(2));
}

/// One `key=value` prefix among files datui does not read is a hive root on both
/// routes. It is not much of one — but the local route has always said so, and the
/// cloud route disagreeing was the divergence. Pinned rather than left to be
/// rediscovered: #275 phase 3 takes the consequence off the label.
#[cfg(feature = "cloud")]
#[test]
fn one_partition_beside_files_datui_cannot_read_answers_alike() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("notes=old")).unwrap();
    for note in ["README.md", "LICENSE", "logo.png"] {
        std::fs::write(dir.path().join(note), b"x").unwrap();
    }
    let objects: Vec<(String, u64)> = ["out/README.md", "out/LICENSE", "out/logo.png"]
        .iter()
        .map(|k| ((*k).to_string(), 12u64))
        .collect();

    assert_eq!(
        classify_directory(dir.path()),
        crate::cloud::cloud_browse::look_at_listing(
            "out/",
            &["out/notes=old/".to_string()],
            &objects
        )
        .0,
        "the two routes answer the same directory alike"
    );
}

/// A partition is a partition whatever it starts with. Spark and Hive partition on
/// internal columns — `_date=2024-01-01`, `_c0=…` — and reading those as a writer's
/// own files loses the whole dataset.
#[test]
fn a_partition_named_like_a_writers_file_is_still_a_partition() {
    let dir = tempfile::tempdir().unwrap();
    let mut directories = Vec::new();
    for day in ["2024-01-01", "2024-01-02", "2024-01-03"] {
        std::fs::create_dir_all(dir.path().join(format!("_date={day}"))).unwrap();
        directories.push(format!("events/_date={day}/"));
    }

    #[cfg(feature = "cloud")]
    assert_eq!(
        classify_directory(dir.path()),
        crate::cloud::cloud_browse::look_at_listing("events/", &directories, &[]).0,
        "the two routes answer the same directory alike"
    );
    assert_eq!(classify_directory(dir.path()), EntryKind::Hive);
    assert!(!is_bookkeeping("_date=2024-01-01"));
    assert!(is_bookkeeping("_temporary"));
}

/// A prefix a writer made for itself is not a directory somebody put data in, on
/// either route. `_temporary/` counted toward the majority in a bucket and not
/// locally, so the same directory came back two different kinds.
#[cfg(feature = "cloud")]
#[test]
fn a_writers_own_directory_is_skipped_on_both_routes() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "part-00000.parquet", &["id"]);
    write(dir.path(), "part-00001.parquet", &["id"]);
    std::fs::create_dir_all(dir.path().join("_temporary")).unwrap();
    std::fs::create_dir_all(dir.path().join("notes")).unwrap();
    std::fs::create_dir_all(dir.path().join("archive")).unwrap();

    let local = classify_directory(dir.path());
    let directories: Vec<String> = ["out/_temporary/", "out/notes/", "out/archive/"]
        .iter()
        .map(|f| (*f).to_string())
        .collect();
    let objects: Vec<(String, u64)> = [
        ("out/part-00000.parquet", 100u64),
        ("out/part-00001.parquet", 100),
    ]
    .iter()
    .map(|(k, s)| ((*k).to_string(), *s))
    .collect();
    let cloud = crate::cloud::cloud_browse::look_at_listing("out/", &directories, &objects).0;

    assert_eq!(
        local, cloud,
        "the two routes answer the same directory alike"
    );
    assert_eq!(local, EntryKind::MultiFile);
}

/// Every entry is in exactly one count, including the ones with nothing behind
/// them. A FIFO and a broken symlink named like data are not data and are not a
/// writer's own; without a count they were in nothing, and the pane said `1 csv`
/// about a directory of three entries.
#[cfg(unix)]
#[test]
fn every_entry_is_in_one_count() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("real.csv"), b"id\n1\n").unwrap();
    std::os::unix::fs::symlink(dir.path().join("gone"), dir.path().join("broken.csv")).unwrap();
    std::fs::write(dir.path().join("notes.md"), b"x").unwrap();
    std::fs::write(dir.path().join("_SUCCESS"), b"").unwrap();

    let holds = look_at_directory(dir.path()).1;
    assert_eq!(holds.data_files(), 1);
    assert_eq!(holds.not_read, 2, "the note and the broken link");
    assert_eq!(holds.skipped, 1);
    assert_eq!(holds.line(true).as_deref(), Some("1 csv"));
}

/// Named like data and impossible to read: a FIFO blocks whoever opens it until a
/// writer appears, and a broken symlink opens as nothing. `directory_format` has
/// always skipped both; the listing now agrees.
#[cfg(unix)]
#[test]
fn a_name_with_nothing_behind_it_is_not_a_data_file() {
    let dir = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(dir.path().join("gone.csv"), dir.path().join("a.csv")).unwrap();
    std::os::unix::fs::symlink(dir.path().join("gone.csv"), dir.path().join("b.csv")).unwrap();
    assert_eq!(
        classify_directory(dir.path()),
        EntryKind::Directory,
        "two broken symlinks are not a dataset"
    );
}

/// A checkpoint directory is the model: its shards are the table, the config and
/// tokenizer JSON beside them are passed over however many there are, and the label
/// names the weights rather than calling the directory mixed.
#[test]
fn a_model_directory_is_its_weights() {
    let dir = tempfile::tempdir().unwrap();
    for name in [
        "model-00001-of-00002.safetensors",
        "model-00002-of-00002.safetensors",
        "config.json",
        "generation_config.json",
        "tokenizer.json",
        "tokenizer_config.json",
        "model.safetensors.index.json",
    ] {
        std::fs::write(dir.path().join(name), b"x").unwrap();
    }
    let DirectoryFormat::Mixed {
        format,
        files,
        passed_over,
    } = directory_format(dir.path())
    else {
        panic!("weights and JSON are two formats");
    };
    assert_eq!(format, crate::FileFormat::Safetensors);
    assert_eq!(files.len(), 3, "the shards, and the index for its metadata");
    assert_eq!(passed_over, [(crate::FileFormat::Json, 4)]);
    let (kind, holds) = look_at_directory(dir.path());
    assert_eq!(kind, EntryKind::MultiFile, "opened as one");
    assert_eq!(holds.label(), "2 safetensors", "the shards, not the index");

    // The index is the model too, named by what it is rather than its extension.
    assert_eq!(
        data_format(Path::new("model.safetensors.index.json")),
        Some(crate::FileFormat::Safetensors)
    );
    // Weights beside another table format are not a model directory.
    std::fs::write(dir.path().join("data.parquet"), b"x").unwrap();
    let (kind, holds) = look_at_directory(dir.path());
    assert_eq!(
        (kind, holds.label().as_str()),
        (EntryKind::Directory, "mixed")
    );
}

/// A model or MIDI file is known by its first bytes under any name.
#[test]
fn signed_files_are_sniffed_by_their_first_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let gguf = dir.path().join("weights");
    std::fs::write(&gguf, b"GGUF\x03\x00\x00\x00").unwrap();
    let st = dir.path().join("checkpoint.bin");
    let mut bytes = 2u64.to_le_bytes().to_vec();
    bytes.extend_from_slice(b"{}");
    std::fs::write(&st, &bytes).unwrap();
    let text = dir.path().join("notes");
    std::fs::write(&text, b"just some text").unwrap();
    assert_eq!(sniff_format(&gguf), Some(crate::FileFormat::Gguf));
    let opened = |path: &Path| crate::formats::readers::sniff_open(path, None);
    assert_eq!(opened(&st), Some(crate::FileFormat::Safetensors));
    assert_eq!(opened(&text), None);
    let midi = dir.path().join("song.bin");
    std::fs::write(&midi, b"MThd\0\0\0\x06\0\0\0\x01\0\x60").unwrap();
    assert_eq!(opened(&midi), Some(crate::FileFormat::Midi));
}

/// A directory is offered as one dataset only when its format can be read as many
/// files. `.tsv`, `.psv` and Excel have a single-file reader and nothing that takes
/// a list, so offering them puts the refusal one keystroke later instead of not
/// making the promise.
#[test]
fn a_format_that_cannot_be_read_as_many_is_not_offered_as_one() {
    for ext in ["tsv", "psv", "xlsx", "xlsb"] {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(format!("a.{ext}")), b"x").unwrap();
        std::fs::write(dir.path().join(format!("b.{ext}")), b"x").unwrap();
        assert_eq!(
            classify_directory(dir.path()),
            EntryKind::Directory,
            "a directory of .{ext} has no reader that takes a list"
        );
    }
    // The ones that do are unaffected — every arm the multi-path open handles.
    for ext in [
        "parquet", "csv", "json", "jsonl", "ndjson", "arrow", "arrows", "ipc", "feather", "avro",
        "orc",
    ] {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(format!("a.{ext}")), b"x").unwrap();
        std::fs::write(dir.path().join(format!("b.{ext}")), b"x").unwrap();
        assert_eq!(
            classify_directory(dir.path()),
            EntryKind::MultiFile,
            ".{ext} reads as many files"
        );
    }
}

/// The readdir-order bug: eight Parquet files and a ninth entry that is a writer's
/// own file. A probe of the first eight entries never saw the JSON and said `multi`;
/// a bucket listing sorts `_metadata.json` first and said `dir`. Same directory, two
/// answers, decided by the order the filesystem happened to return.
#[cfg(feature = "cloud")]
#[test]
fn a_writers_own_file_is_skipped_whatever_order_it_is_listed_in() {
    let dir = tempfile::tempdir().unwrap();
    for part in 0..8 {
        write(dir.path(), &format!("{part}.parquet"), &["season"]);
    }
    std::fs::write(dir.path().join("_metadata.json"), b"{}").unwrap();

    let local = classify_directory(dir.path());
    // The same directory as a bucket lists it: lexicographic, so the JSON comes
    // first.
    let mut keys: Vec<(String, u64)> = vec![("jolpica/2000/_metadata.json".into(), 2)];
    for part in 0..8 {
        keys.push((format!("jolpica/2000/{part}.parquet"), 100));
    }
    keys.sort();
    let cloud = crate::cloud::cloud_browse::look_at_listing("jolpica/2000/", &[], &keys).0;

    assert_eq!(
        local, cloud,
        "the two routes answer the same directory alike"
    );
    assert_eq!(local, EntryKind::MultiFile);
}

/// The files a job leaves beside its output are skipped on every route, not just
/// the two names each route happened to know.
#[test]
fn job_files_are_skipped_on_every_route() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "part-00000.parquet", &["id"]);
    write(dir.path(), "part-00001.parquet", &["id"]);
    for marker in [
        "_SUCCESS",
        "_committed_1727",
        "_committed_1728",
        "_started_1727",
        ".part.crc",
    ] {
        std::fs::write(dir.path().join(marker), b"").unwrap();
    }
    assert_eq!(
        classify_directory(dir.path()),
        EntryKind::MultiFile,
        "five markers beside two data files do not outvote them"
    );

    #[cfg(feature = "cloud")]
    let keys: Vec<(String, u64)> = [
        ("out/_SUCCESS", 0u64),
        ("out/_committed_1727", 12),
        ("out/_committed_1728", 12),
        ("out/_started_1727", 12),
        ("out/.part.crc", 8),
        ("out/part-00000.parquet", 100),
        ("out/part-00001.parquet", 100),
    ]
    .iter()
    .map(|(k, s)| ((*k).to_string(), *s))
    .collect();
    #[cfg(feature = "cloud")]
    assert_eq!(
        crate::cloud::cloud_browse::look_at_listing("out/", &[], &keys).0,
        EntryKind::MultiFile,
        "and the same in a bucket"
    );
}

/// Write `columns` as a one-row Parquet file named `name` under `dir`.
fn write(dir: &Path, name: &str, columns: &[&str]) {
    let mut frame = DataFrame::new(
        1,
        columns
            .iter()
            .map(|c| Column::new((*c).into(), &[1i32]))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let file = std::fs::File::create(dir.join(name)).unwrap();
    ParquetWriter::new(file).finish(&mut frame).unwrap();
}

/// A one-row Parquet file with a struct column, so the leaves and the columns a
/// reader sees are genuinely different things rather than dots in a name.
fn write_nested(dir: &Path, name: &str, struct_name: &str, fields: &[&str]) {
    let inner = DataFrame::new(
        1,
        fields
            .iter()
            .map(|f| Column::new((*f).into(), &[1i32]))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let nested = inner
        .into_struct(struct_name.into())
        .into_series()
        .into_column();
    let mut frame = DataFrame::new(1, vec![Column::new("id".into(), &[1i32]), nested]).unwrap();
    let file = std::fs::File::create(dir.join(name)).unwrap();
    ParquetWriter::new(file).finish(&mut frame).unwrap();
}

fn measured(dir: &Path) -> Entry {
    let (kind, holds) = look_at_directory(dir);
    let mut entry = Entry::new(dir.to_path_buf(), kind);
    entry.holds = holds;
    enrich(&mut entry);
    entry
}

/// The shape that prompted this: one Parquet file per table, sharing an extension
/// and nothing else. Named for what it is rather than what it is called, because
/// the filenames are exactly what cannot decide it.
/// A record cached before the rename still says how many subdirectories it saw.
#[test]
fn holds_written_as_folders_still_reads() {
    let old: Holds = serde_json::from_str(r#"{"folders":3,"partitions":2}"#).unwrap();
    assert_eq!((old.directories, old.partitions), (3, 2));
    let new = serde_json::to_string(&old).unwrap();
    assert!(new.contains(r#""directories":3"#), "{new}");
}

#[test]
fn a_directory_of_separate_tables_is_not_a_dataset() {
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "circuits.parquet",
        &["circuit_id", "lat", "lng"],
    );
    write(
        dir.path(),
        "drivers.parquet",
        &["driver_id", "code", "nationality"],
    );
    write(
        dir.path(),
        "laps.parquet",
        &["lap", "position", "time_millis"],
    );

    assert_eq!(
        classify_directory(dir.path()),
        EntryKind::MultiFile,
        "the filenames alone still say multi"
    );
    let entry = measured(dir.path());
    assert_eq!(
        entry.kind,
        EntryKind::Directory,
        "reading the footers says otherwise"
    );
    assert_eq!(
        entry.rows, None,
        "a sum across separate tables is not a row count"
    );
    assert_eq!(
        entry.cols,
        Some(9),
        "the union of what the directory holds is still a true answer to what is in it"
    );
    assert_eq!(entry.label(), "3 parquet", "and the label counts the files");
}

/// The rows of one table split across files, which is what `multi` is for.
/// The directories the old threshold took as one table and nesting does not.
///
/// Two files that each bring a column the other lacks — a renamed column is the
/// everyday case — scored two thirds against a bar of a half, so they opened as one
/// table and the union carried both spellings with nulls under each. Nothing datui
/// can see tells that apart from two tables that share most of their columns, which
/// is why the number moved rather than the question.
///
/// The directory is not refused. It is a place to look inside, and the row inside it
/// opens the union anyway.
#[test]
fn a_directory_whose_files_each_bring_a_column_is_a_place_to_look_inside() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "old.parquet", &["id", "ts", "amount"]);
    write(dir.path(), "new.parquet", &["id", "ts", "amt"]);

    assert_eq!(
        classify_directory(dir.path()),
        EntryKind::MultiFile,
        "the names alone still say two Parquet files"
    );
    let entry = measured(dir.path());
    assert_eq!(
        entry.kind,
        EntryKind::Directory,
        "and the footers say neither file's columns are in the other's"
    );
    assert_eq!(entry.label(), "2 parquet", "which the label still reports");
    assert_eq!(entry.rows, None, "a sum over two tables is not a number");
}

#[test]
fn a_directory_of_one_table_stays_a_dataset() {
    let dir = tempfile::tempdir().unwrap();
    for part in 0..3 {
        write(
            dir.path(),
            &format!("part-0000{part}.parquet"),
            &["id", "ts", "amount"],
        );
    }

    let entry = measured(dir.path());
    assert_eq!(entry.kind, EntryKind::MultiFile);
    assert_eq!(entry.rows, Some(3));
    assert_eq!(entry.cols, Some(3));
}

/// A dataset whose columns changed over time is still one dataset. This is the
/// case a rule about shared columns gets wrong: the older files have a third of
/// what the newest one does.
#[test]
fn a_dataset_that_gained_columns_stays_a_dataset() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "2009.parquet", &["id", "ts"]);
    write(dir.path(), "2015.parquet", &["id", "ts", "fee"]);
    write(
        dir.path(),
        "2025.parquet",
        &["id", "ts", "fee", "witness", "address", "value"],
    );

    let entry = measured(dir.path());
    assert_eq!(entry.kind, EntryKind::MultiFile);
    assert_eq!(entry.rows, Some(3));
    assert_eq!(
        entry.columns,
        vec!["id", "ts", "fee", "witness", "address", "value"],
        "every column any file has, in the order they first appear — not the \
         2009 shape"
    );
    assert_eq!(entry.cols, Some(6), "and the count is of those");
}

/// The same, for a hive tree: the row is the dataset's columns, not one
/// partition's.
#[test]
fn a_hive_dataset_that_gained_columns_reports_all_of_them() {
    let dir = tempfile::tempdir().unwrap();
    for (part, columns) in [
        ("year=2009", &["id", "ts"][..]),
        ("year=2025", &["id", "ts", "address"][..]),
    ] {
        let sub = dir.path().join(part);
        std::fs::create_dir_all(&sub).unwrap();
        write(&sub, "part-0.parquet", columns);
    }

    let entry = measured(dir.path());
    assert_eq!(entry.kind, EntryKind::Hive);
    assert_eq!(entry.columns, vec!["id", "ts", "address"]);
    assert_eq!(entry.cols, Some(4), "and the partition column `year`");
}

/// Past the counting limit the columns come from a spread of the directory rather
/// than its head, because a directory written over time is narrowest at the start.
#[test]
fn a_directory_too_large_to_count_still_reports_the_columns_it_gained() {
    let dir = tempfile::tempdir().unwrap();
    for part in 0..MAX_FOOTERS_PER_DATASET + 1 {
        let mut columns = vec!["id".to_string(), "ts".to_string()];
        if part > MAX_FOOTERS_PER_DATASET / 2 {
            columns.push("address".to_string());
        }
        let refs: Vec<&str> = columns.iter().map(String::as_str).collect();
        write(dir.path(), &format!("part-{part:03}.parquet"), &refs);
    }

    let entry = measured(dir.path());
    assert_eq!(entry.kind, EntryKind::MultiFile, "still one table");
    assert_eq!(entry.rows, None, "too many files to count");
    assert!(
        entry.columns.contains(&"address".to_string()),
        "the column the dataset gained is in the row: {:?}",
        entry.columns
    );
}

/// A lake table's data files agree on a schema, so the one-table rule says `multi`
/// and is right about the schema and wrong about the rows: the files a delete
/// tombstoned are still on disk, every rewritten version is here together, and
/// compaction leaves both sides in place.
#[test]
fn a_lake_table_is_not_a_directory_of_parquet_files() {
    for (marker, expected) in [
        ("_delta_log", EntryKind::Delta),
        (".hoodie", EntryKind::Hudi),
    ] {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "part-0.parquet", &["id", "amount"]);
        write(dir.path(), "part-1.parquet", &["id", "amount"]);
        write(dir.path(), "part-2.parquet", &["id", "amount"]);
        let log = dir.path().join(marker);
        std::fs::create_dir_all(&log).unwrap();
        std::fs::write(log.join("00000000000000000000.json"), b"{}").unwrap();

        assert_eq!(
            classify_directory(dir.path()),
            expected,
            "{marker} says what this directory is"
        );
        let entry = measured(dir.path());
        assert_eq!(entry.kind, expected);
        assert_eq!(
            entry.rows, None,
            "and no row count is claimed for it: summing the footers would count \
             the rows the log says are gone"
        );
        assert!(!entry.kind.is_dataset(), "it does not open as one table");
    }
}

/// Iceberg's marker is a plain name, so it takes the whole shape rather than the
/// name alone.
#[test]
fn an_iceberg_root_is_metadata_beside_data() {
    let iceberg = tempfile::tempdir().unwrap();
    let data = iceberg.path().join("data");
    let metadata = iceberg.path().join("metadata");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::create_dir_all(&metadata).unwrap();
    write(&data, "00000-0-abc.parquet", &["id", "amount"]);
    write(&data, "00001-0-def.parquet", &["id", "amount"]);
    std::fs::write(metadata.join("v2.metadata.json"), b"{}").unwrap();
    std::fs::write(metadata.join("snap-1.avro"), b"x").unwrap();
    assert_eq!(classify_directory(iceberg.path()), EntryKind::Iceberg);

    // A directory that merely has those names is not a table.
    let plain = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(plain.path().join("data")).unwrap();
    std::fs::create_dir_all(plain.path().join("metadata")).unwrap();
    std::fs::write(plain.path().join("metadata/notes.txt"), b"x").unwrap();
    assert_eq!(
        classify_directory(plain.path()),
        EntryKind::Directory,
        "no *.metadata.json, so no Iceberg table"
    );

    let no_data = tempfile::tempdir().unwrap();
    let metadata = no_data.path().join("metadata");
    std::fs::create_dir_all(&metadata).unwrap();
    std::fs::write(metadata.join("v1.metadata.json"), b"{}").unwrap();
    write(no_data.path(), "part-0.parquet", &["id"]);
    write(no_data.path(), "part-1.parquet", &["id"]);
    assert_eq!(
        classify_directory(no_data.path()),
        EntryKind::MultiFile,
        "metadata with no data/ beside it is somebody's directory, not a table root"
    );
}

/// A single file counts its columns the same way a directory does, and both count
/// what opening it shows.
///
/// `enrich_parquet` read `schema_descr.columns()`, which is the leaf list — so a file
/// with one struct of two fields said `columns 3` above a schema list of two, and a
/// directory holding only that file said something different again.
#[test]
fn a_file_and_a_directory_of_it_count_the_same_columns() {
    let dir = tempfile::tempdir().unwrap();
    write_nested(dir.path(), "one.parquet", "inputs", &["address", "value"]);

    let mut file = Entry::new(dir.path().join("one.parquet"), EntryKind::File);
    enrich(&mut file);
    assert_eq!(
        file.cols,
        Some(2),
        "`id` and `inputs`, which is what opening it shows: {:?}",
        file.columns
    );
    assert!(
        file.columns.iter().any(|c| c == "inputs.address"),
        "the leaves are still searchable: {:?}",
        file.columns
    );

    write_nested(dir.path(), "two.parquet", "inputs", &["address", "value"]);
    let directory = measured(dir.path());
    assert_eq!(directory.kind, EntryKind::MultiFile);
    assert_eq!(
        directory.cols, file.cols,
        "and a directory of them says the same number"
    );
}

/// Dots in a column's own name are not nesting, and are not counted as if they were.
///
/// The obvious fix for the leaf problem — split the dotted path and count the roots —
/// gets this wrong: `user.id` and `user.name` written by a flattening export are two
/// columns, not one. The schema says which is which; the string cannot.
#[test]
fn a_dotted_column_name_is_its_own_column() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "flat.parquet", &["id", "user.id", "user.name"]);

    let mut file = Entry::new(dir.path().join("flat.parquet"), EntryKind::File);
    enrich(&mut file);
    assert_eq!(file.cols, Some(3), "three columns: {:?}", file.columns);
}

/// A directory whose files encode the same nested column differently counts it once.
///
/// The union is over leaf paths, and the same nested column written by parquet-mr and
/// by Arrow gives different leaves — so the row reported roughly twice the width of a
/// directory `is_one_table` had just called one dataset. Counted from each file's own
/// root fields, the two spellings are one `inputs` whatever the leaves under it are.
#[test]
fn a_writer_change_does_not_double_the_column_count() {
    let dir = tempfile::tempdir().unwrap();
    write_nested(dir.path(), "old.parquet", "inputs", &["address"]);
    // The same column, one field wider, as a later writer left it.
    write_nested(dir.path(), "new.parquet", "inputs", &["address", "value"]);

    let entry = measured(dir.path());
    assert_eq!(entry.kind, EntryKind::MultiFile, "still one table");
    assert_eq!(
        entry.cols,
        Some(2),
        "one `inputs`, not one per shape of it: {:?}",
        entry.columns
    );
    assert!(
        entry.columns.len() > 2,
        "while every leaf stays searchable: {:?}",
        entry.columns
    );
}

/// A count read from a spread of a directory rather than all of it says it is a
/// floor.
#[test]
fn a_sampled_column_count_says_it_is_a_floor() {
    let dir = tempfile::tempdir().unwrap();
    for part in 0..MAX_FOOTERS_PER_DATASET * 2 {
        write(
            dir.path(),
            &format!("part-{part:04}.parquet"),
            &["id", "ts"],
        );
    }
    let entry = measured(dir.path());
    assert_eq!(entry.rows, None, "too many files to count");
    assert!(entry.cols.is_some(), "but the width is still worth having");
    assert!(
        entry.cols_sampled,
        "and it is marked as the floor it is, not presented as a total"
    );

    // A directory small enough to read every footer of claims no such thing.
    let small = tempfile::tempdir().unwrap();
    write(small.path(), "a.parquet", &["id", "ts"]);
    write(small.path(), "b.parquet", &["id", "ts"]);
    assert!(!measured(small.path()).cols_sampled);
}

/// The files a directory offers come back in order, whatever order it was
/// written in.
///
/// Every caller reads order as meaning something — `sample_footers` takes the ends
/// and the middle, and the union of the columns is built in the order the files
/// appear. Unsorted, "the last file" was whichever one the filesystem happened to
/// return last, which on the filesystems that return creation order is the one
/// written first as often as not.
#[test]
fn the_files_a_directory_offers_come_back_in_order() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["c.parquet", "a.parquet", "d.parquet", "b.parquet"] {
        write(dir.path(), name, &["id"]);
    }
    let files = parquet_files_under(dir.path());
    let names: Vec<String> = files
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        names,
        vec!["a.parquet", "b.parquet", "c.parquet", "d.parquet"],
        "sorted, not in the order the directory was written"
    );
}

/// A directory past the budget still says so, and the files it keeps are the
/// directory's first rather than the listing's.
///
/// The ordering itself is `the_files_a_directory_offers_come_back_in_order`'s to
/// prove: a directory read may return sorted entries of its own accord, so an
/// assertion here about order could hold for the wrong reason. What this pins is
/// *which* files survive the cap, and that the cap still says "too many to count".
#[test]
fn a_directory_past_the_budget_keeps_the_directories_first_files() {
    let dir = tempfile::tempdir().unwrap();
    for part in 0..MAX_FOOTERS_PER_DATASET * 3 {
        write(dir.path(), &format!("part-{part:04}.parquet"), &["id"]);
    }
    let files = parquet_files_under(dir.path());

    assert_eq!(
        files.len(),
        MAX_FOOTERS_PER_DATASET + 1,
        "one past the budget, which is what says there are too many to count"
    );
    let names: Vec<String> = files
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    let expected: Vec<String> = (0..=MAX_FOOTERS_PER_DATASET)
        .map(|part| format!("part-{part:04}.parquet"))
        .collect();
    // Not "sorted", which a directory read may be of its own accord, but the
    // directory's own first sixty-five. Sorting after truncating gives sixty-five
    // sorted names from wherever the read began, which is a different set.
    assert_eq!(
        names, expected,
        "the directory's first files, not the listing's"
    );
}

/// The log is named rather than looked for, so a table's own data files cannot
/// crowd it out of the listing however many of them there are.
#[test]
fn a_lake_table_is_recognized_among_its_data_files() {
    let dir = tempfile::tempdir().unwrap();
    for part in 0..32 {
        write(dir.path(), &format!("part-{part:03}.parquet"), &["id"]);
    }
    std::fs::create_dir_all(dir.path().join("_delta_log")).unwrap();
    assert_eq!(classify_directory(dir.path()), EntryKind::Delta);
}

/// Past the counting limit the row count is out of reach, but whether the directory
/// is one table is not — and a directory of a hundred tables is exactly where reading
/// them as one costs most.
#[test]
fn a_directory_too_large_to_count_is_still_checked() {
    let dir = tempfile::tempdir().unwrap();
    for table in 0..MAX_FOOTERS_PER_DATASET + 1 {
        write(
            dir.path(),
            &format!("table_{table:03}.parquet"),
            &[&format!("{table}_id"), &format!("{table}_value")],
        );
    }

    let entry = measured(dir.path());
    assert_eq!(entry.kind, EntryKind::Directory);
    assert_eq!(entry.rows, None, "too many files to count either way");
}

/// The same directory size, but one table split across it.
#[test]
fn a_large_directory_of_one_table_stays_a_dataset() {
    let dir = tempfile::tempdir().unwrap();
    for part in 0..MAX_FOOTERS_PER_DATASET + 1 {
        write(
            dir.path(),
            &format!("part-{part:05}.parquet"),
            &["id", "ts"],
        );
    }

    let entry = measured(dir.path());
    assert_eq!(entry.kind, EntryKind::MultiFile);
}

/// Searching the home screen by column should still find a directory that holds one,
/// even once the directory is no longer offered as a single table.
#[test]
fn a_downgraded_directory_keeps_every_column_its_files_have() {
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "circuits.parquet",
        &["circuit_id", "lat", "lng"],
    );
    write(
        dir.path(),
        "drivers.parquet",
        &["driver_id", "code", "nationality"],
    );
    // And one a level down, so "every column its files have" is a claim about more
    // than the directory's own: the row's width is its own files, its names are
    // everything under it, and a fixture with no subdirectory cannot tell those
    // apart.
    let seasons = dir.path().join("seasons");
    std::fs::create_dir_all(&seasons).unwrap();
    write(&seasons, "2024.parquet", &["season_year", "round"]);

    let entry = measured(dir.path());
    assert_eq!(entry.kind, EntryKind::Directory);
    for column in [
        "circuit_id",
        "lat",
        "lng",
        "driver_id",
        "code",
        "nationality",
        "season_year",
        "round",
    ] {
        assert!(
            entry.columns.iter().any(|c| c == column),
            "{column} in {:?}",
            entry.columns
        );
    }
}

#[test]
fn a_name_no_reader_takes_is_refused_before_opening() {
    let refused = |name: &str| unreadable_by_name(std::path::Path::new(name));
    assert!(refused("gs://b/ml/onnx/pipeline_rf.onnx"));
    assert!(refused("model.onnx.gz"));
    assert!(refused("README.md"));
    for readable in [
        "a.csv",
        "a.CSV",
        "a.csv.gz",
        "a.parquet",
        "a.xlsx",
        "data.gz",
        "part-0000",
    ] {
        assert!(!refused(readable), "{readable}");
    }
}
