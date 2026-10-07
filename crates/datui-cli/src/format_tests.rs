use super::{FileFormat, FormatChoice, ReadMode, RemoteRead, Stored, Summary};

/// `ALL` is the list `from_name` searches, so a format missing from it cannot be
/// read back from a stored name. The match below is exhaustive, so a new variant
/// does not compile until it is written here — the reminder to add it to `ALL`,
/// which nothing can enforce outright.
#[test]
fn every_format_is_listed() {
    fn listed(f: FileFormat) -> bool {
        match f {
            FileFormat::Parquet
            | FileFormat::Csv
            | FileFormat::Tsv
            | FileFormat::Psv
            | FileFormat::Json
            | FileFormat::Jsonl
            | FileFormat::Arrow
            | FileFormat::Avro
            | FileFormat::Orc
            | FileFormat::Excel
            | FileFormat::Safetensors
            | FileFormat::Gguf
            | FileFormat::Nmea
            | FileFormat::Gpx
            | FileFormat::Audio
            | FileFormat::Midi
            | FileFormat::Sqlite
            | FileFormat::Vcd
            | FileFormat::Fix
            | FileFormat::Sdf
            | FileFormat::Numpy
            | FileFormat::Elf
            | FileFormat::Ulog
            | FileFormat::Dataflash
            | FileFormat::Candump
            | FileFormat::Text
            | FileFormat::Journal => FileFormat::ALL.contains(&f),
        }
    }
    for format in FileFormat::ALL {
        assert!(listed(format), "{format:?}");
        assert_eq!(FileFormat::from_name(format.name()), Some(format));
    }

    // And the names themselves, because they are not only labels. `Holds` keeps a
    // format as this string and the dataset cache writes it out, so renaming one
    // changes what every directory of that format reads *and* makes the records
    // already on disk unreadable — `from_name` then answers `None`, which the
    // enrich gate takes for "not Parquet" and blanks the directory's size. The round
    // trip above holds for any string; this is what says which.
    assert_eq!(
        FileFormat::ALL.map(|f| f.name()),
        [
            "parquet",
            "csv",
            "tsv",
            "psv",
            "json",
            "jsonl",
            "arrow",
            "avro",
            "orc",
            "excel",
            "safetensors",
            "gguf",
            "nmea",
            "gpx",
            "audio",
            "midi",
            "sqlite",
            "vcd",
            "fix",
            "sdf",
            "numpy",
            "elf",
            "ulog",
            "dataflash",
            "candump",
            "text",
            "journal"
        ]
    );
}

/// Each way a file can sit on disk reads as the format says, and the formats that
/// take the whole file into memory are the ones whose readers do.
#[test]
fn read_mode_by_format_and_storage() {
    use ReadMode::*;
    let plain = |f: FileFormat| f.read_mode(Stored::Plain);
    assert_eq!(plain(FileFormat::Parquet), Some(Lazy));
    assert_eq!(plain(FileFormat::Csv), Some(Lazy));
    assert_eq!(plain(FileFormat::Arrow), Some(Lazy));
    assert_eq!(plain(FileFormat::Audio), Some(Lazy));
    assert_eq!(plain(FileFormat::Sqlite), Some(Lazy));
    assert_eq!(plain(FileFormat::Nmea), Some(Converted));
    assert_eq!(plain(FileFormat::Gpx), Some(Converted));
    for f in [
        FileFormat::Json,
        FileFormat::Jsonl,
        FileFormat::Avro,
        FileFormat::Orc,
        FileFormat::Excel,
        FileFormat::Safetensors,
        FileFormat::Gguf,
        FileFormat::Midi,
    ] {
        assert_eq!(plain(f), Some(InMemory), "{}", f.name());
    }
    for f in FileFormat::ALL {
        assert!(plain(f).is_some(), "every format opens: {}", f.name());
    }

    // An IPC stream is converted; nothing else is one.
    assert_eq!(FileFormat::Arrow.read_mode(Stored::Stream), Some(Converted));
    assert_eq!(FileFormat::Parquet.read_mode(Stored::Stream), None);

    // Compressed text is decompressed once to a file, or read in memory when asked;
    // a GPS log is decompressed as it is converted; nothing else opens compressed.
    let compressed = |f: FileFormat, in_memory| f.read_mode(Stored::Compressed { in_memory });
    for f in [FileFormat::Csv, FileFormat::Tsv, FileFormat::Psv] {
        assert_eq!(compressed(f, false), Some(Decompressed));
        assert_eq!(compressed(f, true), Some(InMemory));
    }
    assert_eq!(compressed(FileFormat::Nmea, true), Some(Converted));
    assert_eq!(compressed(FileFormat::Parquet, false), None);
    assert_eq!(compressed(FileFormat::Json, false), None);
    assert_eq!(compressed(FileFormat::Sqlite, false), None);

    // A spec maps its file, or the decompressed copy of it.
    let spec = FormatChoice::Spec("acme.l2feed".into());
    assert_eq!(spec.read_mode(Stored::Plain), Some(Lazy));
    assert_eq!(
        spec.read_mode(Stored::Compressed { in_memory: true }),
        Some(Decompressed)
    );
    assert_eq!(spec.bucket_object(Stored::Plain), RemoteRead::Downloaded);

    // Parquet objects, Arrow IPC files and model headers are read in place; CSV
    // and NDJSON prefixes are too. Over HTTP only a model's header is.
    let model = |f: FileFormat| matches!(f, FileFormat::Safetensors | FileFormat::Gguf);
    for f in FileFormat::ALL {
        let in_place = f.bucket_object(Stored::Plain) == RemoteRead::InPlace;
        assert_eq!(
            in_place,
            matches!(f, FileFormat::Parquet | FileFormat::Arrow) || model(f),
            "{}",
            f.name()
        );
        assert_eq!(
            f.bucket_object(Stored::Compressed { in_memory: false }),
            RemoteRead::Downloaded
        );
        assert_eq!(
            f.http_file() == RemoteRead::InPlace,
            model(f),
            "{}",
            f.name()
        );
    }
    assert_eq!(spec.http_file(), RemoteRead::Downloaded);
    let prefixes: Vec<_> = FileFormat::ALL
        .into_iter()
        .filter(|f| f.reads_bucket_prefix())
        .collect();
    assert_eq!(
        prefixes,
        [
            FileFormat::Parquet,
            FileFormat::Csv,
            FileFormat::Jsonl,
            FileFormat::Arrow,
            FileFormat::Safetensors,
            FileFormat::Gguf
        ]
    );
    // Streams have no footer: one object, or a prefix of them, is downloaded.
    assert_eq!(
        FileFormat::Arrow.bucket_object(Stored::Stream),
        RemoteRead::Downloaded
    );
    assert_eq!(
        FileFormat::Arrow.bucket_prefix(Stored::Stream),
        Some(RemoteRead::Downloaded)
    );
}

/// The dataset-info page's table of tabs says what each descriptor says: the
/// format's tab of the Info panel, and what the home screen lists inside a file of
/// it. Every format has a row, matched by its title.
#[test]
fn the_docs_tab_table_agrees_with_the_descriptors() {
    let page = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/user-guide/dataset-info.md");
    let text = std::fs::read_to_string(&page).expect("the dataset-info page");
    let header = "| Format | Tab | Lists inside the file |";
    let start = text.find(header).expect("the table of tabs");
    let mut seen: Vec<FileFormat> = Vec::new();
    for line in text[start..]
        .lines()
        .skip(2)
        .take_while(|l| l.starts_with('|'))
    {
        let cells: Vec<&str> = line.trim_matches('|').split(" | ").map(str::trim).collect();
        let [formats, tab, tables] = cells[..] else {
            panic!("three cells: {line}");
        };
        for title in formats.split(", ") {
            let title = title.trim_matches('*');
            let format = FileFormat::ALL
                .into_iter()
                .find(|f| f.title().eq_ignore_ascii_case(title))
                .unwrap_or_else(|| panic!("{title} is a format's title"));
            seen.push(format);
            let said = match format.descriptor().summary {
                Summary::Tab(tab) => format!("**{tab}**"),
                Summary::None(why) => {
                    assert!(text.contains(why), "{title}: the page says why: {why}");
                    "none".to_string()
                }
            };
            assert_eq!(tab, said, "{title}: Tab");
            let listed = format
                .descriptor()
                .tables
                .as_ref()
                .map_or("no", |_| "tables");
            assert_eq!(tables, listed, "{title}: Lists inside the file");
        }
    }
    for f in FileFormat::ALL {
        assert!(seen.contains(&f), "{} has a row", f.title());
    }
}
