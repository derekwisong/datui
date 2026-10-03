//! GPS logs: NMEA 0183 and GPX, read into a table.
//!
//! Neither format can be scanned in place, so an open reads the file once, start to
//! end, a piece at a time, and writes the rows to a temporary Arrow IPC file a batch
//! at a time. The dataset scans that file lazily like any other, and memory stays at
//! one batch however long the log. The file is claimed through the open's
//! [`Writer`], so quitting or replacing the open removes it.
//!
//! A GPX file can find a new extension field a million points in: the rows after it
//! go to a new segment, a second IPC file with the wider schema, and the frame is the
//! segments joined with the missing columns null. An NMEA table's columns are fixed,
//! so it is always one segment.
//!
//! The readers take bytes as they come and hand back rows as they are ready, so a log
//! still being written can later be followed by reading on from where they stopped.

pub mod gpx;
pub mod nmea;
pub mod table;

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use color_eyre::Result;
use color_eyre::eyre::eyre;
use polars::prelude::*;

use crate::download::TempDownload;
use crate::notes::Note;
use crate::numfmt::group_chrome;
use crate::segments::{Converted, Segments};
use crate::unfinished::Writer;
use crate::{CompressionFormat, FileFormat, OpenOptions};

/// What datui does with an NMEA log: see [`crate::readers`].
pub(crate) const NMEA: crate::readers::Reader = crate::readers::Reader {
    // Several logs are one table, and say nothing besides their rows.
    convert: Some(|input| {
        let converted = convert(
            input.files,
            input.display,
            input.format,
            input.options,
            input.writer,
            input.read,
        )?;
        Ok((converted, None))
    }),
    scan: crate::readers::read_into,
    signatures: &[crate::readers::Signature {
        says: |head, _| nmea::looks_like(head),
        kind: crate::readers::Kind::Text,
        trusted: crate::readers::Trusted {
            listing: false,
            ..crate::readers::EVERYWHERE
        },
    }],
    ..crate::readers::BASE
};

/// What datui does with a GPX file: see [`crate::readers`].
pub(crate) const GPX: crate::readers::Reader = crate::readers::Reader {
    // Several logs are one table, and say nothing besides their rows.
    convert: Some(|input| {
        let converted = convert(
            input.files,
            input.display,
            input.format,
            input.options,
            input.writer,
            input.read,
        )?;
        Ok((converted, None))
    }),
    scan: crate::readers::read_into,
    signatures: &[crate::readers::Signature {
        says: |head, _| gpx::looks_like(head),
        kind: crate::readers::Kind::Text,
        trusted: crate::readers::Trusted {
            listing: false,
            ..crate::readers::EVERYWHERE
        },
    }],
    ..crate::readers::BASE
};

/// How much of the file is read at a time.
const CHUNK: usize = 1 << 16;

/// A reader that counts the bytes read through it, for the loading screen.
struct Counted<'a, R> {
    inner: R,
    read: &'a AtomicU64,
}

impl<R: Read> Read for Counted<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.read.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
}

/// The file, decompressed as it is read when its name or `--compression` says so;
/// `read` counts the bytes of the file as stored.
pub(crate) fn open_reader<'a>(
    file: &Path,
    options: &OpenOptions,
    read: &'a AtomicU64,
) -> Result<Box<dyn Read + 'a>> {
    let f = BufReader::new(Counted {
        inner: File::open(file)?,
        read,
    });
    Ok(
        match options
            .compression
            .or_else(|| CompressionFormat::from_extension(file))
        {
            None => Box::new(f),
            Some(CompressionFormat::Gzip) => Box::new(flate2::read::MultiGzDecoder::new(f)),
            Some(CompressionFormat::Zstd) => Box::new(zstd::Decoder::new(f)?),
            Some(CompressionFormat::Bzip2) => Box::new(bzip2::read::BzDecoder::new(f)),
            Some(CompressionFormat::Xz) => Box::new(xz2::read::XzDecoder::new(f)),
        },
    )
}

/// Read `files` (named `display` to the user when there is one) as `format` into
/// temporary IPC files, written through `writer`, counting the bytes of the files read
/// in `read`. Several files are one table with a `file` column first, each file's
/// columns its own and the others null; what the reads noticed is counted across them.
pub(crate) fn convert(
    files: &[PathBuf],
    display: &Path,
    format: FileFormat,
    options: &OpenOptions,
    writer: &Writer,
    read: &AtomicU64,
) -> Result<Converted> {
    let [file] = files else {
        return convert_many(files, format, options, writer, read);
    };
    let one = convert_one(file, display, format, options, writer, read)?;
    let (notes, other_tables) = one.stats.notes(None, options)?;
    Ok(Converted {
        lf: one.lf,
        files: one.files,
        notes,
        other_tables,
    })
}

/// Each of `files` read as [`convert_one`] reads it, then stacked under a `file`
/// column. A GPX file's fields are typed in that file, and stacked as one type where
/// two files disagree.
fn convert_many(
    files: &[PathBuf],
    format: FileFormat,
    options: &OpenOptions,
    writer: &Writer,
    read: &AtomicU64,
) -> Result<Converted> {
    let mut frames = Vec::with_capacity(files.len());
    let mut written = Vec::new();
    let mut stats: Option<Stats> = None;
    for file in files {
        let one = convert_one(file, file, format, options, writer, read)
            .map_err(|e| crate::error_display::in_file(file, e))?;
        let name = file
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| file.display().to_string());
        frames.push(one.lf.select([lit(name).alias("file"), all().as_expr()]));
        written.extend(one.files);
        match &mut stats {
            None => stats = Some(one.stats),
            Some(stats) => stats.absorb(one.stats),
        }
    }
    let stats = stats.ok_or_else(|| eyre!("No GPS logs to read."))?;
    let lf = concat(
        &frames,
        UnionArgs {
            diagonal: true,
            to_supertypes: true,
            ..Default::default()
        },
    )?;
    let (notes, other_tables) = stats.notes(Some(files.len()), options)?;
    Ok(Converted {
        lf,
        files: written,
        notes,
        other_tables,
    })
}

/// What one log's read noticed, of either format.
enum Stats {
    Gpx {
        stats: gpx::Stats,
        /// Files that end inside an element.
        truncated: usize,
    },
    Nmea {
        stats: nmea::Stats,
        /// Files with rows and no sentence that gives the date.
        undated: usize,
    },
}

impl Stats {
    /// Count `other`, another file's, in with these.
    fn absorb(&mut self, other: Stats) {
        match (self, other) {
            (
                Stats::Gpx { stats, truncated },
                Stats::Gpx {
                    stats: more,
                    truncated: also,
                },
            ) => {
                stats.points += more.points;
                stats.tracks += more.tracks;
                stats.routes += more.routes;
                stats.waypoints += more.waypoints;
                stats.fields_dropped += more.fields_dropped;
                stats.bad_times += more.bad_times;
                stats.truncated |= more.truncated;
                *truncated += also;
            }
            (
                Stats::Nmea { stats, undated },
                Stats::Nmea {
                    stats: more,
                    undated: also,
                },
            ) => {
                stats.absorb(&more);
                *undated += also;
            }
            // One format per open.
            _ => {}
        }
    }

    /// The notes for the Notes tab, and the other tables for the Schema tab, of `of`
    /// files (`None` for one).
    fn notes(&self, of: Option<usize>, options: &OpenOptions) -> Result<(Vec<Note>, Vec<String>)> {
        Ok(match self {
            Stats::Gpx { stats, truncated } => (gpx_notes(stats, of, *truncated), Vec::new()),
            Stats::Nmea { stats, undated } => {
                let table = nmea_table(options)?;
                (
                    nmea_notes(stats, of, *undated),
                    nmea_other_tables(stats, table),
                )
            }
        })
    }
}

/// The NMEA table `--table` names, the fixes when it names none.
fn nmea_table(options: &OpenOptions) -> Result<nmea::Table> {
    match options.table.as_deref() {
        None => Ok(nmea::Table::Fixes),
        Some(name) => nmea::Table::from_name(name).ok_or_else(|| {
            eyre!(
                "no table \"{name}\". --table names one of an NMEA log's tables: {}.",
                nmea::Table::ALL.map(nmea::Table::name).join(", ")
            )
        }),
    }
}

/// One log read: the frame over its segments, the files they are, and its counts.
struct One {
    lf: LazyFrame,
    files: Vec<TempDownload>,
    stats: Stats,
}

/// Read `file` (named `display` to the user) as `format` into temporary IPC files.
fn convert_one(
    file: &Path,
    display: &Path,
    format: FileFormat,
    options: &OpenOptions,
    writer: &Writer,
    read: &AtomicU64,
) -> Result<One> {
    let mut reader = open_reader(file, options, read)?;
    let mut segments = Segments::new(options, writer);
    let mut chunk = vec![0u8; CHUNK];
    let mut next = |chunk: &mut [u8]| -> Result<usize> {
        if writer.stopped() {
            return Err(eyre!("Reading was stopped."));
        }
        loop {
            match reader.read(chunk) {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
        }
    };
    match format {
        FileFormat::Gpx => {
            let mut gpx = gpx::GpxReader::new();
            loop {
                let n = next(&mut chunk)?;
                if n == 0 {
                    break;
                }
                gpx.push(&chunk[..n]).map_err(|e| eyre!(e))?;
                if let Some(df) = gpx.take_batch()? {
                    segments.write(&df)?;
                }
            }
            let last = gpx.finish().map_err(|e| eyre!(e))?;
            segments.write(&last)?;
            let (lf, files) = segments.finish()?;
            let stats = gpx.stats().clone();
            Ok(One {
                lf: type_gpx_fields(lf, gpx.fields()),
                files,
                stats: Stats::Gpx {
                    truncated: usize::from(stats.truncated),
                    stats,
                },
            })
        }
        FileFormat::Nmea => {
            let table = nmea_table(options)?;
            let mut log = nmea::NmeaReader::new(table);
            loop {
                let n = next(&mut chunk)?;
                if n == 0 {
                    break;
                }
                log.push(&chunk[..n]);
                if let Some(df) = log.take_batch()? {
                    segments.write(&df)?;
                }
            }
            let last = log.finish()?;
            if log.stats().sentences == 0 {
                return Err(crate::error_display::FileError::new(
                    display,
                    "no NMEA sentences: no line starts with $ and a sentence address.",
                )
                .into());
            }
            segments.write(&last)?;
            let (lf, files) = segments.finish()?;
            let stats = log.stats().clone();
            Ok(One {
                lf,
                files,
                stats: Stats::Nmea {
                    undated: usize::from(!stats.dated && stats.rows > 0),
                    stats,
                },
            })
        }
        other => Err(eyre!("{} is not a GPS format.", other.name())),
    }
}

/// A GPX field column as numbers when every value in it was one.
fn type_gpx_fields(lf: LazyFrame, fields: &[gpx::FieldColumn]) -> LazyFrame {
    let casts: Vec<Expr> = fields
        .iter()
        .filter_map(|f| {
            let to = if f.integers {
                DataType::Int64
            } else if f.numbers {
                DataType::Float64
            } else {
                return None;
            };
            Some(col(f.name.as_str()).cast(to))
        })
        .collect();
    if casts.is_empty() {
        lf
    } else {
        lf.with_columns(casts)
    }
}

fn note(summary: String, scope: String) -> Note {
    Note {
        summary,
        scope,
        read_as_text: None,
        passed_over: None,
    }
}

fn count(n: u64, one: &str, many: &str) -> String {
    let n = usize::try_from(n).unwrap_or(usize::MAX);
    format!("{} {}", group_chrome(n), if n == 1 { one } else { many })
}

/// What reading an NMEA log noticed; of `of` logs, `undated` of them without a date.
fn nmea_notes(stats: &nmea::Stats, of: Option<usize>, undated: usize) -> Vec<Note> {
    let mut notes = Vec::new();
    let of_lines = match of {
        None => format!("of {}", count(stats.lines, "line", "lines")),
        Some(n) => format!(
            "of {} in {}",
            count(stats.lines, "line", "lines"),
            count(n as u64, "log", "logs")
        ),
    };
    if stats.skipped > 0 {
        notes.push(note(
            format!(
                "{} not NMEA and left out",
                count(stats.skipped, "line is", "lines are")
            ),
            of_lines.clone(),
        ));
    }
    if stats.bad_checksums > 0 {
        notes.push(note(
            format!(
                "{} its checksum; checksum_ok is false on its rows",
                count(stats.bad_checksums, "sentence fails", "sentences fail")
            ),
            format!("of {}", count(stats.sentences, "sentence", "sentences")),
        ));
    }
    if undated > 0 {
        let summary = match of {
            None => "No RMC or ZDA sentence gives the date, so time is empty".to_string(),
            Some(n) => format!(
                "{undated} of {n} logs {} no RMC or ZDA sentence to give the date, so their time is empty",
                if undated == 1 { "has" } else { "have" }
            ),
        };
        notes.push(note(summary, of_lines.clone()));
    }
    notes
}

/// The tables of an NMEA log besides `table`, each named as `--table` takes it with the
/// sentences it holds, when one of them holds rows `table` does not show: a sentence
/// type the fixes do not merge (GSA, GSV, ZDA), or any other type beside one type's
/// table. Empty otherwise, so a log of fixes only says nothing.
fn nmea_other_tables(stats: &nmea::Stats, table: nmea::Table) -> Vec<String> {
    use nmea::Table;
    let typed = |t: Table| !matches!(t, Table::Fixes | Table::Sentences);
    let merged = |t: Table| matches!(t, Table::Gga | Table::Rmc | Table::Vtg | Table::Gll);
    let worth = Table::ALL.into_iter().any(|t| {
        t != table && typed(t) && stats.of(t.name()) > 0 && !(table == Table::Fixes && merged(t))
    });
    if !worth {
        return Vec::new();
    }
    Table::ALL
        .into_iter()
        .filter(|t| *t != table)
        .filter_map(|t| match typed(t) {
            false => Some(t.name().to_string()),
            true => {
                let n = stats.of(t.name());
                (n > 0).then(|| format!("{} {}", t.name(), group_chrome(n as usize)))
            }
        })
        .collect()
}

/// What reading a GPX file noticed; of `of` files, `truncated` of them cut short.
fn gpx_notes(stats: &gpx::Stats, of: Option<usize>, truncated: usize) -> Vec<Note> {
    let mut notes = Vec::new();
    let of_points = match of {
        None => format!("of {}", count(stats.points, "point", "points")),
        Some(n) => format!(
            "of {} in {}",
            count(stats.points, "point", "points"),
            count(n as u64, "file", "files")
        ),
    };
    if truncated > 0 {
        let summary = match of {
            None => "The file ends inside an element; the points before it are shown".to_string(),
            Some(n) => format!(
                "{truncated} of {n} files {} inside an element; the points before it are shown",
                if truncated == 1 { "ends" } else { "end" }
            ),
        };
        notes.push(note(summary, of_points.clone()));
    }
    if stats.bad_times > 0 {
        notes.push(note(
            format!(
                "{} not ISO 8601 and left empty",
                count(stats.bad_times, "time is", "times are")
            ),
            of_points.clone(),
        ));
    }
    if stats.fields_dropped > 0 {
        notes.push(note(
            format!(
                "{} past the {} columns kept, or named too long, and left out",
                count(stats.fields_dropped, "field value is", "field values are"),
                gpx::MAX_FIELDS
            ),
            of_points,
        ));
    }
    notes
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every way a GPS log is refused names the file, in the one shape.
    #[test]
    fn errors_name_the_file() {
        use crate::readers::bad_input::{assert_shape, each_names_its_file, opening};
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
        let converted = convert(
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
        let converted = convert(
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
        let converted = convert(
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
            summaries[0].starts_with("1 line is not NMEA"),
            "{summaries:?}"
        );
        assert_eq!(summaries.len(), 1, "{summaries:?}");
        assert_eq!(
            converted.other_tables,
            ["RMC 1", "GSV 1", "sentences"],
            "a GSV the fixes do not show"
        );
        let gsv = convert(
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
        let converted = convert(
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
            [(
                "1 time is not ISO 8601 and left empty",
                "of 3 points in 2 files"
            )]
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
        let converted = convert(
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
            ["1 of 2 logs has no RMC or ZDA sentence to give the date, so their time is empty"]
        );
        assert_eq!(
            converted.other_tables,
            ["GGA 1", "RMC 1", "GSV 1", "sentences"]
        );
    }
}
