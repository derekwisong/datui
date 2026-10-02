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
use polars::io::ipc::BatchedWriter;
use polars::prelude::*;

use crate::download::TempDownload;
use crate::notes::Note;
use crate::numfmt::group_chrome;
use crate::unfinished::Writer;
use crate::{CompressionFormat, FileFormat, OpenOptions};

/// How much of the file is read at a time.
const CHUNK: usize = 1 << 16;
/// Bytes looked at to tell a GPS log by its content.
const HEAD: usize = 4096;

/// Whether `format` is one of the GPS formats read here.
pub fn is_gps(format: FileFormat) -> bool {
    matches!(format, FileFormat::Nmea | FileFormat::Gpx)
}

/// The GPS format a name says, looking through a compression suffix:
/// `track.nmea.gz` is NMEA.
pub fn format_by_name(path: &Path) -> Option<FileFormat> {
    FileFormat::from_path(path)
        .or_else(|| {
            CompressionFormat::from_extension(path)?;
            FileFormat::from_path(Path::new(path.file_stem()?))
        })
        .filter(|f| is_gps(*f))
}

/// The GPS format the first bytes of a file say, if they say one.
pub fn sniff(head: &[u8]) -> Option<FileFormat> {
    if nmea::looks_like(head) {
        Some(FileFormat::Nmea)
    } else if gpx::looks_like(head) {
        Some(FileFormat::Gpx)
    } else {
        None
    }
}

/// The GPS format of the file at `path` by its first bytes. Only for a file whose
/// name says no format, so a file that opens as something today opens the same way.
pub fn sniff_path(path: &Path) -> Option<FileFormat> {
    let mut head = Vec::with_capacity(HEAD);
    File::open(path)
        .ok()?
        .take(HEAD as u64)
        .read_to_end(&mut head)
        .ok()?;
    sniff(&head)
}

/// What a conversion made: the frame over its segments, the files they are, which the
/// dataset holds for as long as it scans them, and what the read noticed.
pub(crate) struct Converted {
    pub lf: LazyFrame,
    pub files: Vec<TempDownload>,
    pub notes: Vec<Note>,
    /// The file's other tables, for the Info panel's Schema tab; empty when none would
    /// show anything this one does not.
    pub other_tables: Vec<String>,
}

/// The temporary IPC files a conversion writes, one per schema.
struct Segments<'a> {
    dir: Option<PathBuf>,
    writer: &'a Writer,
    open: Option<(BatchedWriter<File>, TempDownload, Schema)>,
    done: Vec<(TempDownload, Schema)>,
}

impl<'a> Segments<'a> {
    fn new(options: &OpenOptions, writer: &'a Writer) -> Self {
        Self {
            dir: options.temp_dir.clone(),
            writer,
            open: None,
            done: Vec::new(),
        }
    }

    /// Write `df`, starting a segment when it is the first or its columns differ.
    /// An empty batch is written only when there is no segment yet, so an empty log
    /// still has a table with its columns.
    fn write(&mut self, df: &DataFrame) -> Result<()> {
        if self.writer.stopped() {
            return Err(eyre!("Reading was stopped."));
        }
        let schema = df.schema().as_ref().clone();
        let same = self.open.as_ref().is_some_and(|(_, _, s)| *s == schema);
        if df.height() == 0 && self.open.is_some() {
            return Ok(());
        }
        if !same {
            self.close()?;
            let Some((named, claim)) = self
                .writer
                .create(|| TempDownload::create(self.dir.as_deref(), Some("arrow")))?
            else {
                return Err(eyre!("Reading was stopped."));
            };
            let file = named.as_file().try_clone()?;
            let held = TempDownload::held(named, Some(claim));
            let batched = IpcWriter::new(file).batched(&schema, ipc_fields(&schema))?;
            self.open = Some((batched, held, schema));
        }
        let (batched, _, _) = self.open.as_mut().expect("opened just above");
        batched.write_batch(df)?;
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        if let Some((mut batched, held, schema)) = self.open.take() {
            batched.finish()?;
            self.done.push((held, schema));
        }
        Ok(())
    }

    /// The segments as one frame, each given the columns it lacks as nulls.
    fn finish(mut self) -> Result<(LazyFrame, Vec<TempDownload>)> {
        self.close()?;
        // Columns only ever grow, so the last segment has them all, in order.
        let full = self
            .done
            .last()
            .map(|(_, schema)| schema.clone())
            .ok_or_else(|| eyre!("Nothing was read."))?;
        let mut frames = Vec::with_capacity(self.done.len());
        for (file, schema) in &self.done {
            let lf = LazyFrame::scan_ipc(
                PlRefPath::try_from_path(file.path())?,
                Default::default(),
                Default::default(),
            )?;
            let columns: Vec<Expr> = full
                .iter()
                .map(|(name, dtype)| match schema.contains(name) {
                    true => col(name.clone()),
                    false => lit(NULL).cast(dtype.clone()).alias(name.clone()),
                })
                .collect();
            frames.push(lf.select(columns));
        }
        let lf = match frames.len() {
            1 => frames.pop().expect("one"),
            _ => concat(&frames, UnionArgs::default())?,
        };
        Ok((lf, self.done.into_iter().map(|(file, _)| file).collect()))
    }
}

/// The IPC fields of a schema with no dictionaries, which is every schema here.
fn ipc_fields(schema: &Schema) -> Vec<polars_arrow::io::ipc::IpcField> {
    let arrow = schema.to_arrow(CompatLevel::newest());
    polars_arrow::io::ipc::write::default_ipc_fields(arrow.iter_values())
}

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
fn open_reader<'a>(
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

/// Read `file` (named `display` to the user) as `format` into temporary IPC files,
/// written through `writer`, counting the bytes of `file` read in `read`.
pub(crate) fn convert(
    file: &Path,
    display: &Path,
    format: FileFormat,
    options: &OpenOptions,
    writer: &Writer,
    read: &AtomicU64,
) -> Result<Converted> {
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
    let notes = match format {
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
            let lf = type_gpx_fields(lf, gpx.fields());
            return Ok(Converted {
                lf,
                files,
                notes: gpx_notes(gpx.stats()),
                other_tables: Vec::new(),
            });
        }
        FileFormat::Nmea => {
            let table = match options.table.as_deref() {
                None => nmea::Table::Fixes,
                Some(name) => nmea::Table::from_name(name).ok_or_else(|| {
                    eyre!(
                        "No table {name:?} in an NMEA log. The tables are: {}.",
                        nmea::Table::ALL.map(nmea::Table::name).join(", ")
                    )
                })?,
            };
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
                return Err(eyre!(
                    "No NMEA sentences in {}: no line starts with $ and a sentence address.",
                    display.display()
                ));
            }
            segments.write(&last)?;
            (
                nmea_notes(log.stats()),
                nmea_other_tables(log.stats(), table),
            )
        }
        other => return Err(eyre!("{} is not a GPS format.", other.name())),
    };
    let (notes, other_tables) = notes;
    let (lf, files) = segments.finish()?;
    Ok(Converted {
        lf,
        files,
        notes,
        other_tables,
    })
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

/// What reading an NMEA log noticed.
fn nmea_notes(stats: &nmea::Stats) -> Vec<Note> {
    let mut notes = Vec::new();
    let of_lines = format!("of {}", count(stats.lines, "line", "lines"));
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
    if !stats.dated && stats.rows > 0 {
        notes.push(note(
            "No RMC or ZDA sentence gives the date, so time is empty".to_string(),
            of_lines.clone(),
        ));
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

/// What reading a GPX file noticed.
fn gpx_notes(stats: &gpx::Stats) -> Vec<Note> {
    let mut notes = Vec::new();
    let of_points = format!("of {}", count(stats.points, "point", "points"));
    if stats.truncated {
        notes.push(note(
            "The file ends inside an element; the points before it are shown".to_string(),
            of_points.clone(),
        ));
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

    #[test]
    fn names_and_first_bytes_say_the_format() {
        assert_eq!(format_by_name(Path::new("a.nmea")), Some(FileFormat::Nmea));
        assert_eq!(format_by_name(Path::new("a.GPX")), Some(FileFormat::Gpx));
        assert_eq!(
            format_by_name(Path::new("a.nmea.gz")),
            Some(FileFormat::Nmea)
        );
        assert_eq!(format_by_name(Path::new("a.csv")), None);
        assert_eq!(sniff(b"$GPGGA,1,2"), Some(FileFormat::Nmea));
        assert_eq!(
            sniff(b"<?xml version=\"1.0\"?>\n<gpx>"),
            Some(FileFormat::Gpx)
        );
        assert_eq!(sniff(b"a,b\n1,2"), None);
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
            &path,
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
            &path,
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
            &path,
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
            &path,
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
                &text,
                &text,
                FileFormat::Nmea,
                &options(dir.path()),
                &writer,
                &AtomicU64::default()
            )
            .is_err()
        );
    }
}
