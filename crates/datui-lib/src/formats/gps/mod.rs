//! GPS logs (NMEA 0183, GPX) as a table. Neither scans in place, so an open reads the
//! file once in pieces, writing rows a batch at a time to a temp Arrow IPC file (claimed
//! via the open's `Writer`) that the dataset scans lazily. A GPX extension field
//! appearing late starts a new segment file with the wider schema, joined with nulls;
//! NMEA is always one segment. Readers are incremental, so a growing log can be
//! followed later.

pub mod gpx;
pub mod nmea;

use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;

use color_eyre::Result;
use color_eyre::eyre::eyre;
use polars::prelude::*;

use crate::cloud::download::TempDownload;
use crate::formats::segments::Converted;
use crate::formats::text_formats::{count, note, read_through};
use crate::loading::unfinished::Writer;
use crate::notes::Note;
use crate::numfmt::group_chrome;
use crate::{FileFormat, OpenOptions};

/// What datui does with an NMEA log: see [`crate::formats::readers`].
pub(crate) const NMEA: crate::formats::readers::Reader = crate::formats::readers::Reader {
    // Several logs are one table.
    convert: Some(|input| {
        let (converted, detail) = convert(
            input.files,
            input.display,
            input.format,
            input.options,
            input.writer,
            input.read,
        )?;
        Ok((converted, Some(std::sync::Arc::new(detail))))
    }),
    scan: crate::formats::readers::read_into,
    signatures: &[crate::formats::readers::Signature {
        says: |head, _| nmea::looks_like(head),
        kind: crate::formats::readers::Kind::Text,
        trusted: crate::formats::readers::Trusted {
            listing: false,
            tables: true,
            ..crate::formats::readers::EVERYWHERE
        },
    }],
    // The tables are the sentence types read, whichever the log holds: listing them
    // reads nothing.
    tables: Some(|_| Ok(nmea_tables())),
    ..crate::formats::readers::BASE
};

/// The tables of an NMEA log, as the home screen lists them and `--table` names them.
fn nmea_tables() -> Vec<crate::formats::sqlite::Table> {
    nmea::Table::ALL
        .into_iter()
        .map(|t| {
            let columns = t.columns().into_iter().map(|(name, _)| name);
            crate::formats::sqlite::Table::plain(t.name(), "table", columns)
        })
        .collect()
}

/// What datui does with a GPX file: see [`crate::formats::readers`].
pub(crate) const GPX: crate::formats::readers::Reader = crate::formats::readers::Reader {
    // Several logs are one table.
    convert: Some(|input| {
        let (converted, detail) = convert(
            input.files,
            input.display,
            input.format,
            input.options,
            input.writer,
            input.read,
        )?;
        Ok((converted, Some(std::sync::Arc::new(detail))))
    }),
    scan: crate::formats::readers::read_into,
    signatures: &[crate::formats::readers::Signature {
        says: |head, _| gpx::looks_like(head),
        kind: crate::formats::readers::Kind::Text,
        trusted: crate::formats::readers::Trusted {
            listing: false,
            ..crate::formats::readers::EVERYWHERE
        },
    }],
    ..crate::formats::readers::BASE
};

/// Read `files` (shown as `display`) as `format` into temp IPC files via `writer`,
/// counting bytes read in `read`. Several files form one table with a leading `file`
/// column; read notes are summed for Info's GPS tab.
pub(crate) fn convert(
    files: &[PathBuf],
    display: &Path,
    format: FileFormat,
    options: &OpenOptions,
    writer: &Writer,
    read: &AtomicU64,
) -> Result<(Converted, crate::formats::text_formats::Detail)> {
    let [file] = files else {
        return convert_many(files, format, options, writer, read);
    };
    let one = convert_one(file, display, format, options, writer, read)?;
    let (notes, other_tables) = one.stats.notes(None, options)?;
    let detail = one.stats.detail(None, &one.extent, options)?;
    Ok((
        Converted {
            lf: one.lf,
            files: one.files,
            notes,
            other_tables,
        },
        detail,
    ))
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
) -> Result<(Converted, crate::formats::text_formats::Detail)> {
    let mut frames = Vec::with_capacity(files.len());
    let mut written = Vec::new();
    let mut stats: Option<Stats> = None;
    let mut extent = Extent::default();
    for file in files {
        let one = convert_one(file, file, format, options, writer, read)
            .map_err(|e| crate::error_display::in_file(file, e))?;
        let name = file
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| file.display().to_string());
        frames.push(one.lf.select([lit(name).alias("file"), all().as_expr()]));
        written.extend(one.files);
        extent.merge(&one.extent);
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
    let detail = stats.detail(Some(files.len()), &extent, options)?;
    Ok((
        Converted {
            lf,
            files: written,
            notes,
            other_tables,
        },
        detail,
    ))
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
                stats.long_names += more.long_names;
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

impl Stats {
    /// The GPS tab of the Info panel, for `of` files (`None` for one) whose rows
    /// covered `extent`.
    fn detail(
        &self,
        of: Option<usize>,
        extent: &Extent,
        options: &OpenOptions,
    ) -> Result<crate::formats::text_formats::Detail> {
        use crate::formats::model_files::MetaValue;
        let middot = crate::glyphs::get().middot;
        let mut lines = Vec::new();
        let (list_title, list) = match self {
            Stats::Gpx { stats, .. } => {
                lines.push(
                    [
                        count(stats.points, "point", "points"),
                        count(stats.tracks, "track", "tracks"),
                        count(stats.routes, "route", "routes"),
                        count(stats.waypoints, "waypoint", "waypoints"),
                    ]
                    .join(&format!(" {middot} ")),
                );
                ("", Vec::new())
            }
            Stats::Nmea { stats, .. } => {
                let table = nmea_table(options)?;
                lines.push(format!(
                    "{} of {} {middot} {} of {}",
                    count(stats.rows, "row", "rows"),
                    table.name(),
                    count(stats.sentences, "sentence", "sentences"),
                    count(stats.lines, "line", "lines"),
                ));
                let mut list: Vec<(String, MetaValue)> = stats
                    .types
                    .iter()
                    .map(|(t, n)| (t.clone(), MetaValue::Text(group_chrome(*n as usize))))
                    .collect();
                if stats.other_types > 0 {
                    list.push((
                        "other".to_string(),
                        MetaValue::Text(group_chrome(stats.other_types as usize)),
                    ));
                }
                ("Sentences", list)
            }
        };
        if let Some(n) = of {
            lines.push(count(n as u64, "file", "files"));
        }
        lines.extend(extent.lines());
        Ok(crate::formats::text_formats::Detail {
            tab: crate::formats::text_formats::tab(FileFormat::Nmea),
            lines,
            list_title,
            list,
            ..Default::default()
        })
    }
}

/// When and where a log's rows are: the least and most of their `time`, `lat` and
/// `lon`, taken from each batch as it is written, so nothing is read twice.
#[derive(Debug, Clone, Default, PartialEq)]
struct Extent {
    /// Milliseconds since the epoch, UTC.
    time: Option<(i64, i64)>,
    lat: Option<(f64, f64)>,
    lon: Option<(f64, f64)>,
}

impl Extent {
    fn absorb(&mut self, df: &DataFrame) {
        let time = df
            .column("time")
            .ok()
            .and_then(|c| c.as_materialized_series().datetime().ok().cloned())
            .and_then(|t| Some((t.physical().min()?, t.physical().max()?)));
        let span = |name: &str| {
            let c = df.column(name).ok()?;
            let v = c.as_materialized_series().f64().ok()?;
            Some((v.min()?, v.max()?))
        };
        self.merge(&Extent {
            time,
            lat: span("lat"),
            lon: span("lon"),
        });
    }

    fn merge(&mut self, other: &Extent) {
        fn wider<T: PartialOrd + Copy>(a: Option<(T, T)>, b: Option<(T, T)>) -> Option<(T, T)> {
            match (a, b) {
                (Some((a0, a1)), Some((b0, b1))) => {
                    Some((if b0 < a0 { b0 } else { a0 }, if b1 > a1 { b1 } else { a1 }))
                }
                (a, b) => a.or(b),
            }
        }
        self.time = wider(self.time, other.time);
        self.lat = wider(self.lat, other.lat);
        self.lon = wider(self.lon, other.lon);
    }

    /// `Time: 2024-05-01 12:00:00 to 13:05:12 UTC (1:05:12.000)` and
    /// `Bounds: 51.40000 to 51.60000 N, -0.20000 to 0.10000 E`.
    fn lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        if let Some((first, last)) = self.time {
            let at = |ms: i64| chrono::DateTime::from_timestamp_millis(ms);
            if let (Some(a), Some(b)) = (at(first), at(last)) {
                let end = if a.date_naive() == b.date_naive() {
                    b.format("%H:%M:%S").to_string()
                } else {
                    b.format("%Y-%m-%d %H:%M:%S").to_string()
                };
                lines.push(format!(
                    "Time: {} to {end} UTC ({})",
                    a.format("%Y-%m-%d %H:%M:%S"),
                    crate::widgets::info::clock((last - first) as f64 / 1000.0)
                ));
            }
        }
        if let (Some((lat0, lat1)), Some((lon0, lon1))) = (self.lat, self.lon) {
            lines.push(format!(
                "Bounds: latitude {lat0:.5} to {lat1:.5}, longitude {lon0:.5} to {lon1:.5}"
            ));
        }
        lines
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
    extent: Extent,
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
    let mut extent = Extent::default();
    match format {
        FileFormat::Gpx => {
            let mut gpx = gpx::GpxReader::new();
            let (lf, files) = read_through(file, options, writer, read, &mut gpx, |df| {
                extent.absorb(df)
            })?;
            let stats = gpx.stats().clone();
            Ok(One {
                lf: type_gpx_fields(lf, gpx.fields()),
                files,
                extent,
                stats: Stats::Gpx {
                    truncated: usize::from(stats.truncated),
                    stats,
                },
            })
        }
        FileFormat::Nmea => {
            let mut log = nmea::NmeaReader::new(nmea_table(options)?);
            let (lf, files) = read_through(file, options, writer, read, &mut log, |df| {
                extent.absorb(df)
            })?;
            if log.stats().sentences == 0 {
                return Err(crate::error_display::FileError::new(
                    display,
                    "no NMEA sentences: no line starts with $ and a sentence address.",
                )
                .into());
            }
            let stats = log.stats().clone();
            Ok(One {
                lf,
                files,
                extent,
                stats: Stats::Nmea {
                    undated: usize::from(!stats.dated && stats.rows > 0),
                    stats,
                },
            })
        }
        other => Err(eyre!("{} is not a GPS format.", other.name())),
    }
}

impl crate::formats::text_formats::BatchReader for gpx::GpxReader {
    fn push(&mut self, piece: &[u8]) -> Result<()> {
        self.push(piece).map_err(|e| eyre!(e))
    }

    fn take_batch(&mut self) -> PolarsResult<Option<DataFrame>> {
        self.take_batch()
    }

    fn finish(&mut self) -> Result<DataFrame> {
        self.finish().map_err(|e| eyre!(e))
    }
}

impl crate::formats::text_formats::BatchReader for nmea::NmeaReader {
    fn push(&mut self, piece: &[u8]) -> Result<()> {
        self.push(piece);
        Ok(())
    }

    fn take_batch(&mut self) -> PolarsResult<Option<DataFrame>> {
        self.take_batch()
    }

    fn finish(&mut self) -> Result<DataFrame> {
        Ok(self.finish()?)
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
                "{} left out: not NMEA",
                count(stats.skipped, "line", "lines")
            ),
            of_lines.clone(),
        ));
    }
    if stats.bad_checksums > 0 {
        notes.push(note(
            format!(
                "checksum failed: {} {} checksum_ok false",
                count(stats.bad_checksums, "sentence", "sentences"),
                crate::glyphs::get().middot
            ),
            format!("of {}", count(stats.sentences, "sentence", "sentences")),
        ));
    }
    if undated > 0 {
        let summary = match of {
            None => format!(
                "no RMC or ZDA date {} time empty",
                crate::glyphs::get().middot
            ),
            Some(n) => format!(
                "{undated} of {n} logs without an RMC or ZDA date {} time empty",
                crate::glyphs::get().middot
            ),
        };
        notes.push(note(summary, of_lines.clone()));
    }
    notes
}

/// An NMEA log's other tables (as `--table` names them, with their sentences) when one
/// holds rows `table` lacks (GSA, GSV, ZDA, or another type's table); empty for a
/// fixes-only log.
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
            None => format!(
                "file cut short inside an element {} points before it shown",
                crate::glyphs::get().middot
            ),
            Some(n) => format!(
                "{truncated} of {n} files cut short inside an element {} points before it shown",
                crate::glyphs::get().middot
            ),
        };
        notes.push(note(summary, of_points.clone()));
    }
    if stats.bad_times > 0 {
        notes.push(note(
            format!(
                "{} left empty: not ISO 8601",
                count(stats.bad_times, "time", "times")
            ),
            of_points.clone(),
        ));
    }
    if stats.fields_dropped > 0 {
        notes.push(note(
            crate::limits::left_out(
                &count(stats.fields_dropped, "field value", "field values"),
                crate::limits::get().gpx_fields,
                "gpx_fields",
            ),
            of_points.clone(),
        ));
    }
    if stats.long_names > 0 {
        notes.push(note(
            format!(
                "{} left out: field name too long",
                count(stats.long_names, "field value", "field values"),
            ),
            of_points,
        ));
    }
    notes
}

#[cfg(test)]
mod tests;
