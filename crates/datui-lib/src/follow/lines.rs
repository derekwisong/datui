//! Following an NDJSON file, journal JSON included, or one piped in: read by a scan of
//! datui's own that stops at the last newline written. A file still being written
//! usually ends partway through an object, which no JSON parser takes, and Polars'
//! NDJSON reader fails the whole read on it even when told to skip bad lines. Every
//! read of the file goes through this scan: the open's schema and summary, a count, a
//! filter, a page read from the start.

use std::fs::File;
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use polars::prelude::*;

use super::Spool;

/// The name the scan carries in a plan.
const SCAN_NAME: &str = "NDJSON";

/// The scan of a followed NDJSON file: its complete lines from the start, as many rows
/// as the bound asks for.
pub(crate) struct LinesScan {
    path: PathBuf,
    schema: SchemaRef,
    ignore_errors: bool,
    /// Standard input being copied to the file: once it has ended, a last line with
    /// no newline is a line too.
    spool: Option<Arc<Spool>>,
    /// The handle a deleted file is read through.
    held: Option<Arc<File>>,
}

/// How much of `bytes`, a file written a line at a time, is whole lines: through the
/// last newline, or all of it once nothing more is coming. Looks back from the end
/// only as far as the last line.
pub(crate) fn complete(bytes: &[u8], ended: bool) -> usize {
    if ended {
        return bytes.len();
    }
    memchr::memrchr(b'\n', bytes).map_or(0, |at| at + 1)
}

/// Where the line after the first `rows` lines that are not blank starts in `bytes`:
/// a read of that many rows stops there. Blank lines are not rows, as Polars reads them.
fn after_rows(bytes: &[u8], rows: usize) -> usize {
    let mut start = 0;
    let mut seen = 0;
    while start < bytes.len() && seen < rows {
        let end = memchr::memchr(b'\n', &bytes[start..]).map_or(bytes.len(), |at| start + at);
        if polars::io::ndjson::core::is_json_line(&bytes[start..end]) {
            seen += 1;
        }
        start = end + 1;
    }
    start.min(bytes.len())
}

/// Bytes parsed at once by a read of every row.
const RUN: usize = 64 << 20;

/// Where the run of whole lines from `start` ends: about `run` bytes on, at a line's
/// end, or at the end of `bytes`.
fn run_end(bytes: &[u8], start: usize, run: usize) -> usize {
    let at = start.saturating_add(run);
    if at >= bytes.len() {
        return bytes.len();
    }
    memchr::memchr(b'\n', &bytes[at..]).map_or(bytes.len(), |n| at + n + 1)
}

/// The file's bytes as they stand, mapped rather than read: a followed file can be
/// many times the memory. `None` when it is empty, which cannot be mapped.
fn map(file: &File) -> std::io::Result<Option<memmap2::Mmap>> {
    if file.metadata()?.len() == 0 {
        return Ok(None);
    }
    // SAFETY: the file is only ever appended to while it is followed; a truncation is
    // seen by the watcher, which reads it again from the start. Polars maps a followed
    // file the same way.
    Ok(Some(unsafe { memmap2::Mmap::map(file)? }))
}

impl LinesScan {
    /// The scan of the NDJSON file at `path`, its schema inferred from the first
    /// `infer` complete lines, or all of them.
    pub(crate) fn open(
        path: &Path,
        infer: Option<NonZeroUsize>,
        ignore_errors: bool,
        spool: Option<Arc<Spool>>,
    ) -> PolarsResult<LinesScan> {
        let ended = spool.as_ref().is_some_and(|s| s.ended().is_some());
        let file = File::open(path)?;
        let map = map(&file)?;
        let bytes = map.as_deref().unwrap_or_default();
        let bytes = &bytes[..complete(bytes, ended)];
        let bytes = bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(bytes);
        let schema = match polars::io::ndjson::infer_schema(&mut Cursor::new(bytes), infer) {
            Err(_) if ignore_errors => {
                // A line that is not a JSON object is a row of nulls: the schema comes
                // from the lines that are.
                let mut objects = Vec::new();
                let wanted = infer.map_or(usize::MAX, NonZeroUsize::get);
                let lines = bytes
                    .split(|&b| b == b'\n')
                    .filter(|line| {
                        matches!(
                            serde_json::from_slice::<serde_json::Value>(line),
                            Ok(serde_json::Value::Object(_))
                        )
                    })
                    .take(wanted);
                for line in lines {
                    objects.extend_from_slice(line);
                    objects.push(b'\n');
                }
                polars::io::ndjson::infer_schema(&mut Cursor::new(objects), infer)?
            }
            inferred => inferred?,
        };
        Ok(LinesScan {
            path: path.to_path_buf(),
            schema: Arc::new(schema),
            ignore_errors,
            spool,
            held: None,
        })
    }

    /// The scan as a frame.
    pub(crate) fn lazy(self) -> PolarsResult<LazyFrame> {
        let schema = self.schema.clone();
        LazyFrame::anonymous_scan(
            Arc::new(self),
            ScanArgsAnonymous {
                schema: Some(schema),
                name: SCAN_NAME,
                ..Default::default()
            },
        )
    }

    /// The lines scan `scan` is, if it is one.
    pub(super) fn in_plan(scan: &polars::lazy::dsl::DslPlan) -> Option<&LinesScan> {
        use polars::lazy::dsl::{DslPlan, FileScanDsl};
        let DslPlan::Scan { scan_type, .. } = scan else {
            return None;
        };
        let FileScanDsl::Anonymous { function, .. } = &**scan_type else {
            return None;
        };
        function.as_any().downcast_ref::<LinesScan>()
    }

    /// The lines scan `scan` is, when it reads the file at `path` by its name.
    pub(super) fn of<'a>(
        scan: &'a polars::lazy::dsl::DslPlan,
        path: &str,
    ) -> Option<&'a LinesScan> {
        Self::in_plan(scan)
            .filter(|s| s.held.is_none() && super::same_file(&s.path.to_string_lossy(), path))
    }

    pub(super) fn ignore_errors(&self) -> bool {
        self.ignore_errors
    }

    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// This scan, reading through `file`: the file was deleted.
    pub(super) fn held(&self, file: &File) -> Option<LinesScan> {
        Some(LinesScan {
            held: Some(Arc::new(file.try_clone().ok()?)),
            ..self.with_schema(self.schema.clone())
        })
    }

    /// This scan, reading `schema`'s columns: the fields that arrived after the open
    /// joined to them.
    pub(crate) fn with_schema(&self, schema: SchemaRef) -> LinesScan {
        LinesScan {
            path: self.path.clone(),
            schema,
            ignore_errors: self.ignore_errors,
            spool: self.spool.clone(),
            held: self.held.clone(),
        }
    }
}

impl AnonymousScan for LinesScan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self, _infer_schema_length: Option<usize>) -> PolarsResult<SchemaRef> {
        Ok(self.schema.clone())
    }

    fn allows_predicate_pushdown(&self) -> bool {
        true
    }

    fn allows_projection_pushdown(&self) -> bool {
        true
    }

    fn scan(&self, args: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        // Asked before the file is mapped: an end heard after it could count a line the
        // map cut off.
        let ended = self.spool.as_ref().is_some_and(|s| s.ended().is_some());
        let file = match &self.held {
            Some(held) => held.try_clone()?,
            None => File::open(&self.path)?,
        };
        let columns: Schema = match args.with_columns.as_deref() {
            Some(names) => names
                .iter()
                .filter_map(|name| self.schema.get_field(name))
                .collect(),
            None => (*self.schema).clone(),
        };
        let map = map(&file)?;
        let bytes = map.as_deref().unwrap_or_default();
        let mut bytes = &bytes[..complete(bytes, ended)];
        if let Some(rows) = args.n_rows {
            bytes = &bytes[..after_rows(bytes, rows)];
        }
        if columns.is_empty() {
            // A count: the rows, with no columns to parse.
            let df = DataFrame::empty_with_height(polars::io::ndjson::count_rows(bytes));
            return match args.predicate {
                Some(predicate) => df.lazy().filter(predicate).collect(),
                None => Ok(df),
            };
        }
        let columns = Arc::new(columns);
        // A run of lines at a time, each filtered before the next is parsed: a filter
        // over a large file holds the rows it keeps, not every row's columns.
        let mut out = DataFrame::empty_with_schema(&columns);
        let mut start = 0;
        while start < bytes.len() {
            let end = run_end(bytes, start, RUN);
            let mut df = parse_run(&bytes[start..end], &columns, self.ignore_errors)?;
            if let Some(predicate) = &args.predicate {
                df = df.lazy().filter(predicate.clone()).collect()?;
            }
            out.vstack_mut(&df)?;
            start = end;
        }
        out.rechunk_mut();
        Ok(out)
    }
}

/// The rows of `run`, whole lines, read as `columns` by Polars' NDJSON line parser, as
/// a read from a mark is: its whole-file reader samples line lengths and panics on some
/// short lines. With `ignore_errors`, a line that is not JSON is a row of nulls, as the
/// watcher counts it, rather than the end of the read.
pub(super) fn parse_run(
    run: &[u8],
    columns: &Schema,
    ignore_errors: bool,
) -> PolarsResult<DataFrame> {
    let run = run.strip_prefix(b"\xef\xbb\xbf").unwrap_or(run);
    match polars::io::ndjson::core::parse_ndjson(run, None, columns, ignore_errors) {
        Err(_) if ignore_errors => {}
        read => return read,
    }
    let mut out = DataFrame::empty_with_schema(columns);
    for line in run.split(|&b| b == b'\n') {
        if !polars::io::ndjson::core::is_json_line(line) {
            continue;
        }
        let row = polars::io::ndjson::core::parse_ndjson(line, Some(1), columns, true)
            .unwrap_or_else(|_| DataFrame::full_null(columns, 1));
        out.vstack_mut(&row)?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_whole_lines_are_complete_until_the_end() {
        assert_eq!(complete(b"", false), 0);
        assert_eq!(complete(b"{\"a\":", false), 0);
        assert_eq!(complete(b"{\"a\":1}\n{\"a\"", false), 8);
        assert_eq!(complete(b"{\"a\":1}\n", false), 8);
        assert_eq!(complete(b"{\"a\":1}\n{\"a\":2}", true), 15);
    }

    /// Short lines (blank, whitespace, not JSON) among the objects never fail a read:
    /// every read gives the objects, and a line that is not JSON a row of nulls.
    #[test]
    fn short_and_bad_lines_read_as_the_watcher_counts_them() {
        let dir = tempfile::tempdir().unwrap();
        let cases: [(&str, Vec<Option<i64>>); 4] = [
            ("{\"a\":1}\n\n   \n{\"a\":2}\n", vec![Some(1), Some(2)]),
            (
                "{\"a\":1}\n{\"a\":2}\ngarbage\n{\"a\":3}\n",
                vec![Some(1), Some(2), None, Some(3)],
            ),
            (
                "{\"a\":1} \n{\"a\":2}\t\n{\"a\":3}\n",
                vec![Some(1), Some(2), Some(3)],
            ),
            ("\u{feff}{\"a\":1}\n{\"a\":2}\n", vec![Some(1), Some(2)]),
        ];
        for (text, ids) in cases {
            let path = dir.path().join("short.ndjson");
            std::fs::write(&path, text).unwrap();
            let lf = LinesScan::open(&path, None, true, None)
                .unwrap()
                .lazy()
                .unwrap();
            let all = lf.clone().collect().unwrap();
            let got: Vec<Option<i64>> = all.column("a").unwrap().i64().unwrap().to_vec();
            assert_eq!(got, ids, "{text:?}");
            let head = lf.clone().slice(0, 2).collect().unwrap();
            assert_eq!(head.height(), 2, "{text:?}");
            let kept = lf.clone().filter(col("a").gt(lit(1))).collect().unwrap();
            let above = ids.iter().filter(|v| v.is_some_and(|v| v > 1)).count();
            assert_eq!(kept.height(), above, "{text:?}");
            let count = lf.clone().select([len()]).collect().unwrap();
            assert_eq!(
                count
                    .column("len")
                    .unwrap()
                    .get(0)
                    .unwrap()
                    .extract::<usize>(),
                Some(ids.len()),
                "{text:?}"
            );
        }
    }

    #[test]
    fn a_run_ends_at_a_line_s_end() {
        let bytes = b"{\"a\":1}\n{\"a\":22}\n{\"a\":3}";
        assert_eq!(run_end(bytes, 0, 3), 8);
        assert_eq!(run_end(bytes, 8, 3), 17);
        assert_eq!(run_end(bytes, 17, 3), bytes.len());
        assert_eq!(run_end(bytes, 0, 100), bytes.len());
    }

    #[test]
    fn a_read_of_some_rows_skips_blank_lines() {
        let bytes = b"{\"a\":1}\n\n{\"a\":2}\n  \n{\"a\":3}\n";
        assert_eq!(after_rows(bytes, 0), 0);
        assert_eq!(after_rows(bytes, 1), 8);
        assert_eq!(after_rows(bytes, 2), 17);
        assert_eq!(after_rows(bytes, 3), bytes.len());
        assert_eq!(after_rows(bytes, 9), bytes.len());
    }

    /// The scan reads what the file holds at each moment, through its last newline,
    /// wherever the writer has got to: never an error for an object cut off.
    #[test]
    fn a_cut_off_last_object_is_never_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("growing.ndjson");
        let mut text = String::new();
        for i in 0..200 {
            text.push_str(&format!("{{\"id\": {i}, \"msg\": \"line {i}\"}}\n"));
        }
        let bytes = text.as_bytes();
        let first = bytes.iter().position(|&b| b == b'\n').unwrap() + 1;
        std::fs::write(&path, &bytes[..first]).unwrap();
        let lf = LinesScan::open(&path, None, true, None)
            .unwrap()
            .lazy()
            .unwrap();
        for cut in (first..=bytes.len()).step_by(7).chain([bytes.len()]) {
            std::fs::write(&path, &bytes[..cut]).unwrap();
            let whole = bytes[..cut].iter().filter(|&&b| b == b'\n').count();
            let df = lf.clone().collect().unwrap();
            assert_eq!(df.height(), whole, "cut at {cut}");
            let count = lf.clone().select([len()]).collect().unwrap();
            assert_eq!(
                count
                    .column("len")
                    .unwrap()
                    .get(0)
                    .unwrap()
                    .extract::<usize>(),
                Some(whole),
                "count at {cut}"
            );
            let filtered = lf
                .clone()
                .filter(col("id").gt_eq(lit(0)))
                .select([col("msg")])
                .collect()
                .unwrap();
            assert_eq!(filtered.height(), whole, "filter at {cut}");
            let head = lf.clone().slice(0, 3).collect().unwrap();
            assert_eq!(head.height(), whole.min(3), "head at {cut}");
        }
    }
}
