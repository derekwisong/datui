//! Files read into a table of their own: the temporary Arrow IPC files a conversion
//! writes a batch at a time, and the frame over them. GPS logs and SQLite tables are
//! read this way; the dataset scans the files lazily and holds them, so quitting or
//! replacing the open removes them.

use std::fs::File;
use std::path::PathBuf;

use color_eyre::Result;
use color_eyre::eyre::eyre;
use polars::io::ipc::BatchedWriter;
use polars::prelude::*;

use crate::OpenOptions;
use crate::download::TempDownload;
use crate::notes::Note;
use crate::unfinished::Writer;

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
pub(crate) struct Segments<'a> {
    dir: Option<PathBuf>,
    writer: &'a Writer,
    open: Option<(BatchedWriter<File>, TempDownload, Schema)>,
    done: Vec<(TempDownload, Schema)>,
}

impl<'a> Segments<'a> {
    pub(crate) fn new(options: &OpenOptions, writer: &'a Writer) -> Self {
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
    pub(crate) fn write(&mut self, df: &DataFrame) -> Result<()> {
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
    pub(crate) fn finish(mut self) -> Result<(LazyFrame, Vec<TempDownload>)> {
        self.close()?;
        // Columns only ever grow and their types only ever widen, so the last segment
        // has them all, in order, as wide as they get.
        let full = self
            .done
            .last()
            .map(|(_, schema)| schema.clone())
            .ok_or_else(|| eyre!("Nothing was read."))?;
        let mut frames = Vec::with_capacity(self.done.len());
        for (file, schema) in &self.done {
            // The temp directory is the user's to name, `[` and all.
            let args = UnifiedScanArgs {
                glob: crate::source::expands_as_glob(file.path()),
                ..Default::default()
            };
            let lf = LazyFrame::scan_ipc(
                PlRefPath::try_from_path(file.path())?,
                Default::default(),
                args,
            )?;
            let columns: Vec<Expr> = full
                .iter()
                .map(|(name, dtype)| match schema.get(name) {
                    Some(had) if had == dtype => col(name.clone()),
                    Some(_) => col(name.clone()).cast(dtype.clone()),
                    None => lit(NULL).cast(dtype.clone()).alias(name.clone()),
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
