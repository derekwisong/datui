//! A spec and a file opened as a table.

use super::*;

impl Spec {
    /// The columns the record fields become, a record `stride` bytes apart (or one
    /// file per field for the columns layout), and the bytes the fields take.
    pub(crate) fn record_columns(
        &self,
        header: &HeaderValues,
        header_size: usize,
        stride: Option<usize>,
    ) -> Result<(Vec<ColumnLayout>, u64), String> {
        let mut columns = Vec::new();
        let mut at = 0u64;
        for (i, field) in self.records.fields.iter().enumerate() {
            let (width, count) = sized(field, header)?;
            if width == 0 {
                return Err(format!(
                    "field `{}` takes no bytes",
                    field.name.as_deref().unwrap_or("pad")
                ));
            }
            let start = match self.layout {
                Layout::Rows => header_size + at as usize,
                Layout::Columns => header_size,
            };
            if let Some(name) = &field.name {
                let source = match self.layout {
                    Layout::Rows => 0,
                    Layout::Columns => i,
                };
                if field.flatten {
                    for j in 0..count as usize {
                        let place = Place {
                            start: start + j * width as usize,
                            stride,
                            width: width as usize,
                            count: 1,
                        };
                        let mut layout =
                            layout_of(self, field, &format!("{name}_{j}"), place, header)?;
                        layout.source = source;
                        columns.push(layout);
                    }
                } else {
                    let place = Place {
                        start,
                        stride,
                        width: width as usize,
                        count: count as usize,
                    };
                    let mut layout = layout_of(self, field, name, place, header)?;
                    layout.source = source;
                    columns.push(layout);
                }
            }
            at += width * count;
        }
        Ok((columns, at))
    }

    /// Check the magic of `bytes`, the front of one file.
    fn check_magic(&self, bytes: &[u8], named: &str) -> Result<(), String> {
        if self.magic.is_empty() || self.magic_matches(bytes) {
            return Ok(());
        }
        let start = (self.magic_offset as usize).min(bytes.len());
        let found = &bytes[start..(start + self.magic.len()).min(bytes.len())];
        Err(format!(
            "{named} is not {}: expected magic {} at byte {}, found {}",
            self.name,
            crate::fixed_records::hex(&self.magic),
            self.magic_offset,
            if found.is_empty() {
                "the end of the file".to_string()
            } else {
                crate::fixed_records::hex(found)
            }
        ))
    }

    /// Read `bytes`, one file of the rows layout, named `named` in what it says.
    pub fn open_rows(&self, bytes: Arc<Bytes>, named: &str) -> Result<Opened, String> {
        if self.is_delimited() {
            return Err(format!(
                "{} is a delimited spec; it reads text through the CSV reader",
                self.name
            ));
        }
        self.open_rows_in(bytes, named, None)
    }

    /// The spec with its byte order settled by `head`, for `endian = "auto"`: as the
    /// spec says when the magic reads as written, big-endian when it reads reversed.
    pub fn for_file(&self, head: &[u8]) -> std::borrow::Cow<'_, Spec> {
        if !self.endian_auto {
            return std::borrow::Cow::Borrowed(self);
        }
        let reversed: Vec<u8> = self.magic.iter().rev().copied().collect();
        let start = self.magic_offset as usize;
        if reversed != self.magic && head.get(start..start + reversed.len()) == Some(&reversed[..])
        {
            let mut spec = self.clone();
            spec.endian = Endian::Big;
            spec.endian_auto = false;
            spec.magic = reversed;
            return std::borrow::Cow::Owned(spec);
        }
        std::borrow::Cow::Borrowed(self)
    }

    /// Whether the spec reads a directory: column files, or a tree of its files.
    pub fn reads_directory(&self) -> bool {
        self.files.is_some()
            || (self.layout == Layout::Columns
                && !self.records.fields.iter().any(|f| f.at.is_some()))
    }

    /// The spec reading the variant `name` alone: only its records, and only its columns.
    pub fn with_variant(&self, name: &str) -> Result<Spec, String> {
        if !self.records.variants.iter().any(|v| v.name == name) {
            let names: Vec<&str> = self
                .records
                .variants
                .iter()
                .map(|v| v.name.as_str())
                .collect();
            return Err(if names.is_empty() {
                format!("{} has no variants", self.name)
            } else {
                format!(
                    "{} has no variant {name}; it has {}",
                    self.name,
                    names.join(", ")
                )
            });
        }
        let mut spec = self.clone();
        spec.variant = Some(name.to_string());
        Ok(spec)
    }

    /// Read `bytes`, one file, named `named` in what it says; a symbol list a field
    /// names is looked for beside `path`, the file the bytes are, which also keeps the
    /// walk of its records for the next open of it.
    pub fn open_rows_in(
        &self,
        bytes: Arc<Bytes>,
        named: &str,
        path: Option<&Path>,
    ) -> Result<Opened, String> {
        let dir = path.and_then(Path::parent);
        if self.reads_directory() {
            return Err(format!(
                "{} reads a directory ({}); open the directory",
                self.name,
                if self.files.is_some() {
                    "a tree of its files"
                } else {
                    "column files, layout = \"columns\""
                }
            ));
        }
        let spec = self.for_file(bytes.as_slice());
        let spec = spec.as_ref();
        if spec.layout == Layout::Columns {
            return spec.open_columns_file(bytes, named, dir);
        }
        let data = bytes.as_slice();
        let mut header = if spec.capture.is_some() {
            if !crate::framed_records::capture::is_capture(data) {
                return Err(format!("{named} is not a pcap or pcapng capture"));
            }
            HeaderValues::default()
        } else {
            spec.check_magic(data, named)?;
            read_header(spec, data)?
        };
        let mut notes = Vec::new();
        let full = data.len() as u64;
        if header.size > full {
            return Err(format!(
                "{named} is {full} bytes, shorter than its {}-byte header",
                header.size
            ));
        }
        let (len, footer_note) = read_footer(spec, data, &mut header)?;
        notes.extend(footer_note);
        let fields: Vec<Field> = all_fields(&spec.records).cloned().collect();
        read_lookups(&fields, dir, &mut header)?;
        if crate::framed_records::needed(spec) {
            let (records, more) = crate::framed_records::FramedRecords::open(
                spec,
                bytes.clone(),
                &header,
                header.size as usize..len as usize,
                named,
                path,
            )?;
            notes.extend(more);
            return Ok(Opened {
                records: Arc::new(records),
                notes,
                header,
            });
        }
        let data = &data[..len as usize];
        // The fields' own width first, to check the record size against it.
        let (_, fields_width) = spec.record_columns(&header, 0, Some(1))?;
        let record = match &spec.records.size {
            None => fields_width,
            Some(amount) => {
                let size = header.resolve(amount, "record size")?;
                if size < fields_width {
                    return Err(format!(
                        "the record's fields take {fields_width} bytes, more than its size of {size}"
                    ));
                }
                size
            }
        };
        if record == 0 || record > MAX_SIZE {
            return Err(format!(
                "a record of {record} bytes is outside 1 to {MAX_SIZE}"
            ));
        }
        let room = len - header.size;
        let whole = room / record;
        let rows = match &spec.records.count {
            None => {
                let trailing = room % record;
                if trailing > 0 {
                    notes.push(trailing_note(named, &data[(len - trailing) as usize..]));
                }
                whole
            }
            Some(amount) => {
                let count = header.resolve(amount, "count")?;
                if count > whole {
                    notes.push(format!(
                        "header says {count} records {} {whole} whole ones shown",
                        crate::glyphs::get().middot
                    ));
                    whole
                } else {
                    let end = header.size + count * record;
                    if end < len {
                        let after = len - end;
                        notes.push(format!(
                            "{named} has {after} {} after its {count} records, left out",
                            if after == 1 { "byte" } else { "bytes" }
                        ));
                    }
                    count
                }
            }
        };
        let (columns, _) =
            spec.record_columns(&header, header.size as usize, Some(record as usize))?;
        let records =
            FixedRecords::new(vec![bytes], columns, rows as usize).map_err(|e| e.to_string())?;
        past_limit(&mut notes, rows, &records);
        Ok(Opened {
            records: Arc::new(records),
            notes,
            header,
        })
    }

    /// Read `dir`, a directory of one file per record field.
    pub fn open_columns(&self, dir: &Path) -> Result<Opened, String> {
        if self.is_delimited() {
            return Err(format!(
                "{} is a delimited spec; it reads text through the CSV reader",
                self.name
            ));
        }
        if self.layout == Layout::Rows {
            return Err(format!(
                "{} reads one file, and {} is a directory",
                self.name,
                dir.display()
            ));
        }
        let mut sources = Vec::new();
        let mut header: Option<HeaderValues> = None;
        let mut notes = Vec::new();
        let mut counts: Vec<(String, u64)> = Vec::new();
        // Each file's own header size: a size read from the header may differ by file.
        let mut starts = Vec::new();
        for field in &self.records.fields {
            let file_name = field
                .file
                .clone()
                .or_else(|| field.name.clone())
                .expect("a column field is named");
            let path = dir.join(&file_name);
            let bytes = Bytes::map(&path).map_err(|e| format!("{}: {e}", path.display()))?;
            self.check_magic(bytes.as_slice(), &file_name)?;
            let read = read_header(self, bytes.as_slice())?;
            let (width, count) = sized(field, header.as_ref().unwrap_or(&read))?;
            let cell = width * count;
            let len = bytes.len() as u64;
            if read.size > len {
                return Err(format!(
                    "{file_name} is {len} bytes, shorter than its {}-byte header",
                    read.size
                ));
            }
            let room = len - read.size;
            if cell > 0 && !room.is_multiple_of(cell) {
                let trailing = room % cell;
                notes.push(trailing_note(
                    &file_name,
                    &bytes.as_slice()[(len - trailing) as usize..],
                ));
            }
            counts.push((file_name, room.checked_div(cell).unwrap_or(0)));
            starts.push(read.size as usize);
            if header.is_none() {
                header = Some(read);
            }
            sources.push(Arc::new(bytes));
        }
        let mut header = header.unwrap_or_default();
        read_lookups(&self.records.fields, Some(dir), &mut header)?;
        let fewest = counts.iter().map(|(_, n)| *n).min().unwrap_or(0);
        if counts.iter().any(|(_, n)| *n != fewest) {
            let said: Vec<String> = counts
                .iter()
                .map(|(name, n)| format!("{name} {n}"))
                .collect();
            notes.push(format!(
                "column files differ in length ({}) {} first {fewest} rows shown",
                said.join(", "),
                crate::glyphs::get().middot
            ));
        }
        let mut rows = fewest;
        if let Some(amount) = &self.records.count {
            let count = header.resolve(amount, "count")?;
            if count > fewest {
                notes.push(format!(
                    "header says {count} records {} {fewest} shown",
                    crate::glyphs::get().middot
                ));
            }
            rows = rows.min(count);
        }
        let (mut columns, _) = self.record_columns(&header, 0, None)?;
        for column in &mut columns {
            column.start = starts[column.source];
        }
        let records =
            FixedRecords::new(sources, columns, rows as usize).map_err(|e| e.to_string())?;
        past_limit(&mut notes, rows, &records);
        Ok(Opened {
            records: Arc::new(records),
            notes,
            header,
        })
    }

    /// Read `bytes`, one file holding each column's values in a run at its `offset`.
    fn open_columns_file(
        &self,
        bytes: Arc<Bytes>,
        named: &str,
        dir: Option<&Path>,
    ) -> Result<Opened, String> {
        let data = bytes.as_slice();
        self.check_magic(data, named)?;
        let mut header = read_header(self, data)?;
        let mut notes = Vec::new();
        let (len, footer_note) = read_footer(self, data, &mut header)?;
        notes.extend(footer_note);
        read_lookups(&self.records.fields, dir, &mut header)?;
        let (mut columns, _) = self.record_columns(&header, 0, None)?;
        let mut rows = u64::MAX;
        let mut starts = Vec::new();
        for field in &self.records.fields {
            let (width, count) = sized(field, &header)?;
            let at = field.at.as_ref().expect("checked at parse");
            let start = header.resolve_any(at, "offset")?;
            if start < header.size || start > len {
                return Err(format!(
                    "column `{}` starts at byte {start}, outside the data from {} to {len}",
                    field.name.as_deref().unwrap_or("pad"),
                    header.size
                ));
            }
            rows = rows.min((len - start) / (width * count).max(1));
            starts.push(start as usize);
        }
        if let Some(amount) = &self.records.count {
            let count = header.resolve_any(amount, "count")?;
            if count > rows {
                notes.push(format!(
                    "header says {count} records {} room for {rows} shown",
                    crate::glyphs::get().middot
                ));
            }
            rows = rows.min(count);
        } else {
            notes.push(format!(
                "no count is given, so the rows are the {rows} the shortest column has room for"
            ));
        }
        for column in &mut columns {
            column.start = starts[column.source];
        }
        let sources = vec![bytes; self.records.fields.len()];
        let records = FixedRecords::new(sources, columns, rows.min(usize::MAX as u64) as usize)
            .map_err(|e| e.to_string())?;
        Ok(Opened {
            records: Arc::new(records),
            notes,
            header,
        })
    }

    /// Open `path`: a file, or a directory of column files or of the spec's files.
    pub fn open(&self, path: &Path, named: &str) -> Result<Opened, String> {
        if self.files.is_some() {
            return crate::formats::files::open(self, path);
        }
        if self.reads_directory() {
            return self.open_columns(path);
        }
        if path.is_dir() {
            return Err(format!(
                "{} reads one file, and {} is a directory",
                self.name,
                path.display()
            ));
        }
        let bytes = Bytes::map(path).map_err(|e| format!("{}: {e}", path.display()))?;
        self.open_rows_in(Arc::new(bytes), named, Some(path))
    }
}
