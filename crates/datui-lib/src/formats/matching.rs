//! A spec read from text or a file, which files it matches, and its docs.

use super::*;

impl Spec {
    /// Read a spec from its text. `path` names it in errors.
    pub fn parse(text: &str, path: Option<&Path>) -> Result<Self, SpecError> {
        let reader = Reader { text, path };
        let document = DeTable::parse(text).map_err(|e| {
            let (line, column) = e
                .span()
                .map_or((0, 0), |span| line_column(text, span.start));
            SpecError {
                path: path.map(Path::to_path_buf),
                line,
                column,
                message: e.message().to_string(),
            }
        })?;
        let kind = document
            .get_ref()
            .iter()
            .find(|(key, _)| {
                let key: &str = key.get_ref();
                key == "kind"
            })
            .map(|(_, v)| v);
        if let Some(v) = kind {
            match reader.string(v, "kind")?.as_str() {
                "binary" => {}
                "delimited" => return Self::parse_delimited(&reader, document.get_ref(), path),
                _ => return Err(reader.error(&v.span(), "kind: expected binary or delimited")),
            }
        }
        let top = reader.entries(
            document.get_ref(),
            "the spec",
            &[
                "name",
                "description",
                "documentation",
                "kind",
                "match",
                "endian",
                "layout",
                "header",
                "records",
                "footer",
                "variants",
                "blocks",
                "capture",
                "files",
                "sections",
            ],
        )?;
        let whole = 0..0;
        let name = reader.spec_name(&top)?;
        let description = top
            .get("description")
            .map(|v| reader.prose(v, "description"))
            .transpose()?;
        let documentation = top
            .get("documentation")
            .map(|v| reader.documentation(v))
            .transpose()?;
        let (endian, endian_auto) = match top.get("endian") {
            None => (Endian::Little, false),
            Some(v) => match reader.string(v, "endian")?.as_str() {
                "le" => (Endian::Little, false),
                "be" => (Endian::Big, false),
                "auto" => (Endian::Little, true),
                _ => return Err(reader.error(&v.span(), "endian: expected le, be or auto")),
            },
        };
        let layout = match top.get("layout") {
            None => Layout::Rows,
            Some(v) => match reader.string(v, "layout")?.as_str() {
                "rows" => Layout::Rows,
                "columns" => Layout::Columns,
                _ => return Err(reader.error(&v.span(), "layout: expected rows or columns")),
            },
        };

        let mut header = Header::default();
        if let Some(v) = top.get("header") {
            let table = reader.table(v, "[header]")?;
            let keys = reader.entries(table, "[header]", &["fields", "size", "size_adjust"])?;
            if let Some(f) = keys.get("fields") {
                header.fields = reader.fields(f, Part::Header, layout, &Scopes::default(), &[])?;
            }
            if let Some(s) = keys.get("size") {
                let scopes = Scopes {
                    header: &header.fields,
                    footer: &[],
                };
                header.size = Some(reader.amount(
                    s,
                    keys.get("size_adjust").copied(),
                    "size",
                    Part::Header,
                    &header.fields,
                    &scopes,
                )?);
            }
        }
        let footer = top
            .get("footer")
            .map(|v| reader.footer(v, &header.fields))
            .transpose()?;
        let footer_fields: &[Field] = footer.as_ref().map_or(&[], |f| &f.fields);
        let scopes = Scopes {
            header: &header.fields,
            footer: footer_fields,
        };

        let MatchRules {
            globs,
            glob_set,
            magic,
            magic_offset,
            expect,
        } = match top.get("match") {
            Some(v) => reader.match_rules(v, Some(&header.fields))?,
            None => MatchRules::default(),
        };

        if endian_auto && magic.len() < 2 {
            return Err(reader.error(
                &top["endian"].span(),
                "endian = \"auto\" reads the byte order from the magic, so it needs a magic of at least two bytes",
            ));
        }

        let sections = top
            .get("sections")
            .map(|v| reader.sections(v, &scopes))
            .transpose()?
            .unwrap_or_default();

        let Some(records_value) = top.get("records") else {
            return Err(reader.error(&whole, "missing [records], with the fields of one record"));
        };
        let records =
            reader.records(records_value, top.get("variants").copied(), layout, &scopes)?;
        for field in all_fields(&records) {
            if let Some(section) = &field.string_at
                && !sections.iter().any(|s| &s.name == section)
            {
                return Err(reader.error(
                    &records_value.span(),
                    format!("string_at: no section named `{section}` under [sections]"),
                ));
            }
        }
        let blocks = top
            .get("blocks")
            .map(|v| reader.blocks(v, &scopes))
            .transpose()?;
        let capture = top.get("capture").map(|v| reader.capture(v)).transpose()?;
        let files = top.get("files").map(|v| reader.files(v)).transpose()?;

        if let Some(v) = top.get("capture")
            && (blocks.is_some() || top.contains_key("header") || footer.is_some())
        {
            return Err(reader.error(
                &v.span(),
                "[capture]: the capture's own headers frame the payloads; leave out [header], [footer] and [blocks]",
            ));
        }
        let framed = blocks.is_some() || capture.is_some();
        if layout == Layout::Columns
            && let Some(v) = top.get("blocks").or(top.get("files"))
        {
            return Err(reader.error(
                &v.span(),
                "layout = \"columns\" reads values, not blocks or a tree of files",
            ));
        }
        if let Some(ring) = &records.ring {
            let ring_ok = records.framing == Framing::Fixed
                && !framed
                && records.variants.is_empty()
                && matches!(
                    ring,
                    Amount::Header { .. } | Amount::Footer { .. } | Amount::Given(_)
                );
            if !ring_ok {
                return Err(reader.error(
                    &records_value.span(),
                    "ring: is for fixed records, the oldest's index read from the header",
                ));
            }
        }
        if let Some(files) = &files {
            let columns: std::collections::HashSet<String> =
                all_fields(&records).flat_map(output_names).collect();
            if let Some(part) = files.parts.iter().find(|p| columns.contains(&p.name)) {
                return Err(reader.error(
                    &top["files"].span(),
                    format!("path: `{}` is also a field's name", part.name),
                ));
            }
        }

        if layout == Layout::Columns {
            reader.check_columns(records_value, &records)?;
            if let Some(v) = top
                .get("footer")
                .or(top.get("variants"))
                .or(top.get("capture"))
            {
                return Err(reader.error(
                    &v.span(),
                    "layout = \"columns\" holds fixed values: no footer, variants or capture",
                ));
            }
        }
        // Sizes written down are checked now; one that comes from the file is checked
        // when the file is read.
        if let (Some(Amount::Given(size)), Some(sum)) =
            (&records.size, given_width(&records.fields))
            && *size < sum
            && records.variants.is_empty()
        {
            return Err(reader.error(
                &records_value.span(),
                format!("size: the fields take {sum} bytes, more than {size}"),
            ));
        }
        if let (Some(Amount::Given(size)), Some(sum)) = (&header.size, given_width(&header.fields))
            && *size < sum
        {
            return Err(reader.error(
                &records_value.span(),
                format!("[header] size: the fields take {sum} bytes, more than {size}"),
            ));
        }
        if let Some(sum) = given_width(&records.fields) {
            if sum == 0 && records.variants.is_empty() && records.sync.is_empty() {
                return Err(reader.error(&records_value.span(), "fields: a record takes no bytes"));
            }
            if sum > MAX_SIZE {
                return Err(reader.error(
                    &records_value.span(),
                    format!("fields: a record of {sum} bytes is more than {MAX_SIZE}"),
                ));
            }
        }
        Ok(Self {
            name,
            description,
            documentation,
            notes: Vec::new(),
            path: path.map(Path::to_path_buf),
            globs,
            glob_set,
            magic,
            magic_offset,
            expect,
            endian,
            endian_auto,
            layout,
            header,
            records,
            delimited: None,
            footer,
            blocks,
            capture,
            files,
            sections,
            variant: None,
        })
    }

    /// A `kind = "delimited"` spec: a CSV-like file's reading options, with no
    /// header or record fields.
    fn parse_delimited(
        reader: &Reader<'_>,
        document: &DeTable<'_>,
        path: Option<&Path>,
    ) -> Result<Self, SpecError> {
        let keys = delimited_spec_keys();
        let top = reader.entries(document, "a delimited spec", &keys)?;
        let name = reader.spec_name(&top)?;
        let description = top
            .get("description")
            .map(|v| reader.prose(v, "description"))
            .transpose()?;
        let documentation = top
            .get("documentation")
            .map(|v| reader.documentation(v))
            .transpose()?;
        let MatchRules {
            globs,
            glob_set,
            magic,
            magic_offset,
            expect,
        } = match top.get("match") {
            Some(v) => reader.match_rules(v, None)?,
            None => MatchRules::default(),
        };
        let (delimited, notes) = reader.delimited(&top)?;
        Ok(Self {
            name,
            description,
            documentation,
            notes,
            path: path.map(Path::to_path_buf),
            globs,
            glob_set,
            magic,
            magic_offset,
            expect,
            endian: Endian::Little,
            endian_auto: false,
            layout: Layout::Rows,
            header: Header::default(),
            records: Records::default(),
            delimited: Some(Arc::new(delimited)),
            footer: None,
            blocks: None,
            capture: None,
            files: None,
            sections: Vec::new(),
            variant: None,
        })
    }

    /// Whether the spec is `kind = "delimited"`.
    pub fn is_delimited(&self) -> bool {
        self.delimited.is_some()
    }

    /// Read the spec in `path`, which may be no more than [`MAX_SPEC_BYTES`].
    pub fn load(path: &Path) -> Result<Self, SpecError> {
        use std::io::Read;
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .and_then(|f| f.take(MAX_SPEC_BYTES + 1).read_to_end(&mut bytes))
            .map_err(|e| SpecError {
                path: Some(path.to_path_buf()),
                line: 0,
                column: 0,
                message: format!(
                    "could not read it. {}",
                    crate::error_display::user_message_from_io(&e, None)
                ),
            })?;
        Self::from_bytes(&bytes, path)
    }

    /// The spec in `bytes`, read from `from` (a file or a URL), refused past
    /// [`MAX_SPEC_BYTES`].
    pub fn from_bytes(bytes: &[u8], from: &Path) -> Result<Self, SpecError> {
        let refused = |message: String| SpecError {
            path: Some(from.to_path_buf()),
            line: 0,
            column: 0,
            message,
        };
        if bytes.len() as u64 > MAX_SPEC_BYTES {
            return Err(refused(format!("a format spec is at most {MAX_SPEC_SAID}")));
        }
        let text = std::str::from_utf8(bytes)
            .map_err(|_| refused("not UTF-8 text, which a format spec is".to_string()))?;
        Self::parse(text, Some(from))
    }

    /// Whether `path`'s name matches one of the spec's globs. A glob with a `/` is
    /// matched against the whole path, one without against the name.
    pub fn glob_matches(&self, path: &Path) -> bool {
        let Some(set) = &self.glob_set else {
            return false;
        };
        let name = path.file_name().map(Path::new);
        name.is_some_and(|n| set.is_match(n)) || set.is_match(path)
    }

    /// Whether `head`, the first bytes of a file, carries the spec's magic. A delimited
    /// spec's magic is the start of the first line, after any byte-order mark.
    pub fn magic_matches(&self, head: &[u8]) -> bool {
        let head = if self.is_delimited() {
            head.strip_prefix(UTF8_BOM).unwrap_or(head)
        } else {
            head
        };
        let start = self.magic_offset as usize;
        let found = head.get(start..start + self.magic.len());
        !self.magic.is_empty()
            && (found == Some(&self.magic)
                || (self.endian_auto
                    && found.is_some_and(|f| f.iter().eq(self.magic.iter().rev()))))
    }

    /// Whether `head` holds the header values `match.where` asks for.
    pub fn header_matches(&self, head: &[u8]) -> bool {
        if self.expect.is_empty() {
            return true;
        }
        let Ok(header) = read_header(self, head) else {
            return false;
        };
        self.expect.iter().all(|(field, wanted)| match wanted {
            Expected::Int(v) => header.int(field) == Some(*v),
            Expected::Text(v) => header.text(field).as_deref() == Some(v.as_str()),
        })
    }

    /// Bytes from the front of a file that settle the spec's magic and `where`.
    pub fn match_reach(&self) -> u64 {
        let magic = if self.magic.is_empty() {
            0
        } else {
            let bom = if self.is_delimited() {
                UTF8_BOM.len() as u64
            } else {
                0
            };
            bom + self.magic_offset + self.magic.len() as u64
        };
        let header = if self.expect.is_empty() {
            0
        } else {
            given_width(&self.header.fields).unwrap_or(MAX_MATCH_READ)
        };
        magic.max(header).min(MAX_MATCH_READ)
    }

    /// What the spec says files of it look like, one chip per condition: its magic,
    /// its header values, then its globs. Empty when only `--format` picks it.
    pub fn match_chips(&self) -> Vec<MatchChip> {
        let mut chips = Vec::new();
        if !self.magic.is_empty() {
            let ellipsis = crate::glyphs::get().ellipsis;
            let (value, kind) = if self.magic.iter().all(|b| b.is_ascii_graphic()) {
                let text = String::from_utf8_lossy(&self.magic);
                let value = if text.chars().count() > CHIP_MAGIC_CHARS {
                    let head: String = text.chars().take(CHIP_MAGIC_CHARS).collect();
                    format!("{head}{ellipsis}")
                } else {
                    text.into_owned()
                };
                (value, ChipKind::Magic)
            } else if self.magic.len() > CHIP_MAGIC_BYTES {
                let head = crate::formats::fixed_records::hex(&self.magic[..CHIP_MAGIC_BYTES]);
                (format!("{head} {ellipsis}"), ChipKind::Hex)
            } else {
                (
                    crate::formats::fixed_records::hex(&self.magic),
                    ChipKind::Hex,
                )
            };
            chips.push(MatchChip {
                name: "magic".to_string(),
                value,
                kind,
                offset: (self.magic_offset > 0).then_some(self.magic_offset),
            });
        }
        for (field, wanted) in &self.expect {
            let (value, kind) = match wanted {
                Expected::Int(v) => (v.to_string(), ChipKind::Int),
                Expected::Text(v) => (v.clone(), ChipKind::Text),
            };
            chips.push(MatchChip {
                name: field.clone(),
                value,
                kind,
                offset: None,
            });
        }
        if !self.globs.is_empty() {
            chips.push(MatchChip {
                name: "glob".to_string(),
                value: self.globs.join(" "),
                kind: ChipKind::Glob,
                offset: None,
            });
        }
        chips
    }

    /// The chips that named `path` on the home screen: the glob alone when it names the
    /// file, since a listing names a file by its glob without reading its header; else
    /// the magic and the header values it was checked against. All of them when
    /// neither does.
    pub fn match_chips_for(&self, path: &Path) -> Vec<MatchChip> {
        if self.glob_matches(path) {
            let mut chips = self.match_chips();
            chips.retain(|c| c.kind == ChipKind::Glob);
            return chips;
        }
        let by = (!self.magic.is_empty()).then_some(ChipKind::Magic);
        self.match_chips_by(by)
    }

    /// The chips of the rule that chose the spec: `Chosen::Glob` or `Chosen::Magic`
    /// leave the other out; any other choice keeps every chip.
    pub fn match_chips_chosen(&self, by: Chosen) -> Vec<MatchChip> {
        match by {
            Chosen::Glob => self.match_chips_by(Some(ChipKind::Glob)),
            Chosen::Magic => self.match_chips_by(Some(ChipKind::Magic)),
            Chosen::SpecFile | Chosen::Named => self.match_chips(),
        }
    }

    fn match_chips_by(&self, by: Option<ChipKind>) -> Vec<MatchChip> {
        let mut chips = self.match_chips();
        match by {
            Some(ChipKind::Glob) => {
                chips.retain(|c| !matches!(c.kind, ChipKind::Magic | ChipKind::Hex));
            }
            Some(_) => chips.retain(|c| c.kind != ChipKind::Glob),
            None => {}
        }
        chips
    }

    /// Whether the spec reads a file's records as several variants, each listed as a
    /// table inside the file.
    pub fn lists_variants(&self) -> bool {
        !self.is_delimited() && self.records.variants.len() > 1 && self.variant.is_none()
    }

    /// The columns a file of the spec opens with, when the spec alone says them: fixed
    /// records of one file whose fields take nothing from the file (no sizes, symbols or
    /// dates from its header). `None` when the open has to read the file to know.
    pub fn static_columns(&self) -> Option<Vec<(String, polars::prelude::DataType)>> {
        if self.is_delimited()
            || self.layout != Layout::Rows
            || crate::formats::framed_records::needed(self)
        {
            return None;
        }
        let (columns, _) = self
            .record_columns(&HeaderValues::default(), 0, Some(1))
            .ok()?;
        Some(
            columns
                .iter()
                .map(|c| (c.name.to_string(), c.dtype()))
                .collect(),
        )
    }
}

/// What a spec says of its files, for the Documentation view: its own words and the
/// notes its fields carry. None of it changes how a file is read.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SpecDocs {
    /// The spec's name.
    pub spec: String,
    /// The file the spec was read from; none for one built in or parsed from text.
    pub file: Option<PathBuf>,
    pub description: String,
    /// An `https://` link to the format's own documentation.
    pub documentation: String,
    /// The variants, each a record type the file holds.
    pub record_types: Vec<RecordType>,
    /// What each column means, by its name, in the spec's order: a field's description,
    /// its unit, and its enum as the value legend.
    pub columns: Vec<(String, ColumnNote)>,
    /// The `[header]` fields that say what they hold, in the spec's order.
    pub header: Vec<(String, ColumnNote)>,
    /// The `[footer]` fields that say what they hold, in the spec's order.
    pub footer: Vec<(String, ColumnNote)>,
}

/// A field's description and unit, when it gives either.
fn field_note(field: &Field) -> Option<(String, ColumnNote)> {
    let name = field.name.clone()?;
    let note = ColumnNote {
        description: field.description.clone().unwrap_or_default(),
        unit: field.unit.clone().unwrap_or_default(),
        values: Vec::new(),
        ty: String::new(),
    };
    (note != ColumnNote::default()).then_some((name, note))
}

/// The condition on the type field that picks a variant: `msg_type = 1`, or
/// `kind in ("E", "C")`, text quoted.
fn picked_by(type_field: Option<&str>, when: &[Expected]) -> String {
    let field = type_field.unwrap_or("type");
    let values: Vec<String> = when
        .iter()
        .map(|value| match value {
            Expected::Int(v) => v.to_string(),
            Expected::Text(v) => format!("\"{v}\""),
        })
        .collect();
    match values.as_slice() {
        [one] => format!("{field} = {one}"),
        _ => format!("{field} in ({})", values.join(", ")),
    }
}

/// One variant of a spec, as its documentation shows it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RecordType {
    pub name: String,
    /// What picks it, as a condition on the type field: `msg_type = 1`, or
    /// `kind in ("E", "C")` for several values.
    pub picked_by: String,
    pub description: String,
    /// Its columns: the common ones and its own.
    pub columns: usize,
}

impl Spec {
    /// What the spec documents, or `None` when it says nothing beyond how to read: no
    /// description or link, no record type described, no column noted.
    pub fn docs(&self) -> Option<SpecDocs> {
        let mut columns: Vec<(String, ColumnNote)> = Vec::new();
        // A name two variants share is one column: the first note of it stands.
        let mut add = |name: &str, note: ColumnNote| {
            if note != ColumnNote::default() && !columns.iter().any(|(n, _)| n == name) {
                columns.push((name.to_string(), note));
            }
        };
        let legend = |labels: &BTreeMap<i64, String>| {
            labels
                .iter()
                .map(|(code, label)| (code.to_string(), label.clone()))
                .collect::<Vec<_>>()
        };
        for field in all_fields(&self.records) {
            let note = ColumnNote {
                description: field.description.clone().unwrap_or_default(),
                unit: field.unit.clone().unwrap_or_default(),
                values: match &field.meaning {
                    Meaning::Enum(labels) => legend(labels),
                    _ => Vec::new(),
                },
                ty: String::new(),
            };
            // A flattened field is filed under each column it makes.
            for name in own_names(field) {
                add(&name, note.clone());
            }
            for bit in &field.bits {
                if let Some(labels) = &bit.labels {
                    add(
                        &bit.name,
                        ColumnNote {
                            values: legend(labels),
                            ..ColumnNote::default()
                        },
                    );
                }
            }
        }
        for (name, note) in &self.notes {
            add(name, note.clone());
        }
        let tables = crate::formats::members::variant_tables(self);
        let record_types: Vec<RecordType> = self
            .records
            .variants
            .iter()
            .zip(&tables)
            .map(|(variant, table)| RecordType {
                name: variant.name.clone(),
                picked_by: picked_by(self.records.type_field.as_deref(), &variant.when),
                description: variant.description.clone().unwrap_or_default(),
                columns: table.columns.len(),
            })
            .collect();
        let header: Vec<(String, ColumnNote)> =
            self.header.fields.iter().filter_map(field_note).collect();
        let footer: Vec<(String, ColumnNote)> = self
            .footer
            .iter()
            .flat_map(|f| &f.fields)
            .filter_map(field_note)
            .collect();
        let documented = self.description.is_some()
            || self.documentation.is_some()
            || !columns.is_empty()
            || !header.is_empty()
            || !footer.is_empty()
            || record_types.iter().any(|r| !r.description.is_empty());
        documented.then(|| SpecDocs {
            spec: self.name.clone(),
            file: self.path.clone(),
            description: self.description.clone().unwrap_or_default(),
            documentation: self.documentation.clone().unwrap_or_default(),
            record_types,
            columns,
            header,
            footer,
        })
    }
}

/// Bytes one field takes in each record, when nothing about it comes from the file.
fn field_width(field: &Field) -> Option<u64> {
    let width = match (&field.size, field.ty.width()) {
        (_, Some(w)) => w,
        (Some(Amount::Given(n)), None) => *n,
        _ => return None,
    };
    let count = match &field.count {
        None => 1,
        Some(Amount::Given(n)) => *n,
        Some(_) => return None,
    };
    width.checked_mul(count)
}

/// The bytes the fields take, when none of their sizes comes from the file.
pub(crate) fn given_width(fields: &[Field]) -> Option<u64> {
    fields
        .iter()
        .try_fold(0u64, |sum, f| sum.checked_add(field_width(f)?))
}

/// The bytes `fields` take, when none of their sizes comes from the file.
pub(crate) fn fields_width(fields: &[Field]) -> Option<u64> {
    given_width(fields)
}
