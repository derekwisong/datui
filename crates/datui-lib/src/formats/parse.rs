//! Reading a spec's TOML into the model, with each error's place in the text.

use super::*;

/// The byte-order mark a text file may start with.
pub(crate) const UTF8_BOM: &[u8] = b"\xEF\xBB\xBF";

/// What a spec's `match` says files of it look like.
#[derive(Default)]
pub(crate) struct MatchRules {
    pub(crate) globs: Vec<String>,
    pub(crate) glob_set: Option<GlobSet>,
    pub(crate) magic: Vec<u8>,
    pub(crate) magic_offset: u64,
    pub(crate) expect: Vec<(String, Expected)>,
}

/// Line and column (one-based) of byte `offset` in `text`.
pub(crate) fn line_column(text: &str, offset: usize) -> (usize, usize) {
    let mut offset = offset.min(text.len());
    while !text.is_char_boundary(offset) {
        offset -= 1;
    }
    let before = &text[..offset];
    let line = before.matches('\n').count() + 1;
    let column = before
        .rsplit('\n')
        .next()
        .map_or(0, |last| last.chars().count())
        + 1;
    (line, column)
}

/// Which part of a file a field belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Part {
    Header,
    Footer,
    /// A record, a block header, a payload header or a group item: read one at a
    /// time, so a field may size a later one.
    Records,
}

/// The keys a field takes.
const FIELD_KEYS: &[&str] = &[
    "name",
    "type",
    "size",
    "size_adjust",
    "count",
    "flatten",
    "null",
    "time",
    "epoch",
    "of_day",
    "date",
    "scale",
    "factor",
    "offset",
    "enum",
    "file",
    "encoding",
    "delta",
    "bits",
    "group",
    "string_at",
    "lookup",
    "description",
    "unit",
];

/// The parts a field can refer to by name: `header.NAME`, `footer.NAME`.
#[derive(Clone, Copy, Default)]
pub(crate) struct Scopes<'f> {
    pub(crate) header: &'f [Field],
    pub(crate) footer: &'f [Field],
}

/// Reads a spec's TOML, keeping where each value was so a problem can say.
pub(crate) struct Reader<'a> {
    pub(crate) text: &'a str,
    pub(crate) path: Option<&'a Path>,
}

type Value<'i> = toml::Spanned<DeValue<'i>>;

impl Reader<'_> {
    pub(crate) fn error(&self, span: &Range<usize>, message: impl Into<String>) -> SpecError {
        let (line, column) = line_column(self.text, span.start);
        SpecError {
            path: self.path.map(Path::to_path_buf),
            line,
            column,
            message: message.into(),
        }
    }

    /// The table's entries, after checking every key is one of `known`.
    pub(crate) fn entries<'t, 'i>(
        &self,
        table: &'t DeTable<'i>,
        what: &str,
        known: &[&str],
    ) -> Result<BTreeMap<&'t str, &'t Value<'i>>, SpecError> {
        let mut out = BTreeMap::new();
        for (key, value) in table {
            let name: &str = key.get_ref();
            if !known.contains(&name) {
                return Err(self.error(
                    &key.span(),
                    format!(
                        "unknown key `{name}` in {what}; expected one of {}",
                        known.join(", ")
                    ),
                ));
            }
            out.insert(name, value);
        }
        Ok(out)
    }

    pub(crate) fn string(&self, value: &Value<'_>, what: &str) -> Result<String, SpecError> {
        match value.get_ref() {
            DeValue::String(s) => Ok(s.to_string()),
            _ => Err(self.error(&value.span(), format!("{what}: expected a string"))),
        }
    }

    /// Documentation text: trimmed, and never empty.
    pub(crate) fn prose(&self, value: &Value<'_>, what: &str) -> Result<String, SpecError> {
        let text = self.string(value, what)?.trim().to_string();
        if text.is_empty() {
            return Err(self.error(&value.span(), format!("{what}: must not be empty")));
        }
        Ok(text)
    }

    /// A link to documentation: an `https://` URL, as a catalog's `documentation` is.
    pub(crate) fn documentation(&self, value: &Value<'_>) -> Result<String, SpecError> {
        let link = self.prose(value, "documentation")?;
        if !link.starts_with("https://") || link.chars().any(char::is_whitespace) {
            return Err(self.error(
                &value.span(),
                format!("documentation: \"{link}\" is not an https:// link"),
            ));
        }
        Ok(link)
    }

    fn integer(&self, value: &Value<'_>, what: &str) -> Result<i64, SpecError> {
        match value.get_ref() {
            DeValue::Integer(i) => {
                let digits = i.as_str().replace('_', "");
                i64::from_str_radix(&digits, i.radix())
                    .map_err(|_| self.error(&value.span(), format!("{what}: too large")))
            }
            _ => Err(self.error(&value.span(), format!("{what}: expected an integer"))),
        }
    }

    fn number(&self, value: &Value<'_>, what: &str) -> Result<f64, SpecError> {
        match value.get_ref() {
            DeValue::Integer(_) => Ok(self.integer(value, what)? as f64),
            DeValue::Float(f) => f
                .as_str()
                .replace('_', "")
                .parse::<f64>()
                .ok()
                .filter(|v| v.is_finite())
                .ok_or_else(|| {
                    self.error(&value.span(), format!("{what}: expected a finite number"))
                }),
            _ => Err(self.error(&value.span(), format!("{what}: expected a number"))),
        }
    }

    fn boolean(&self, value: &Value<'_>, what: &str) -> Result<bool, SpecError> {
        match value.get_ref() {
            DeValue::Boolean(b) => Ok(*b),
            _ => Err(self.error(&value.span(), format!("{what}: expected true or false"))),
        }
    }

    pub(crate) fn table<'t, 'i>(
        &self,
        value: &'t Value<'i>,
        what: &str,
    ) -> Result<&'t DeTable<'i>, SpecError> {
        match value.get_ref() {
            DeValue::Table(t) => Ok(t),
            _ => Err(self.error(&value.span(), format!("{what}: expected a table"))),
        }
    }

    /// The earlier field `reference` names: `header.NAME`, `footer.NAME`, or `NAME`
    /// for an earlier field of the same part.
    fn earlier<'f>(
        &self,
        value: &Value<'_>,
        reference: &str,
        what: &str,
        earlier: &'f [Field],
        scopes: &Scopes<'f>,
    ) -> Result<(&'f Field, Option<&'static str>), SpecError> {
        let (scope, field) = match reference.split_once('.') {
            Some((scope, field)) => (Some(scope), field),
            None => (None, reference),
        };
        let (fields, scope) = match scope {
            Some("header") => (scopes.header, Some("header")),
            Some("footer") => (scopes.footer, Some("footer")),
            None => (earlier, None),
            Some(other) => {
                return Err(self.error(
                    &value.span(),
                    format!(
                        "{what}: `{other}.` is not a part; expected `header.NAME` or `footer.NAME`"
                    ),
                ));
            }
        };
        fields
            .iter()
            .find(|f| f.name.as_deref() == Some(field))
            .map(|f| (f, scope))
            .ok_or_else(|| {
                let part = scope.unwrap_or("earlier");
                let hint = if scope.is_none()
                    && scopes
                        .header
                        .iter()
                        .any(|f| f.name.as_deref() == Some(field))
                {
                    format!("; a header field is named `header.{field}`")
                } else {
                    String::new()
                };
                self.error(
                    &value.span(),
                    format!("{what}: no {part} field named `{field}`{hint}"),
                )
            })
    }

    /// A size or a count: a whole number, `rest`, or the name of an earlier plain
    /// integer field, with an optional `adjust`.
    pub(crate) fn amount(
        &self,
        value: &Value<'_>,
        adjust: Option<&Value<'_>>,
        what: &str,
        part: Part,
        earlier: &[Field],
        scopes: &Scopes<'_>,
    ) -> Result<Amount, SpecError> {
        let adjust = adjust
            .map(|a| self.integer(a, &format!("{what}_adjust")))
            .transpose()?;
        match value.get_ref() {
            DeValue::Integer(_) => {
                let n = self.integer(value, what)?;
                if adjust.is_some() {
                    return Err(self.error(
                        &value.span(),
                        format!("{what}_adjust goes with a {what} read from a field"),
                    ));
                }
                if n < 0 || n as u64 > MAX_SIZE {
                    return Err(
                        self.error(&value.span(), format!("{what}: expected 0 to {MAX_SIZE}"))
                    );
                }
                Ok(Amount::Given(n as u64))
            }
            DeValue::String(reference) if reference.as_ref() == "rest" && what == "size" => {
                if part != Part::Records {
                    return Err(
                        self.error(&value.span(), "size = \"rest\" is for a field of a record")
                    );
                }
                if adjust.is_some() {
                    return Err(self.error(
                        &value.span(),
                        "size_adjust goes with a size read from a field",
                    ));
                }
                Ok(Amount::Rest)
            }
            DeValue::String(reference) => {
                let (target, scope) = self.earlier(value, reference, what, earlier, scopes)?;
                if !matches!(
                    target.ty,
                    Type::Unsigned(_) | Type::Signed(_) | Type::VarU | Type::VarS
                ) || target.meaning != Meaning::Plain
                    || target.count.is_some()
                    || target.delta != Delta::None
                {
                    return Err(self.error(
                        &value.span(),
                        format!("{what}: `{reference}` is not a plain integer field"),
                    ));
                }
                let field = target.name.clone().expect("a referenced field is named");
                let adjust = adjust.unwrap_or(0);
                Ok(match (scope, part) {
                    (Some("footer"), _) | (None, Part::Footer) => Amount::Footer { field, adjust },
                    (Some(_), _) | (None, Part::Header) => Amount::Header { field, adjust },
                    (None, Part::Records) => Amount::Record { field, adjust },
                })
            }
            _ => Err(self.error(
                &value.span(),
                format!("{what}: expected a whole number or the name of an earlier field"),
            )),
        }
    }

    /// A spec's `name`: namespaced, such as `acme.l2feed`.
    pub(crate) fn spec_name(&self, top: &BTreeMap<&str, &Value<'_>>) -> Result<String, SpecError> {
        match top.get("name") {
            Some(v) => {
                let name = self.string(v, "name")?;
                if !is_spec_name(&name) {
                    return Err(self.error(
                        &v.span(),
                        "name: expected a namespaced name of letters, digits, `_` and `-`, such as acme.l2feed",
                    ));
                }
                Ok(name)
            }
            None => Err(self.error(&(0..0), "missing `name`, such as name = \"acme.l2feed\"")),
        }
    }

    /// A spec's `match`: its globs, magic and, given a binary header's fields, the
    /// header values a file must hold.
    pub(crate) fn match_rules(
        &self,
        v: &Value<'_>,
        header: Option<&[Field]>,
    ) -> Result<MatchRules, SpecError> {
        let (mut globs, mut magic, mut magic_offset) = (Vec::new(), Vec::new(), 0u64);
        let mut glob_set = None;
        let mut expect = Vec::new();
        let table = self.table(v, "match")?;
        let keys = self.entries(table, "match", &["glob", "magic", "magic_offset", "where"])?;
        if let Some(g) = keys.get("glob") {
            globs = match g.get_ref() {
                DeValue::String(s) => vec![s.to_string()],
                DeValue::Array(items) => items
                    .iter()
                    .map(|item| self.string(item, "glob"))
                    .collect::<Result<_, _>>()?,
                _ => {
                    return Err(self.error(&g.span(), "glob: expected a string or a list of them"));
                }
            };
            let mut builder = GlobSetBuilder::new();
            for glob in &globs {
                builder.add(
                    Glob::new(glob).map_err(|e| {
                        self.error(&g.span(), format!("glob `{glob}`: {}", e.kind()))
                    })?,
                );
            }
            glob_set = Some(
                builder
                    .build()
                    .map_err(|e| self.error(&g.span(), format!("glob: {e}")))?,
            );
        }
        if let Some(m) = keys.get("magic") {
            magic = match m.get_ref() {
                DeValue::String(s) => s.as_bytes().to_vec(),
                DeValue::Array(items) => items
                    .iter()
                    .map(|item| {
                        let byte = self.integer(item, "magic")?;
                        u8::try_from(byte)
                            .map_err(|_| self.error(&item.span(), "magic: a byte is 0 to 255"))
                    })
                    .collect::<Result<_, _>>()?,
                _ => {
                    return Err(
                        self.error(&m.span(), "magic: expected a string or a list of bytes")
                    );
                }
            };
            if magic.is_empty() || magic.len() as u64 > MAX_MATCH_READ {
                return Err(self.error(&m.span(), "magic: expected 1 to 65536 bytes"));
            }
        }
        if let Some(o) = keys.get("magic_offset") {
            let offset = self.integer(o, "magic_offset")?;
            let room = MAX_MATCH_READ - magic.len() as u64;
            if offset < 0 || offset as u64 > room {
                return Err(self.error(&o.span(), format!("magic_offset: expected 0 to {room}")));
            }
            magic_offset = offset as u64;
        }
        if let Some(w) = keys.get("where") {
            let Some(header_fields) = header else {
                return Err(self.error(
                    &w.span(),
                    "`where` compares a binary header's fields; a delimited spec matches by glob and magic",
                ));
            };
            let table = self.table(w, "where")?;
            for (key, value) in table {
                let reference: &str = key.get_ref();
                let Some(field) = reference.strip_prefix("header.") else {
                    return Err(self.error(
                        &key.span(),
                        "where: expected header fields, such as \"header.version\" = 3",
                    ));
                };
                let Some(target) = header_fields
                    .iter()
                    .find(|f| f.name.as_deref() == Some(field))
                else {
                    return Err(self.error(
                        &key.span(),
                        format!("where: no header field named `{field}`"),
                    ));
                };
                let wanted = self.expected(value, target, "where")?;
                expect.push((field.to_string(), wanted));
            }
        }
        Ok(MatchRules {
            globs,
            glob_set,
            magic,
            magic_offset,
            expect,
        })
    }

    /// The reading options of a `kind = "delimited"` spec, from its top-level keys.
    pub(crate) fn delimited(
        &self,
        top: &BTreeMap<&str, &Value<'_>>,
    ) -> Result<
        (
            crate::formats::delimited_spec::Delimited,
            Vec<(String, ColumnNote)>,
        ),
        SpecError,
    > {
        use crate::formats::delimited_spec::{
            Delimited, Derived, DerivedKind, HeaderRows, MAX_HEAD_LINE,
        };
        let line = |value: &Value<'_>, what: &str| -> Result<usize, SpecError> {
            let n = self.integer(value, what)?;
            if n < 1 || n as usize > MAX_HEAD_LINE {
                return Err(self.error(
                    &value.span(),
                    format!("{what}: expected a line from 1 to {MAX_HEAD_LINE}"),
                ));
            }
            Ok(n as usize)
        };
        let lines = |value: &Value<'_>, what: &str| -> Result<Vec<usize>, SpecError> {
            match value.get_ref() {
                DeValue::Integer(_) => Ok(vec![line(value, what)?]),
                DeValue::Array(items) if !items.is_empty() => {
                    let mut rows = Vec::with_capacity(items.len());
                    for item in items {
                        let n = line(item, what)?;
                        if rows.contains(&n) {
                            return Err(self
                                .error(&item.span(), format!("{what}: line {n} is named twice")));
                        }
                        rows.push(n);
                    }
                    Ok(rows)
                }
                _ => Err(self.error(
                    &value.span(),
                    format!("{what}: expected a line number or a list of them"),
                )),
            }
        };
        let mut spec = Delimited::default();
        // Each note with where its key sits, to keep the file's order: the table reads
        // back sorted by name.
        let mut notes: Vec<(usize, String, ColumnNote)> = Vec::new();
        // The `[csv]` keys and `--delimiter`'s words, read by the same rules.
        if let Some(v) = top.get("delimiter") {
            let text = self.string(v, "delimiter")?;
            let byte = datui_cli::parse_delimiter(&text)
                .map_err(|e| self.error(&v.span(), format!("delimiter: {e}")))?;
            spec.delimiter = Some(byte);
        }
        if let Some(v) = top.get("comment") {
            let text = self.string(v, "comment")?;
            crate::formats::csv_dialect::check_comment_char(&text)
                .map_err(|e| self.error(&v.span(), format!("comment: {e}")))?;
            spec.comment_char = Some(text);
        }
        if let Some(v) = top.get("skip_initial_space") {
            spec.skip_initial_space = Some(self.boolean(v, "skip_initial_space")?);
        }
        if let Some(v) = top.get("header_join") {
            spec.header_join = Some(self.string(v, "header_join")?);
        }
        if let Some(v) = top.get("skip_lines") {
            let n = self.integer(v, "skip_lines")?;
            if n < 0 || n > i64::from(u32::MAX) {
                return Err(self.error(&v.span(), "skip_lines: expected 0 or more"));
            }
            spec.skip_lines = Some(n as usize);
        }
        if let Some(v) = top.get("null_values") {
            spec.null_values = match v.get_ref() {
                DeValue::String(s) => vec![s.to_string()],
                DeValue::Array(items) => items
                    .iter()
                    .map(|item| self.string(item, "null_values"))
                    .collect::<Result<_, _>>()?,
                _ => {
                    return Err(self.error(
                        &v.span(),
                        "null_values: expected a string or a list of them, such as \"NA\" or \"COL=-999\"",
                    ));
                }
            };
        }
        if let Some(v) = top.get("header_rows") {
            let rows = match v.get_ref() {
                DeValue::Table(table) => {
                    let keys =
                        self.entries(table, "header_rows", &["name", "unit", "description"])?;
                    if let Some(d) = keys.get("description") {
                        return Err(
                            self.error(&d.span(), "header_rows.description is not yet supported")
                        );
                    }
                    let Some(name) = keys.get("name") else {
                        return Err(self.error(
                            &v.span(),
                            "header_rows: missing `name`, the line that names the columns",
                        ));
                    };
                    let name = lines(name, "header_rows.name")?;
                    let unit = keys
                        .get("unit")
                        .map(|u| {
                            let n = line(u, "header_rows.unit")?;
                            if name.contains(&n) {
                                return Err(self.error(
                                    &u.span(),
                                    format!("header_rows.unit: line {n} is also a name line"),
                                ));
                            }
                            Ok(n)
                        })
                        .transpose()?;
                    HeaderRows { name, unit }
                }
                _ => HeaderRows {
                    name: lines(v, "header_rows")?,
                    unit: None,
                },
            };
            spec.header_rows = Some(rows);
        }
        if let Some(v) = top.get("metadata_line") {
            let n = line(v, "metadata_line")?;
            let header = spec.header_rows.as_ref();
            if header.is_some_and(|h| h.name.contains(&n) || h.unit == Some(n)) {
                return Err(self.error(
                    &v.span(),
                    format!("metadata_line: line {n} is a header line"),
                ));
            }
            let before_data = header
                .map_or(0, HeaderRows::last)
                .max(spec.skip_lines.unwrap_or(0));
            if n > before_data && spec.comment_char.is_none() {
                return Err(self.error(
                    &v.span(),
                    format!(
                        "metadata_line: line {n} would be read as data; put it above header_rows, or set skip_lines or comment"
                    ),
                ));
            }
            spec.metadata_line = Some(n);
        }
        if let Some(v) = top.get("columns") {
            let table = self.table(v, "[columns]")?;
            for (key, value) in table {
                let name: &str = key.get_ref();
                let what = format!("columns.{name}");
                let entry = self.table(value, &what)?;
                let keys = self.entries(
                    entry,
                    &what,
                    &["from", "as", "format", "description", "unit", "type"],
                )?;
                if name.trim().is_empty() {
                    return Err(self.error(&key.span(), "columns: a column needs a name"));
                }
                // A column of the file, or a derived one, may say what it means.
                let said = |k: &str| {
                    keys.get(k)
                        .map(|v| self.prose(v, &format!("{what}.{k}")))
                        .transpose()
                        .map(Option::unwrap_or_default)
                };
                // A column of the file read as a type: `{ type = "date", format = ... }`.
                let typed = match keys.get("type") {
                    Some(t) => {
                        if let Some(k) = ["from", "as"].iter().find(|k| keys.contains_key(*k)) {
                            return Err(self.error(
                                &keys[k].span(),
                                format!(
                                    "{what}: a derived column takes `as` for its type, not `type`"
                                ),
                            ));
                        }
                        let type_name = self.string(t, &format!("{what}.type"))?;
                        let format = keys
                            .get("format")
                            .map(|f| self.string(f, &format!("{what}.format")))
                            .transpose()?;
                        let ty =
                            crate::formats::column_types::ColumnType::named(&type_name, format)
                                .map_err(|e| self.error(&t.span(), format!("{what}.type: {e}")))?;
                        Some(ty)
                    }
                    None => None,
                };
                let note = ColumnNote {
                    description: said("description")?,
                    unit: said("unit")?,
                    values: Vec::new(),
                    ty: typed.as_ref().map(|t| t.name()).unwrap_or_default(),
                };
                let documented =
                    !(note.description.is_empty() && note.unit.is_empty() && note.ty.is_empty());
                if documented {
                    notes.push((key.span().start, name.to_string(), note));
                }
                if let Some(ty) = typed {
                    spec.types.push((name.to_string(), ty));
                    continue;
                }
                let derived = ["from", "as", "format"]
                    .iter()
                    .any(|k| keys.contains_key(k));
                if documented && !derived {
                    continue;
                }
                let Some(from) = keys.get("from") else {
                    return Err(self.error(
                        &value.span(),
                        format!(
                            "{what}: missing `from`, the columns it is made from (a column of the file takes description or unit alone)"
                        ),
                    ));
                };
                let from_columns: Vec<String> = match from.get_ref() {
                    DeValue::String(s) => vec![s.to_string()],
                    DeValue::Array(items) => items
                        .iter()
                        .map(|item| self.string(item, &format!("{what}.from")))
                        .collect::<Result<_, _>>()?,
                    _ => {
                        return Err(self.error(
                            &from.span(),
                            format!("{what}.from: expected a column name or a list of them"),
                        ));
                    }
                };
                let Some(kind) = keys.get("as") else {
                    return Err(self.error(
                        &value.span(),
                        format!("{what}: missing `as`; expected datetime, date or time"),
                    ));
                };
                let kind = match self.string(kind, &format!("{what}.as"))?.as_str() {
                    "datetime" => DerivedKind::Datetime,
                    "date" => DerivedKind::Date,
                    "time" => DerivedKind::Time,
                    _ => {
                        return Err(self.error(
                            &kind.span(),
                            format!("{what}.as: expected datetime, date or time"),
                        ));
                    }
                };
                let most = if kind == DerivedKind::Datetime { 3 } else { 1 };
                if from_columns.is_empty() || from_columns.len() > most {
                    let expected = if most == 3 {
                        "1 to 3 columns: a date, a time and a UTC offset"
                    } else {
                        "one column"
                    };
                    return Err(self.error(
                        &from.span(),
                        format!("{what}.from: as = \"{}\" takes {expected}", kind.name()),
                    ));
                }
                let format = keys
                    .get("format")
                    .map(|f| self.string(f, &format!("{what}.format")))
                    .transpose()?;
                spec.columns.push(Derived {
                    name: name.to_string(),
                    from: from_columns,
                    kind,
                    format,
                });
            }
        }
        notes.sort_by_key(|(at, ..)| *at);
        let notes = notes
            .into_iter()
            .map(|(_, name, note)| (name, note))
            .collect();
        Ok((spec, notes))
    }

    pub(crate) fn fields(
        &self,
        value: &Value<'_>,
        part: Part,
        layout: Layout,
        scopes: &Scopes<'_>,
        prefix: &[Field],
    ) -> Result<Vec<Field>, SpecError> {
        let DeValue::Array(items) = value.get_ref() else {
            return Err(self.error(
                &value.span(),
                "fields: expected an array of tables, such as [{ name = \"ts\", type = \"u8\" }]",
            ));
        };
        let mut fields: Vec<Field> = prefix.to_vec();
        let mut names: std::collections::HashSet<String> =
            prefix.iter().flat_map(output_names).collect();
        for item in items {
            let field = self.field(item, part, layout, &fields, scopes)?;
            for name in output_names(&field) {
                if !names.insert(name.clone()) {
                    return Err(self.error(&item.span(), format!("a second field named `{name}`")));
                }
            }
            fields.push(field);
        }
        Ok(fields.split_off(prefix.len()))
    }

    fn field(
        &self,
        value: &Value<'_>,
        part: Part,
        layout: Layout,
        earlier: &[Field],
        scopes: &Scopes<'_>,
    ) -> Result<Field, SpecError> {
        let table = self.table(value, "field")?;
        let mut keys = self.entries(table, "a field", FIELD_KEYS)?;
        let at = value.span();
        let (ty, endian) = match (keys.get("type"), keys.get("group")) {
            (Some(_), Some(g)) => {
                return Err(self.error(&g.span(), "group: a group has no type of its own"));
            }
            (None, Some(_)) => (Type::Group, None),
            (None, None) => return Err(self.error(&at, "field: missing `type`")),
            (Some(ty_value), None) => parse_type(&self.string(ty_value, "type")?).ok_or_else(|| {
                self.error(
                    &ty_value.span(),
                    "type: expected u1 to u8, s1 to s8, f2, f4, f8 (each with an optional le or be), bf2, vu, vs, bool, str, strz, bytes or pad",
                )
            })?,
        };
        let name = keys
            .get("name")
            .map(|v| {
                let name = self.string(v, "name")?;
                if name.trim().is_empty() {
                    return Err(self.error(&v.span(), "name: must not be empty"));
                }
                if name.contains('.') {
                    return Err(self.error(&v.span(), "name: must not contain `.`"));
                }
                Ok(name)
            })
            .transpose()?;
        if ty == Type::Pad {
            if let Some(key) = keys
                .keys()
                .find(|k| !matches!(**k, "type" | "size" | "size_adjust"))
            {
                return Err(self.error(
                    &keys[key].span(),
                    format!("pad: skipped bytes take only a size, not `{key}`"),
                ));
            }
        } else if name.is_none() {
            return Err(self.error(&at, "field: missing `name` (only pad goes without)"));
        }
        // A string offset is where a column starts (`offset = "header.px_off"`); a
        // number is the linear conversion's.
        let column_at = match keys.get("offset") {
            Some(v)
                if matches!(v.get_ref(), DeValue::String(_))
                    && layout == Layout::Columns
                    && part == Part::Records =>
            {
                let v = keys.remove("offset").expect("just read");
                Some(self.amount(v, None, "offset", Part::Header, &[], scopes)?)
            }
            _ => None,
        };
        if part != Part::Records
            && let Some(key) = ["delta", "bits", "group", "string_at", "lookup"]
                .iter()
                .find(|k| keys.contains_key(**k))
        {
            return Err(self.error(
                &keys[*key].span(),
                format!("{key}: is for a field of the records"),
            ));
        }
        if part != Part::Records && matches!(ty, Type::VarU | Type::VarS | Type::Strz) {
            return Err(self.error(
                &at,
                format!("{}: is for a field of the records", type_name(ty)),
            ));
        }
        let size = match (ty.width(), keys.get("size")) {
            (Some(width), Some(v)) => {
                return Err(self.error(
                    &v.span(),
                    format!(
                        "size: a {} is {width} bytes; size is for str, strz, bytes and pad",
                        type_name(ty)
                    ),
                ));
            }
            (None, Some(v)) if matches!(ty, Type::VarU | Type::VarS | Type::Group) => {
                return Err(
                    self.error(&v.span(), format!("size: a {} sizes itself", type_name(ty)))
                );
            }
            (Some(_), None) => None,
            (None, Some(v)) => Some(self.amount(
                v,
                keys.get("size_adjust").copied(),
                "size",
                part,
                earlier,
                scopes,
            )?),
            (None, None) if matches!(ty, Type::Strz | Type::VarU | Type::VarS | Type::Group) => {
                None
            }
            (None, None) => {
                return Err(self.error(&at, format!("{}: missing `size`", type_name(ty))));
            }
        };
        if !size.as_ref().is_some_and(|s| {
            matches!(
                s,
                Amount::Header { .. } | Amount::Footer { .. } | Amount::Record { .. }
            )
        }) && let Some(v) = keys.get("size_adjust")
        {
            return Err(self.error(&v.span(), "size_adjust goes with a size read from a field"));
        }
        if layout == Layout::Columns
            && part == Part::Records
            && size.as_ref().is_some_and(|s| !s.is_fixed())
        {
            return Err(self.error(
                &keys["size"].span(),
                "size: a column file's values are all one size",
            ));
        }
        let mut group = Vec::new();
        let mut count = keys
            .get("count")
            .map(|v| {
                if ty == Type::Group {
                    return Err(self.error(
                        &v.span(),
                        "count: a group's count goes inside group = { count = ... }",
                    ));
                }
                let count = self.amount(v, None, "count", part, earlier, scopes)?;
                if count == Amount::Given(0) {
                    return Err(self.error(&v.span(), "count: expected at least 1"));
                }
                Ok(count)
            })
            .transpose()?;
        if ty == Type::Group {
            let v = keys["group"];
            if layout == Layout::Columns {
                return Err(self.error(&v.span(), "group: a column file holds one value a row"));
            }
            let table = self.table(v, "group")?;
            let inner = self.entries(table, "group", &["count", "fields"])?;
            let Some(c) = inner.get("count") else {
                return Err(self.error(
                    &v.span(),
                    "group: missing `count`, such as count = \"n_levels\"",
                ));
            };
            count = Some(self.amount(c, None, "count", part, earlier, scopes)?);
            let Some(f) = inner.get("fields") else {
                return Err(self.error(&v.span(), "group: missing `fields`"));
            };
            group = self.fields(f, Part::Records, Layout::Rows, scopes, &[])?;
            if !group.iter().any(|f| f.name.is_some()) {
                return Err(self.error(&f.span(), "fields: a group item needs a named field"));
            }
        }
        let flatten = keys
            .get("flatten")
            .map(|v| self.boolean(v, "flatten"))
            .transpose()?
            .unwrap_or(false);
        if flatten {
            let v = keys["flatten"];
            match &count {
                Some(Amount::Given(n)) if *n > MAX_FLATTEN => {
                    return Err(self.error(
                        &v.span(),
                        format!(
                            "flatten: at most {MAX_FLATTEN} columns; leave {n} values an Array"
                        ),
                    ));
                }
                Some(Amount::Given(_)) if ty != Type::Group => {}
                _ => {
                    return Err(self.error(
                        &v.span(),
                        "flatten: goes with a count written in the spec, such as count = 10",
                    ));
                }
            }
        }
        if matches!(count, Some(Amount::Rest)) {
            return Err(self.error(&keys["count"].span(), "count: expected a number or a field"));
        }

        let null = keys
            .get("null")
            .map(|v| {
                let null = match v.get_ref() {
                    DeValue::String(s) => match s.as_ref() {
                        "min" => Null::Min,
                        "max" => Null::Max,
                        "nan" => Null::NaN,
                        _ => {
                            return Err(self.error(
                                &v.span(),
                                "null: expected \"min\", \"max\", \"nan\" or an integer",
                            ));
                        }
                    },
                    DeValue::Integer(_) => Null::Value(i128::from(self.integer(v, "null")?)),
                    _ => {
                        return Err(self.error(
                            &v.span(),
                            "null: expected \"min\", \"max\", \"nan\" or an integer",
                        ));
                    }
                };
                let fits = match (null, ty) {
                    (Null::Min | Null::Max, t) => matches!(t, Type::Unsigned(_) | Type::Signed(_)),
                    (Null::NaN, t) => matches!(t, Type::Float(_) | Type::BFloat16),
                    (Null::Value(_), t) => t.is_number() || t == Type::Bool,
                };
                if !fits {
                    return Err(
                        self.error(&v.span(), format!("null: does not fit a {}", type_name(ty)))
                    );
                }
                // A sentinel the type cannot hold would never match.
                if let (Null::Value(value), Some((low, high))) = (null, integer_range(ty))
                    && !(low..=high).contains(&value)
                {
                    return Err(self.error(
                        &v.span(),
                        format!(
                            "null: a {} holds {low} to {high}, not {value}",
                            type_name(ty)
                        ),
                    ));
                }
                Ok(null)
            })
            .transpose()?;

        let meaning = self.meaning(&keys, ty)?;
        let file = keys
            .get("file")
            .map(|v| {
                if part != Part::Records || layout == Layout::Rows {
                    return Err(self.error(
                        &v.span(),
                        "file: only a record field of layout = \"columns\" has a file of its own",
                    ));
                }
                let file = self.string(v, "file")?;
                if !is_file_name(&file) {
                    return Err(self.error(
                        &v.span(),
                        "file: expected the name of a file in the directory, such as px.dat",
                    ));
                }
                Ok(file)
            })
            .transpose()?;
        if let (Some(_), Some(v)) = (&column_at, keys.get("file")) {
            return Err(self.error(&v.span(), "file: a column at an offset is in the one file"));
        }
        // Its name names its file.
        if layout == Layout::Columns
            && part == Part::Records
            && file.is_none()
            && column_at.is_none()
            && let Some(name) = &name
            && !is_file_name(name)
        {
            return Err(self.error(
                &keys["name"].span(),
                "name: names the column's file, so it cannot hold a path; give the file with file = \"...\"",
            ));
        }
        let encoding = keys
            .get("encoding")
            .map(|v| {
                if !ty.is_text() {
                    return Err(self.error(&v.span(), "encoding: is for str and strz"));
                }
                match self.string(v, "encoding")?.as_str() {
                    "utf8" | "utf-8" | "ascii" => Ok(Encoding::Utf8),
                    "latin1" | "iso-8859-1" => Ok(Encoding::Latin1),
                    "utf16le" | "utf-16le" => Ok(Encoding::Utf16Le),
                    "utf16be" | "utf-16be" => Ok(Encoding::Utf16Be),
                    _ => Err(self.error(
                        &v.span(),
                        "encoding: expected utf8, latin1, utf16le or utf16be",
                    )),
                }
            })
            .transpose()?
            .unwrap_or_default();
        let delta = keys
            .get("delta")
            .map(|v| {
                if !ty.is_integer() || count.is_some() {
                    return Err(self.error(&v.span(), "delta: is for one integer a record"));
                }
                match v.get_ref() {
                    DeValue::Boolean(true) => Ok(Delta::All),
                    DeValue::Boolean(false) => Ok(Delta::None),
                    DeValue::String(s) if s.as_ref() == "block" => Ok(Delta::Block),
                    _ => Err(self.error(&v.span(), "delta: expected true or \"block\"")),
                }
            })
            .transpose()?
            .unwrap_or_default();
        if delta != Delta::None && layout == Layout::Columns {
            return Err(self.error(
                &keys["delta"].span(),
                "delta: is for records, not column files",
            ));
        }
        let bits = match keys.get("bits") {
            None => Vec::new(),
            Some(v) => self.bits(v, ty, count.is_some())?,
        };
        let string_at = keys
            .get("string_at")
            .map(|v| {
                if !matches!(ty, Type::Unsigned(_)) || meaning != Meaning::Plain || count.is_some()
                {
                    return Err(self.error(
                        &v.span(),
                        "string_at: is for an unsigned offset, one a record",
                    ));
                }
                self.string(v, "string_at")
            })
            .transpose()?;
        let lookup = keys
            .get("lookup")
            .map(|v| {
                if !matches!(ty, Type::Unsigned(_) | Type::Signed(_)) || meaning != Meaning::Plain {
                    return Err(self.error(&v.span(), "lookup: is for a plain integer index"));
                }
                let table = self.table(v, "lookup")?;
                let inner = self.entries(table, "lookup", &["file", "format"])?;
                let Some(f) = inner.get("file") else {
                    return Err(self.error(&v.span(), "lookup: missing `file`"));
                };
                let file = self.string(f, "file")?;
                if file.trim().is_empty() || Path::new(&file).is_absolute() {
                    return Err(self.error(
                        &f.span(),
                        "file: expected a path relative to the data, such as ../sym",
                    ));
                }
                let format = match inner.get("format") {
                    None => LookupFormat::Lines,
                    Some(fv) => {
                        let text = self.string(fv, "format")?;
                        match text.as_str() {
                            "lines" => LookupFormat::Lines,
                            "nul" => LookupFormat::Nul,
                            _ => match text
                                .strip_prefix("str:")
                                .and_then(|n| n.parse::<u64>().ok())
                            {
                                Some(n) if (1..=MAX_SIZE).contains(&n) => LookupFormat::Fixed(n),
                                _ => {
                                    return Err(self.error(
                                        &fv.span(),
                                        "format: expected lines, nul or str:N",
                                    ));
                                }
                            },
                        }
                    }
                };
                Ok(Lookup { file, format })
            })
            .transpose()?;
        if lookup.is_some() && string_at.is_some() {
            return Err(self.error(
                &keys["lookup"].span(),
                "lookup: a field takes one of lookup and string_at",
            ));
        }
        let description = keys
            .get("description")
            .map(|v| self.prose(v, "description"))
            .transpose()?;
        let unit = keys
            .get("unit")
            .map(|v| self.prose(v, "unit"))
            .transpose()?;
        Ok(Field {
            name,
            ty,
            size,
            endian,
            meaning,
            null,
            count,
            flatten,
            file,
            at: column_at,
            encoding,
            delta,
            bits,
            group,
            string_at,
            lookup,
            description,
            unit,
        })
    }

    /// A value a field must hold: an integer for an integer field, text for text.
    fn expected(
        &self,
        value: &Value<'_>,
        target: &Field,
        what: &str,
    ) -> Result<Expected, SpecError> {
        match value.get_ref() {
            DeValue::Integer(_) if target.ty.is_integer() && target.meaning == Meaning::Plain => {
                Ok(Expected::Int(i128::from(self.integer(value, what)?)))
            }
            DeValue::String(s) if target.ty.is_text() => Ok(Expected::Text(s.to_string())),
            _ => Err(self.error(
                &value.span(),
                format!(
                    "{what}: `{}` is a {}; expected a value of that type",
                    target.name.as_deref().unwrap_or("pad"),
                    type_name(target.ty)
                ),
            )),
        }
    }

    /// Bytes written as hex (`"1ACFFC1D"`, `"0x1a cf"`) or as a list of bytes.
    fn hex_bytes(&self, value: &Value<'_>, what: &str) -> Result<Vec<u8>, SpecError> {
        let bad = || {
            self.error(
                &value.span(),
                format!("{what}: expected hex such as \"1ACFFC1D\", or a list of bytes"),
            )
        };
        let bytes = match value.get_ref() {
            DeValue::String(s) => {
                let digits: String = s
                    .trim()
                    .trim_start_matches("0x")
                    .chars()
                    .filter(|c| !c.is_whitespace())
                    .collect();
                // Hex digits only, so every pair is two bytes of the string.
                if digits.is_empty()
                    || !digits.len().is_multiple_of(2)
                    || !digits.bytes().all(|b| b.is_ascii_hexdigit())
                {
                    return Err(bad());
                }
                (0..digits.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&digits[i..i + 2], 16).map_err(|_| bad()))
                    .collect::<Result<Vec<u8>, _>>()?
            }
            DeValue::Array(items) => items
                .iter()
                .map(|item| {
                    let byte = self.integer(item, what)?;
                    u8::try_from(byte).map_err(|_| {
                        self.error(&item.span(), format!("{what}: a byte is 0 to 255"))
                    })
                })
                .collect::<Result<_, _>>()?,
            _ => return Err(bad()),
        };
        if bytes.is_empty() || bytes.len() > 64 {
            return Err(self.error(&value.span(), format!("{what}: expected 1 to 64 bytes")));
        }
        Ok(bytes)
    }

    pub(crate) fn footer(&self, value: &Value<'_>, header: &[Field]) -> Result<Footer, SpecError> {
        let table = self.table(value, "[footer]")?;
        let keys = self.entries(table, "[footer]", &["fields", "size", "checksum"])?;
        let scopes = Scopes {
            header,
            footer: &[],
        };
        let fields = match keys.get("fields") {
            Some(f) => self.fields(f, Part::Footer, Layout::Rows, &scopes, &[])?,
            None => Vec::new(),
        };
        let width = given_width(&fields);
        let size = match keys.get("size") {
            None => None,
            Some(v) => {
                let n = self.integer(v, "size")?;
                if n < 0 || n as u64 > MAX_SIZE {
                    return Err(self.error(&v.span(), format!("size: expected 0 to {MAX_SIZE}")));
                }
                if width.is_some_and(|w| w > n as u64) {
                    return Err(self.error(
                        &v.span(),
                        format!(
                            "size: the fields take {} bytes, more than {n}",
                            width.unwrap_or(0)
                        ),
                    ));
                }
                Some(n as u64)
            }
        };
        if size.is_none() && width.is_none() {
            return Err(self.error(
                &value.span(),
                "[footer]: read from the end of the file, so its fields' sizes are written in the spec, or give its size",
            ));
        }
        let checksum = keys
            .get("checksum")
            .map(|v| {
                let table = self.table(v, "checksum")?;
                let inner = self.entries(table, "checksum", &["algo", "field"])?;
                let algo = self.algo(inner.get("algo").copied(), v)?;
                let Some(f) = inner.get("field") else {
                    return Err(self.error(
                        &v.span(),
                        "checksum: missing `field`, the footer field holding it",
                    ));
                };
                let field = self.string(f, "field")?;
                let field = field.strip_prefix("footer.").unwrap_or(&field).to_string();
                if !fields
                    .iter()
                    .any(|x| x.name.as_deref() == Some(field.as_str()) && x.ty.is_integer())
                {
                    return Err(self.error(
                        &f.span(),
                        format!("field: no integer footer field named `{field}`"),
                    ));
                }
                Ok((algo, field))
            })
            .transpose()?;
        Ok(Footer {
            fields,
            size,
            checksum,
        })
    }

    fn algo(&self, value: Option<&Value<'_>>, at: &Value<'_>) -> Result<ChecksumAlgo, SpecError> {
        let Some(v) = value else {
            return Err(self.error(&at.span(), "checksum: missing `algo`"));
        };
        ChecksumAlgo::parse(&self.string(v, "algo")?).ok_or_else(|| {
            let names: Vec<&str> = ChecksumAlgo::NAMES.iter().map(|(n, _)| *n).collect();
            self.error(
                &v.span(),
                format!("algo: expected one of {}", names.join(", ")),
            )
        })
    }

    pub(crate) fn sections(
        &self,
        value: &Value<'_>,
        scopes: &Scopes<'_>,
    ) -> Result<Vec<Section>, SpecError> {
        let table = self.table(value, "[sections]")?;
        let mut out = Vec::new();
        for (key, v) in table {
            let name: &str = key.get_ref();
            let inner = self.table(v, "section")?;
            let keys = self.entries(inner, "a section", &["offset", "size"])?;
            let (Some(o), Some(s)) = (keys.get("offset"), keys.get("size")) else {
                return Err(self.error(
                    &v.span(),
                    format!("[sections.{name}]: needs offset and size"),
                ));
            };
            let offset = self.amount(o, None, "offset", Part::Header, &[], scopes)?;
            let size = self.amount(s, None, "size", Part::Header, &[], scopes)?;
            out.push(Section {
                name: name.to_string(),
                offset,
                size,
            });
        }
        Ok(out)
    }

    pub(crate) fn records(
        &self,
        value: &Value<'_>,
        variants_value: Option<&Value<'_>>,
        layout: Layout,
        scopes: &Scopes<'_>,
    ) -> Result<Records, SpecError> {
        let table = self.table(value, "[records]")?;
        let keys = self.entries(
            table,
            "[records]",
            &[
                "framing",
                "fields",
                "size",
                "size_adjust",
                "count",
                "length_suffix",
                "align",
                "sync",
                "type",
                "checksum",
                "ring",
                "common",
            ],
        )?;
        let framing = match keys.get("framing") {
            None => Framing::Fixed,
            Some(v) => match self.string(v, "framing")?.as_str() {
                "fixed" => Framing::Fixed,
                "length_prefixed" => Framing::LengthPrefixed,
                "variant" => Framing::Variant,
                "sync" => Framing::Sync,
                "blocks" => {
                    return Err(self.error(
                        &v.span(),
                        "framing: blocks are described under [blocks]; framing is how records sit inside each block",
                    ));
                }
                _ => {
                    return Err(self.error(
                        &v.span(),
                        "framing: expected fixed, length_prefixed, variant or sync",
                    ));
                }
            },
        };
        let common_value = match (keys.get("fields"), keys.get("common")) {
            (Some(_), Some(c)) => {
                return Err(self.error(
                    &c.span(),
                    "[records.common]: give the shared fields here or as `fields`, not both",
                ));
            }
            (Some(f), None) => Some(*f),
            (None, Some(c)) => {
                let t = self.table(c, "[records.common]")?;
                let k = self.entries(t, "[records.common]", &["fields"])?;
                k.get("fields").copied()
            }
            (None, None) => None,
        };
        let mut fields = match common_value {
            Some(f) => self.fields(f, Part::Records, layout, scopes, &[])?,
            None => Vec::new(),
        };
        let mut type_field = None;
        if let Some(t) = keys.get("type") {
            let name =
                match t.get_ref() {
                    DeValue::String(s) => s.to_string(),
                    DeValue::Table(inner) => {
                        let k = self.entries(inner, "type", &["field", "type"])?;
                        let Some(f) = k.get("field") else {
                            return Err(self.error(&t.span(), "type: missing `field`"));
                        };
                        let name = self.string(f, "field")?;
                        if let Some(ty) = k.get("type") {
                            if fields
                                .iter()
                                .any(|x| x.name.as_deref() == Some(name.as_str()))
                            {
                                return Err(self.error(
                                    &ty.span(),
                                    format!("type: `{name}` is already a field; name it alone"),
                                ));
                            }
                            let (ty, endian) = parse_type(&self.string(ty, "type")?)
                                .filter(|(t, _)| {
                                    matches!(t, Type::Unsigned(_) | Type::Signed(_) | Type::Str)
                                })
                                .ok_or_else(|| {
                                    self.error(
                                        &ty.span(),
                                        "type: a type field is an integer, or str with a size",
                                    )
                                })?;
                            if ty == Type::Str {
                                return Err(self.error(
                                    &t.span(),
                                    "type: a str type field goes in the fields, with its size",
                                ));
                            }
                            fields.push(Field::plain(&name, ty, endian));
                        }
                        name
                    }
                    _ => return Err(self.error(
                        &t.span(),
                        "type: expected the name of a common field, or { field = ..., type = ... }",
                    )),
                };
            let Some(target) = fields
                .iter()
                .find(|f| f.name.as_deref() == Some(name.as_str()))
            else {
                return Err(self.error(&t.span(), format!("type: no common field named `{name}`")));
            };
            if !(target.ty.is_integer() || target.ty.is_text()) || target.count.is_some() {
                return Err(self.error(
                    &t.span(),
                    format!("type: `{name}` is not an integer or text field"),
                ));
            }
            type_field = Some(name);
        }
        let mut variants = Vec::new();
        if let Some(v) = variants_value {
            let Some(type_name) = &type_field else {
                return Err(self.error(
                    &v.span(),
                    "[[variants]]: needs [records] type, the field that picks one",
                ));
            };
            let target = fields
                .iter()
                .find(|f| f.name.as_deref() == Some(type_name.as_str()))
                .expect("checked above")
                .clone();
            let DeValue::Array(items) = v.get_ref() else {
                return Err(self.error(&v.span(), "variants: expected [[variants]] tables"));
            };
            let mut seen = std::collections::HashSet::new();
            for item in items {
                let t = self.table(item, "variant")?;
                let k = self.entries(
                    t,
                    "a variant",
                    &[
                        "name",
                        "when",
                        "fields",
                        "size",
                        "size_adjust",
                        "description",
                    ],
                )?;
                let Some(n) = k.get("name") else {
                    return Err(self.error(&item.span(), "variant: missing `name`"));
                };
                let name = self.string(n, "name")?;
                if name.trim().is_empty() || !seen.insert(name.clone()) {
                    return Err(self.error(
                        &n.span(),
                        format!("name: `{name}` is empty or a second variant's"),
                    ));
                }
                let Some(w) = k.get("when") else {
                    return Err(self.error(
                        &item.span(),
                        "variant: missing `when`, the type value that picks it",
                    ));
                };
                let when = match w.get_ref() {
                    DeValue::Array(values) if values.is_empty() => {
                        return Err(self.error(
                            &w.span(),
                            "when: an empty list picks no record; give the type value",
                        ));
                    }
                    DeValue::Array(values) => values
                        .iter()
                        .map(|x| self.expected(x, &target, "when"))
                        .collect::<Result<Vec<_>, _>>()?,
                    _ => vec![self.expected(w, &target, "when")?],
                };
                let vfields = match k.get("fields") {
                    Some(f) => self.fields(f, Part::Records, layout, scopes, &fields)?,
                    None => Vec::new(),
                };
                let mut every = fields.clone();
                every.extend(vfields.iter().cloned());
                let size = k
                    .get("size")
                    .map(|s| {
                        self.amount(
                            s,
                            k.get("size_adjust").copied(),
                            "size",
                            Part::Records,
                            &every,
                            scopes,
                        )
                    })
                    .transpose()?;
                if let (Some(Amount::Given(size)), Some(sum)) = (&size, given_width(&every))
                    && *size < sum
                {
                    return Err(self.error(
                        &k["size"].span(),
                        format!("size: the fields take {sum} bytes, more than {size}"),
                    ));
                }
                let description = k
                    .get("description")
                    .map(|d| self.prose(d, "description"))
                    .transpose()?;
                variants.push(Variant {
                    name,
                    when,
                    fields: vfields,
                    size,
                    description,
                });
            }
            // A name two variants share is one column, so it has one type.
            let mut by_name: BTreeMap<String, &Field> = BTreeMap::new();
            for variant in &variants {
                for f in &variant.fields {
                    let Some(n) = &f.name else { continue };
                    if let Some(other) = by_name.get(n)
                        && (other.ty != f.ty
                            || other.meaning != f.meaning
                            || other.count != f.count
                            || other.flatten != f.flatten
                            || other.bits != f.bits
                            || other.group != f.group
                            || other.encoding != f.encoding)
                    {
                        return Err(self.error(
                            &v.span(),
                            format!("variants: `{n}` is a different field in two variants; one column has one type, so name them apart"),
                        ));
                    }
                    by_name.insert(n.clone(), f);
                }
            }
            if fields
                .iter()
                .filter_map(|f| f.name.as_deref())
                .any(|n| n == "type")
                || by_name.contains_key("type")
            {
                return Err(self.error(&v.span(), "variants: the variant's name is shown in a column named `type`, so no field may be named that"));
            }
        } else if type_field.is_some() {
            return Err(self.error(
                &keys["type"].span(),
                "type: picks one of the [[variants]], and there are none",
            ));
        }
        if fields.is_empty() && variants.is_empty() {
            return Err(self.error(&value.span(), "[records]: missing `fields`"));
        }
        if !fields.iter().any(|f| f.name.is_some()) && variants.is_empty() {
            return Err(self.error(&value.span(), "fields: a record needs a named field"));
        }
        let size = keys
            .get("size")
            .map(|s| {
                self.amount(
                    s,
                    keys.get("size_adjust").copied(),
                    "size",
                    Part::Records,
                    &fields,
                    scopes,
                )
            })
            .transpose()?;
        if matches!(size, Some(Amount::Rest)) {
            return Err(self.error(&keys["size"].span(), "size: expected a number or a field"));
        }
        let count = keys
            .get("count")
            .map(|c| self.amount(c, None, "count", Part::Header, &[], scopes))
            .transpose()?;
        let length_suffix = keys
            .get("length_suffix")
            .map(|v| self.boolean(v, "length_suffix"))
            .transpose()?
            .unwrap_or(false);
        let align = match keys.get("align") {
            None => 1,
            Some(v) => {
                let n = self.integer(v, "align")?;
                if !(1..=65536).contains(&n) {
                    return Err(self.error(&v.span(), "align: expected 1 to 65536"));
                }
                n as u64
            }
        };
        let sync = keys
            .get("sync")
            .map(|v| self.hex_bytes(v, "sync"))
            .transpose()?
            .unwrap_or_default();
        let ring = keys
            .get("ring")
            .map(|v| self.amount(v, None, "ring", Part::Header, &[], scopes))
            .transpose()?;
        let record_size = matches!(size, Some(Amount::Record { .. }));
        let at = |key: &str| keys.get(key).map_or(value.span(), |v| v.span());
        match framing {
            Framing::LengthPrefixed if !record_size => {
                return Err(self.error(&at("size"), "framing = \"length_prefixed\": size names the field holding each record's length, such as size = \"len\""));
            }
            Framing::Variant if variants.is_empty() => {
                return Err(self.error(
                    &at("framing"),
                    "framing = \"variant\": needs [[variants]] and a type field",
                ));
            }
            Framing::Sync if sync.is_empty() => {
                return Err(self.error(&at("framing"), "framing = \"sync\": needs sync, the marker each record starts with, such as sync = \"1ACFFC1D\""));
            }
            Framing::Fixed | Framing::Variant if record_size => {
                return Err(self.error(&at("size"), "size: a record whose size is in its own field needs framing = \"length_prefixed\""));
            }
            _ => {}
        }
        if framing != Framing::Sync && !sync.is_empty() {
            return Err(self.error(&at("sync"), "sync: goes with framing = \"sync\""));
        }
        if length_suffix && !record_size {
            return Err(self.error(
                &at("length_suffix"),
                "length_suffix: repeats a length read from the record; give size = \"len\"",
            ));
        }
        let checksum = keys
            .get("checksum")
            .map(|v| {
                let table = self.table(v, "checksum")?;
                let inner = self.entries(table, "checksum", &["algo", "field", "from", "to"])?;
                let algo = self.algo(inner.get("algo").copied(), v)?;
                let named = |key: &str| -> Result<Option<String>, SpecError> {
                    let Some(x) = inner.get(key) else {
                        return Ok(None);
                    };
                    let name = self.string(x, key)?;
                    let known = fields
                        .iter()
                        .chain(variants.iter().flat_map(|v| &v.fields))
                        .any(|f| f.name.as_deref() == Some(name.as_str()));
                    if !known {
                        return Err(
                            self.error(&x.span(), format!("{key}: no record field named `{name}`"))
                        );
                    }
                    Ok(Some(name))
                };
                let Some(field) = named("field")? else {
                    return Err(
                        self.error(&v.span(), "checksum: missing `field`, the field holding it")
                    );
                };
                let holder = fields
                    .iter()
                    .chain(variants.iter().flat_map(|v| &v.fields))
                    .find(|f| f.name.as_deref() == Some(field.as_str()))
                    .expect("checked");
                if !matches!(holder.ty, Type::Unsigned(_) | Type::Signed(_)) {
                    return Err(self.error(&v.span(), "checksum: its field is an integer"));
                }
                Ok(Checksum {
                    algo,
                    field,
                    from: named("from")?,
                    to: named("to")?,
                })
            })
            .transpose()?;
        if checksum.is_some()
            && fields
                .iter()
                .chain(variants.iter().flat_map(|v| &v.fields))
                .any(|f| f.name.as_deref() == Some("checksum_ok"))
        {
            return Err(self.error(&value.span(), "checksum: its result is a column named `checksum_ok`, so no field may be named that"));
        }
        Ok(Records {
            framing,
            fields,
            size,
            count,
            length_suffix,
            align,
            sync,
            type_field,
            variants,
            checksum,
            ring,
        })
    }

    pub(crate) fn blocks(
        &self,
        value: &Value<'_>,
        scopes: &Scopes<'_>,
    ) -> Result<Blocks, SpecError> {
        let table = self.table(value, "[blocks]")?;
        let keys = self.entries(
            table,
            "[blocks]",
            &[
                "header",
                "size",
                "size_adjust",
                "compression",
                "records",
                "uncompressed",
                "index",
            ],
        )?;
        let header = match keys.get("header") {
            Some(h) => self.fields(h, Part::Header, Layout::Rows, scopes, &[])?,
            None => Vec::new(),
        };
        let Some(s) = keys.get("size") else {
            return Err(self.error(&value.span(), "[blocks]: missing `size`, the bytes after each block's header, such as size = \"clen\""));
        };
        let size = self.amount(
            s,
            keys.get("size_adjust").copied(),
            "size",
            Part::Records,
            &header,
            scopes,
        )?;
        if matches!(size, Amount::Rest) {
            return Err(self.error(&s.span(), "size: expected a number or a block header field"));
        }
        let header_field = |key: &str| -> Result<Option<String>, SpecError> {
            let Some(v) = keys.get(key) else {
                return Ok(None);
            };
            let name = self.string(v, key)?;
            if !header.iter().any(|f| {
                f.name.as_deref() == Some(name.as_str())
                    && f.ty.is_integer()
                    && f.meaning == Meaning::Plain
            }) {
                return Err(self.error(
                    &v.span(),
                    format!("{key}: no plain integer block header field named `{name}`"),
                ));
            }
            Ok(Some(name))
        };
        let records = header_field("records")?;
        let uncompressed = header_field("uncompressed")?;
        let codec = match keys.get("compression") {
            None => Codec::Fixed(Compression::None),
            Some(v) => match v.get_ref() {
                DeValue::String(name) => Codec::Fixed(self.compression(v, name)?),
                DeValue::Table(inner) => {
                    let k = self.entries(inner, "compression", &["field", "values"])?;
                    let (Some(f), Some(vals)) = (k.get("field"), k.get("values")) else {
                        return Err(self.error(&v.span(), "compression: expected { field = \"codec\", values = { 0 = \"none\", 1 = \"zstd\" } }"));
                    };
                    let field = self.string(f, "field")?;
                    if !header
                        .iter()
                        .any(|x| x.name.as_deref() == Some(field.as_str()) && x.ty.is_integer())
                    {
                        return Err(self.error(
                            &f.span(),
                            format!("field: no integer block header field named `{field}`"),
                        ));
                    }
                    let mut values = BTreeMap::new();
                    for (code, name) in self.table(vals, "values")? {
                        let text: &str = code.get_ref();
                        let code_value: i64 = text.parse().map_err(|_| {
                            self.error(
                                &code.span(),
                                format!("values: `{text}` is not a whole number"),
                            )
                        })?;
                        let DeValue::String(n) = name.get_ref() else {
                            return Err(self.error(&name.span(), "values: expected a codec's name"));
                        };
                        values.insert(code_value, self.compression(name, n)?);
                    }
                    Codec::ByField { field, values }
                }
                _ => {
                    return Err(self.error(
                        &v.span(),
                        "compression: expected a codec's name or { field, values }",
                    ));
                }
            },
        };
        let lz4_block = match &codec {
            Codec::Fixed(c) => *c == Compression::Lz4Block,
            Codec::ByField { values, .. } => values.values().any(|c| *c == Compression::Lz4Block),
        };
        if lz4_block && uncompressed.is_none() {
            return Err(self.error(&value.span(), "compression: lz4_block needs uncompressed, the header field with each block's decompressed size"));
        }
        let index = keys
            .get("index")
            .map(|v| {
                let t = self.table(v, "index")?;
                let k = self.entries(t, "index", &["at", "count", "fields"])?;
                let (Some(a), Some(c), Some(f)) = (k.get("at"), k.get("count"), k.get("fields"))
                else {
                    return Err(self.error(&v.span(), "index: needs at, count and fields"));
                };
                let at = self.amount(a, None, "at", Part::Header, &[], scopes)?;
                let count = self.amount(c, None, "count", Part::Header, &[], scopes)?;
                let fields = self.fields(f, Part::Header, Layout::Rows, scopes, &[])?;
                if given_width(&fields).is_none() {
                    return Err(self.error(
                        &f.span(),
                        "fields: an index entry's sizes are written in the spec",
                    ));
                }
                if !fields
                    .iter()
                    .any(|x| x.name.as_deref() == Some("offset") && x.ty.is_integer())
                {
                    return Err(self.error(
                        &f.span(),
                        "fields: an index entry needs an integer `offset`, where its block starts",
                    ));
                }
                Ok(BlockIndex { at, count, fields })
            })
            .transpose()?;
        Ok(Blocks {
            header,
            size,
            codec,
            records,
            uncompressed,
            index,
        })
    }

    fn compression(&self, at: &Value<'_>, name: &str) -> Result<Compression, SpecError> {
        Compression::parse(name).ok_or_else(|| {
            let names: Vec<&str> = Compression::NAMES.iter().map(|(n, _)| *n).collect();
            self.error(
                &at.span(),
                format!("compression: expected one of {}", names.join(", ")),
            )
        })
    }

    pub(crate) fn capture(&self, value: &Value<'_>) -> Result<Capture, SpecError> {
        let table = self.table(value, "[capture]")?;
        // pcap or pcapng is told by the file's magic, so there is no key for it.
        let keys = self.entries(table, "[capture]", &["header", "count", "time"])?;
        let header = match keys.get("header") {
            Some(h) => self.fields(h, Part::Records, Layout::Rows, &Scopes::default(), &[])?,
            None => Vec::new(),
        };
        let count = keys
            .get("count")
            .map(|v| {
                let name = self.string(v, "count")?;
                if !header
                    .iter()
                    .any(|f| f.name.as_deref() == Some(name.as_str()) && f.ty.is_integer())
                {
                    return Err(self.error(
                        &v.span(),
                        format!("count: no integer payload header field named `{name}`"),
                    ));
                }
                Ok(name)
            })
            .transpose()?;
        let time = keys
            .get("time")
            .map(|v| {
                let name = self.string(v, "time")?;
                if name.trim().is_empty() || name.contains('.') {
                    return Err(self.error(&v.span(), "time: expected a column name"));
                }
                Ok(name)
            })
            .transpose()?;
        Ok(Capture {
            header,
            count,
            time,
        })
    }

    pub(crate) fn files(&self, value: &Value<'_>) -> Result<Files, SpecError> {
        let table = self.table(value, "[files]")?;
        let keys = self.entries(table, "[files]", &["path"])?;
        let Some(p) = keys.get("path") else {
            return Err(self.error(
                &value.span(),
                "[files]: missing `path`, such as path = \"{date:%Y%m%d}/{venue}/trades.bin\"",
            ));
        };
        let pattern = self.string(p, "path")?;
        let parts =
            path_parts(&pattern).map_err(|e| self.error(&p.span(), format!("path: {e}")))?;
        Ok(Files { pattern, parts })
    }

    /// What the columns layout allows: fixed values, one file each or all at offsets in
    /// one file.
    pub(crate) fn check_columns(
        &self,
        value: &Value<'_>,
        records: &Records,
    ) -> Result<(), SpecError> {
        if records.size.is_some() {
            return Err(self.error(
                &value.span(),
                "size: each column holds one field, so layout = \"columns\" takes no record size",
            ));
        }
        if records.framing != Framing::Fixed || records.checksum.is_some() || records.ring.is_some()
        {
            return Err(self.error(
                &value.span(),
                "layout = \"columns\": values are fixed, with no framing, checksum or ring",
            ));
        }
        let at = records.fields.iter().filter(|f| f.at.is_some()).count();
        if at != 0 && at != records.fields.len() {
            return Err(self.error(
                &value.span(),
                "offset: in one file, every column gives where it starts",
            ));
        }
        for field in &records.fields {
            let said = if field.ty == Type::Pad {
                Some("pad: layout = \"columns\" has no bytes between fields to skip")
            } else if field.flatten {
                Some("flatten: a column file holds one column; leave the values an Array")
            } else if matches!(field.ty, Type::Strz | Type::VarU | Type::VarS | Type::Group)
                || !field.bits.is_empty()
                || field.string_at.is_some()
            {
                Some("layout = \"columns\": a column's values are fixed-width")
            } else if field.size.as_ref().is_some_and(|s| !s.is_fixed())
                || field.count.as_ref().is_some_and(|s| !s.is_fixed())
            {
                Some("layout = \"columns\": a column's values are all one size")
            } else {
                None
            };
            if let Some(said) = said {
                return Err(self.error(&value.span(), said));
            }
        }
        Ok(())
    }

    /// An integer's bit fields.
    fn bits(&self, value: &Value<'_>, ty: Type, counted: bool) -> Result<Vec<BitField>, SpecError> {
        let total = match ty {
            Type::Unsigned(n) | Type::Signed(n) => u32::from(n) * 8,
            Type::VarU | Type::VarS => 64,
            _ => return Err(self.error(&value.span(), "bits: are for an integer field")),
        };
        if counted {
            return Err(self.error(&value.span(), "bits: are for one integer a record"));
        }
        let DeValue::Array(items) = value.get_ref() else {
            return Err(self.error(
                &value.span(),
                "bits: expected a list, such as [{ name = \"valid\", bit = 0 }]",
            ));
        };
        let mut out = Vec::new();
        for item in items {
            let table = self.table(item, "bit")?;
            let keys = self.entries(table, "a bit field", &["name", "bit", "width", "enum"])?;
            let Some(n) = keys.get("name") else {
                return Err(self.error(&item.span(), "bit: missing `name`"));
            };
            let name = self.string(n, "name")?;
            if name.trim().is_empty() || name.contains('.') {
                return Err(self.error(&n.span(), "name: must not be empty or contain `.`"));
            }
            let Some(b) = keys.get("bit") else {
                return Err(self.error(
                    &item.span(),
                    "bit: missing `bit`, the lowest bit, 0 the least significant",
                ));
            };
            let bit = self.integer(b, "bit")?;
            let width = keys
                .get("width")
                .map(|w| self.integer(w, "width"))
                .transpose()?
                .unwrap_or(1);
            if bit < 0 || width < 1 || bit + width > i64::from(total) {
                return Err(self.error(
                    &item.span(),
                    format!(
                        "bit: bits {bit} to {} are outside the field's {total}",
                        bit + width - 1
                    ),
                ));
            }
            let labels = keys
                .get("enum")
                .map(|e| {
                    let table = self.table(e, "enum")?;
                    let mut labels = BTreeMap::new();
                    for (code, label) in table {
                        let text: &str = code.get_ref();
                        let code_value: i64 = text.parse().map_err(|_| {
                            self.error(
                                &code.span(),
                                format!("enum: `{text}` is not a whole number"),
                            )
                        })?;
                        labels.insert(code_value, self.string(label, "enum label")?);
                    }
                    Ok::<_, SpecError>(Arc::new(labels))
                })
                .transpose()?;
            out.push(BitField {
                name,
                bit: bit as u32,
                width: width as u32,
                labels,
            });
        }
        Ok(out)
    }

    /// What a field means: at most one of a time, a scale, a linear conversion and an
    /// enum.
    fn meaning(&self, keys: &BTreeMap<&str, &Value<'_>>, ty: Type) -> Result<Meaning, SpecError> {
        let groups: [(&[&str], &str); 4] = [
            (&["time", "of_day", "date", "epoch"], "time"),
            (&["scale"], "scale"),
            (&["factor", "offset"], "factor"),
            (&["enum"], "enum"),
        ];
        let used: Vec<(&str, Range<usize>)> = groups
            .iter()
            .filter_map(|(group, said)| {
                group
                    .iter()
                    .find_map(|k| keys.get(k))
                    .map(|v| (*said, v.span()))
            })
            .collect();
        if used.len() > 1 {
            return Err(self.error(
                &used[1].1,
                format!(
                    "a field takes one of time (or date), scale, factor and enum, not {} and {}",
                    used[0].0, used[1].0
                ),
            ));
        }
        let Some((kind, span)) = used.into_iter().next() else {
            return Ok(Meaning::Plain);
        };
        let integers_only = |what: &str| {
            if ty.is_integer() {
                Ok(())
            } else {
                Err(self.error(
                    &span,
                    format!("{what} is for integer types, not {}", type_name(ty)),
                ))
            }
        };
        match kind {
            "scale" => {
                integers_only("scale")?;
                let v = keys["scale"];
                let scale = self.integer(v, "scale")?;
                if !(0..=38).contains(&scale) {
                    return Err(self.error(&v.span(), "scale: expected 0 to 38"));
                }
                Ok(Meaning::Scale(scale as u32))
            }
            "factor" => {
                if !ty.is_number() {
                    return Err(self.error(
                        &span,
                        format!(
                            "`factor` and `offset` are for numbers, not {}",
                            type_name(ty)
                        ),
                    ));
                }
                let factor = keys
                    .get("factor")
                    .map(|v| self.number(v, "factor"))
                    .transpose()?
                    .unwrap_or(1.0);
                let offset = keys
                    .get("offset")
                    .map(|v| self.number(v, "offset"))
                    .transpose()?
                    .unwrap_or(0.0);
                Ok(Meaning::Linear { factor, offset })
            }
            "enum" => {
                integers_only("enum")?;
                let v = keys["enum"];
                let table = self.table(v, "enum")?;
                let mut labels = BTreeMap::new();
                for (code, label) in table {
                    let text: &str = code.get_ref();
                    let code_value: i64 = text.parse().map_err(|_| {
                        self.error(
                            &code.span(),
                            format!("enum: `{text}` is not a whole number"),
                        )
                    })?;
                    labels.insert(code_value, self.string(label, "enum label")?);
                }
                Ok(Meaning::Enum(Arc::new(labels)))
            }
            _ => self.time_meaning(keys, ty, &span),
        }
    }

    fn time_meaning(
        &self,
        keys: &BTreeMap<&str, &Value<'_>>,
        ty: Type,
        span: &Range<usize>,
    ) -> Result<Meaning, SpecError> {
        let unit = keys
            .get("time")
            .map(|v| match self.string(v, "time")?.as_str() {
                "days" => Ok(TimeUnitSpec::Days),
                "s" => Ok(TimeUnitSpec::Seconds),
                "ms" => Ok(TimeUnitSpec::Millis),
                "us" => Ok(TimeUnitSpec::Micros),
                "ns" => Ok(TimeUnitSpec::Nanos),
                _ => Err(self.error(&v.span(), "time: expected days, s, ms, us or ns")),
            })
            .transpose()?;
        let of_day = keys
            .get("of_day")
            .map(|v| self.boolean(v, "of_day"))
            .transpose()?
            .unwrap_or(false);
        let date = keys
            .get("date")
            .map(|v| Ok::<_, SpecError>((self.string(v, "date")?, v.span())))
            .transpose()?;
        if let Some(v) = keys.get("epoch")
            && (unit.is_none() || of_day)
        {
            return Err(self.error(&v.span(), "`epoch` goes with time, and not with of_day"));
        }
        match (unit, of_day, date) {
            (None, false, Some((date, at))) => {
                if date != "yyyymmdd" {
                    return Err(self.error(
                        &at,
                        "date: expected \"yyyymmdd\" (or, with of_day, a header field)",
                    ));
                }
                if !ty.is_integer() {
                    return Err(self.error(
                        &at,
                        format!("`date` is for integer types, not {}", type_name(ty)),
                    ));
                }
                Ok(Meaning::Yyyymmdd)
            }
            (None, _, _) => Err(self.error(
                span,
                "of_day goes with time = \"s\", \"ms\", \"us\" or \"ns\"",
            )),
            (Some(unit), true, date) => {
                if !ty.is_integer() {
                    return Err(self.error(
                        span,
                        format!("of_day is for integer types, not {}", type_name(ty)),
                    ));
                }
                if unit == TimeUnitSpec::Days {
                    return Err(self.error(span, "of_day counts s, ms, us or ns since midnight"));
                }
                let date = date
                    .map(|(date, at)| {
                        let field = date.strip_prefix("header.").ok_or_else(|| {
                            self.error(&at, "date: with of_day, expected a header field such as header.trade_date")
                        })?;
                        Ok::<_, SpecError>(field.to_string())
                    })
                    .transpose()?;
                Ok(Meaning::TimeOfDay { unit, date })
            }
            (Some(unit), false, date) => {
                if let Some((_, at)) = date {
                    return Err(self.error(&at, "date: goes with of_day, or alone as \"yyyymmdd\""));
                }
                if !ty.is_number() {
                    return Err(self.error(
                        span,
                        format!("`time` is for numbers, not {}", type_name(ty)),
                    ));
                }
                let epoch_ns = keys
                    .get("epoch")
                    .map(|e| self.epoch(e))
                    .transpose()?
                    .unwrap_or(0);
                Ok(Meaning::Time { unit, epoch_ns })
            }
        }
    }

    /// An epoch: a TOML date or date-time, or one written as a string.
    fn epoch(&self, value: &Value<'_>) -> Result<i64, SpecError> {
        let text = match value.get_ref() {
            DeValue::Datetime(dt) => dt.to_string(),
            DeValue::String(s) => s.to_string(),
            _ => {
                return Err(self.error(&value.span(), "epoch: expected a date such as 2000-01-01"));
            }
        };
        parse_epoch(&text).ok_or_else(|| {
            self.error(
                &value.span(),
                "epoch: expected a date such as 2000-01-01 or a date-time such as 2000-01-01T00:00:00Z",
            )
        })
    }
}
