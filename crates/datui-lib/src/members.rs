//! Files that hold several tables: a SQLite database's tables and views, a NumPy
//! archive's arrays.
//!
//! Each table has a path inside its file (`shop.db/orders`, `run.npz/weights`) that
//! nothing on disk has. The home screen lists a file of several as a place whose rows
//! are its tables, recents record a table by that path, and `--table` names one. A file
//! of one table opens it.

use std::path::{Path, PathBuf};

use crate::FileFormat;
pub use crate::sqlite::{Pick, Table};

/// The format of a file that can hold several tables, by its first bytes (and, for a
/// NumPy archive, its name: a zip file is many things).
pub fn holder(path: &Path) -> Option<FileFormat> {
    crate::readers::sniff_file(path, crate::readers::Asked::Tables)
}

/// The path of the table `name` inside `file`, as the home screen lists it and recents
/// record it: `shop.db/orders`. The name is appended as it is, so a name that looks like
/// a path (`/etc`, `a/../b`) stays inside the file and reads back whole.
pub fn place(file: &Path, name: &str) -> PathBuf {
    let mut place = file.as_os_str().to_owned();
    place.push("/");
    place.push(name);
    place.into()
}

/// The file and the table a path inside a file of tables names, the inverse of
/// [`place`]: `shop.db/orders` is the table `orders` of `shop.db`, and `shop.db/a/b` the
/// table `a/b`. `None` for a path that is there, or that is inside no such file.
pub fn split(path: &Path) -> Option<(PathBuf, String)> {
    split_inside(path, |file| holder(file).is_some())
}

/// The file and the variant a path inside a file of a format spec's variants names:
/// `day.itch/add` is the variant `add` of `day.itch`, as the spec writes it. `None` for
/// a path that is there, inside no such file, or naming no variant of it.
pub fn split_variant(path: &Path, formats: &crate::formats::Registry) -> Option<(PathBuf, String)> {
    if formats.is_empty() {
        return None;
    }
    let (file, name) = split_inside(path, |file| formats.variants_of(file).is_some())?;
    let (_, tables) = variants(&file, formats)?;
    let found = tables.iter().find(|t| t.name == name).or_else(|| {
        let mut alike = tables.iter().filter(|t| t.name.eq_ignore_ascii_case(&name));
        match (alike.next(), alike.next()) {
            (Some(one), None) => Some(one),
            _ => None,
        }
    })?;
    Some((file, found.name.clone()))
}

/// The variants of the records of `file`, as the format spec whose glob names it reads
/// them, each a table: the spec's name, and its variants with their columns. `None`
/// unless the spec reads several. Its name is all that is read.
pub fn variants(file: &Path, formats: &crate::formats::Registry) -> Option<(String, Vec<Table>)> {
    let spec = formats.variants_of(file)?;
    let named = |fields: &[crate::formats::Field]| {
        fields
            .iter()
            .filter_map(|f| f.name.clone())
            .map(|name| (name, String::new()))
            .collect::<Vec<_>>()
    };
    let common = named(&spec.records.fields);
    let tables = spec
        .records
        .variants
        .iter()
        .map(|v| Table {
            name: v.name.clone(),
            kind: "variant".to_string(),
            internal: false,
            columns: common.iter().cloned().chain(named(&v.fields)).collect(),
        })
        .collect();
    Some((spec.name.clone(), tables))
}

/// [`split`], for files `holds` says are files of tables.
fn split_inside(path: &Path, holds: impl Fn(&Path) -> bool) -> Option<(PathBuf, String)> {
    // A path ending in `..` is a table so named: Windows resolves it before it looks,
    // and `app.db/..` is the directory the database is in.
    if path.file_name().is_some() && path.exists() {
        return None;
    }
    let file = path
        .ancestors()
        .skip(1)
        .take_while(|p| !p.as_os_str().is_empty())
        // Windows resolves `..` before it looks, so `app.db/a/..` would be the
        // database itself and the table `b` rather than `a/../b`.
        .find(|p| p.file_name().is_some() && p.is_file())?;
    if !holds(file) {
        return None;
    }
    // What follows the file's name, less the one separator after it, as written: a
    // parent is a prefix of the path's own text.
    let rest = path.to_str()?.strip_prefix(file.to_str()?)?;
    let mut chars = rest.chars();
    chars.next().filter(|c| std::path::is_separator(*c))?;
    let name = chars.as_str().trim_end_matches(std::path::is_separator);
    (!name.is_empty()).then(|| (file.to_path_buf(), name.to_string()))
}

/// The tables of `file`, a file of `format`, as the home screen lists them. Cheap: a
/// database's schema, an archive's directory and its arrays' headers.
pub fn tables(file: &Path, format: FileFormat) -> color_eyre::Result<Vec<Table>> {
    match crate::readers::of(format).tables {
        Some(tables) => tables(file),
        None => Err(color_eyre::eyre::eyre!(
            "A {} file holds one table.",
            format.name()
        )),
    }
}

/// What a file of tables calls one of them, singular and plural.
pub fn noun(format: FileFormat) -> (&'static str, &'static str) {
    format
        .descriptor()
        .tables
        .as_ref()
        .map_or(("table", "tables"), |t| t.noun)
}

/// The table `wanted` names among `tables`, or the file's one table of its own when
/// nothing is named. `display` names the file in errors; `format` says what its
/// tables are called, and `empty` what to say of a file of none.
pub fn pick(
    tables: Vec<Table>,
    wanted: Option<&str>,
    display: &Path,
    format: FileFormat,
    empty: &str,
) -> color_eyre::Result<Pick> {
    use color_eyre::eyre::eyre;
    let (one, many) = noun(format);
    let own: Vec<&Table> = tables.iter().filter(|t| !t.internal).collect();
    let names = || {
        const SHOWN: usize = 20;
        let mut names: Vec<&str> = own.iter().take(SHOWN).map(|t| t.name.as_str()).collect();
        let more = own.len().saturating_sub(SHOWN);
        let more = format!("and {more} more");
        if own.len() > SHOWN {
            names.push(&more);
        }
        names.join(", ")
    };
    if let Some(wanted) = wanted {
        // Exactly as written first, then without regard to case when that is one.
        let found = tables.iter().find(|t| t.name == wanted).or_else(|| {
            let mut alike = tables
                .iter()
                .filter(|t| t.name.eq_ignore_ascii_case(wanted));
            match (alike.next(), alike.next()) {
                (Some(one), None) => Some(one),
                _ => None,
            }
        });
        return match found {
            Some(table) => Ok(Pick::One(table.clone())),
            None if own.is_empty() => Err(eyre!(
                "No {one} {wanted:?} in {}, which holds no {many}.",
                display.display()
            )),
            None => Err(eyre!(
                "No {one} {wanted:?} in {}. Its {many}: {}.",
                display.display(),
                names()
            )),
        };
    }
    match own.as_slice() {
        [] => Err(eyre!("{} holds no {many}.{empty}", display.display())),
        [one] => Ok(Pick::One((*one).clone())),
        _ => Ok(Pick::Several(tables)),
    }
}

/// What a reader that decodes its table from the file found, carried from the scan to
/// the dataset: a window read straight from the file, its row count, its Info panel
/// tab, its other tables, notes and units.
#[derive(Default)]
pub struct Opened {
    /// Rows read straight from the source, and how many it holds, so neither a page
    /// deep in the table nor the count builds the frame's row index.
    pub window: Option<(std::sync::Arc<dyn crate::pushdown::Windowed>, usize)>,
    pub detail: Option<std::sync::Arc<crate::text_formats::Detail>>,
    pub other_tables: Vec<String>,
    pub notes: Vec<crate::notes::Note>,
    /// Each column's unit, where the file says one.
    pub units: Vec<(String, String)>,
}

impl std::fmt::Debug for Opened {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Opened")
            .field("rows", &self.window.as_ref().map(|(_, rows)| rows))
            .field("other_tables", &self.other_tables)
            .finish_non_exhaustive()
    }
}

/// The other tables of a file, each as `--table` names it, for the Info panel.
pub fn others(tables: &[Table], opened: &str) -> Vec<String> {
    tables
        .iter()
        .filter(|t| !t.internal && t.name != opened)
        .map(|t| t.name.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_place_inside_a_file_reads_back_whole() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("app.db");
        let mut head = crate::sqlite::MAGIC.to_vec();
        head.resize(100, 0);
        std::fs::write(&db, head).unwrap();
        for name in ["users", "a/b", "/etc", "a/../b"] {
            let place = place(&db, name);
            assert_eq!(
                split(&place),
                Some((db.clone(), name.to_string())),
                "{name}"
            );
        }
        assert_eq!(split(&db), None);
        let text = dir.path().join("notes.txt");
        std::fs::write(&text, "hello").unwrap();
        assert_eq!(split(&text.join("users")), None);
    }

    #[test]
    fn a_place_inside_a_file_of_variants_names_one() {
        let spec = crate::formats::Spec::parse(
            r#"name = "acme.v"
match = { glob = ["*.v"] }
[records]
framing = "length_prefixed"
size = "len"
type = "kind"
fields = [{ name = "len", type = "u1" }, { name = "kind", type = "u1" }]
[[variants]]
name = "Add"
when = 1
fields = [{ name = "a", type = "u1" }]
[[variants]]
name = "exec"
when = 2
fields = [{ name = "b", type = "u1" }]"#,
            None,
        )
        .unwrap();
        let formats = crate::formats::Registry::of(vec![spec]);
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("day.v");
        std::fs::write(&file, [3u8, 1, 7]).unwrap();
        assert_eq!(
            split_variant(&place(&file, "add"), &formats),
            Some((file.clone(), "Add".to_string())),
            "as the spec writes it"
        );
        assert_eq!(split_variant(&place(&file, "nope"), &formats), None);
        assert_eq!(split_variant(&file, &formats), None);
        let other = dir.path().join("day.w");
        std::fs::write(&other, [0u8]).unwrap();
        assert_eq!(split_variant(&place(&other, "exec"), &formats), None);
        let (spec, tables) = variants(&file, &formats).unwrap();
        assert_eq!(spec, "acme.v");
        assert_eq!(tables[1].columns.len(), 3);
    }

    #[test]
    fn pick_names_what_is_there() {
        let t = |name: &str| Table {
            name: name.into(),
            kind: "array".into(),
            internal: false,
            columns: Vec::new(),
        };
        let display = Path::new("run.npz");
        let several = vec![t("x"), t("y")];
        assert!(matches!(
            pick(several.clone(), None, display, FileFormat::Numpy, "").unwrap(),
            Pick::Several(_)
        ));
        assert_eq!(
            pick(several.clone(), Some("Y"), display, FileFormat::Numpy, "").unwrap(),
            Pick::One(t("y"))
        );
        let missing = pick(several, Some("z"), display, FileFormat::Numpy, "")
            .unwrap_err()
            .to_string();
        assert_eq!(missing, "No array \"z\" in run.npz. Its arrays: x, y.");
        let empty = pick(Vec::new(), None, display, FileFormat::Numpy, "")
            .unwrap_err()
            .to_string();
        assert_eq!(empty, "run.npz holds no arrays.");
    }
}
