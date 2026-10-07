//! ELF files as their symbol table: what uses flash and RAM.
//!
//! An ELF file (a firmware image, an executable, a library, an object file) opens as
//! one row per symbol: its name (Rust names demangled), address, size, kind, binding,
//! section, and the region its section sits in: `flash` for what is loaded and not
//! written (code, constants), `ram` for what is written (`.data`, `.bss`). Sorted by
//! size and grouped by section, it says what fills each. Its sections are a second
//! table, `--table sections`.
//!
//! Only the headers and the symbol and string tables are read, through a map of the
//! file, by the `object` crate; the table is small beside the file.

use color_eyre::Result;

use std::sync::Arc;

use crate::columns::{Builder, Cell, Kind};
use object::{Object, ObjectSection, ObjectSymbol, SectionFlags, SymbolFlags, SymbolSection};
use polars::prelude::*;

use crate::model_files::MetaValue;
use crate::sqlite::Table;
use crate::text_formats::Detail;

/// What datui does with an ELF file: see [`crate::readers`].
pub(crate) const READER: crate::readers::Reader = crate::readers::Reader {
    scan,
    // Never in a listing, which would list every executable.
    signatures: &[crate::readers::Signature {
        says: |head, _| looks_like(head),
        kind: crate::readers::Kind::Magic,
        trusted: crate::readers::Trusted {
            listing: false,
            tables: true,
            ..crate::readers::EVERYWHERE
        },
    }],
    tables: Some(|_| Ok(tables())),
    ..crate::readers::BASE
};

/// The first four bytes of every ELF file.
pub const MAGIC: &[u8; 4] = b"\x7fELF";

/// The table an ELF file opens on, and the other one.
pub const SYMBOLS: &str = "symbols";
pub const SECTIONS: &str = "sections";

// `sh_flags` bits.
const SHF_WRITE: u64 = 0x1;
const SHF_ALLOC: u64 = 0x2;

/// Whether `head`, the first bytes of a file, begins an ELF file.
pub fn looks_like(head: &[u8]) -> bool {
    head.starts_with(MAGIC)
}

/// The tables of an ELF file, for the home screen and `--table`.
pub fn tables() -> Vec<Table> {
    let symbols = ["name", "addr", "size", "kind", "bind", "section", "region"];
    let sections = ["name", "addr", "size", "flags", "kind", "region"];
    vec![
        Table::plain(SYMBOLS, "table", symbols),
        Table::plain(SECTIONS, "table", sections),
    ]
}

/// The region a section with `sh_flags` sits in: what is written is RAM, what is only
/// loaded is flash; a section not loaded is in neither.
fn region(flags: u64) -> Option<&'static str> {
    match (flags & SHF_ALLOC != 0, flags & SHF_WRITE != 0) {
        (false, _) => None,
        (true, true) => Some("ram"),
        (true, false) => Some("flash"),
    }
}

/// `sh_flags` as `readelf` writes them: `WAX` for a writable, allocated, executable
/// section.
fn flags_text(flags: u64) -> String {
    const LETTERS: [(u64, char); 11] = [
        (0x1, 'W'),
        (0x2, 'A'),
        (0x4, 'X'),
        (0x10, 'M'),
        (0x20, 'S'),
        (0x40, 'I'),
        (0x80, 'L'),
        (0x100, 'O'),
        (0x200, 'G'),
        (0x400, 'T'),
        (0x800, 'C'),
    ];
    LETTERS
        .iter()
        .filter(|(bit, _)| flags & bit != 0)
        .map(|(_, c)| *c)
        .collect()
}

fn sh_flags(flags: SectionFlags) -> u64 {
    match flags {
        SectionFlags::Elf { sh_flags } => sh_flags,
        _ => 0,
    }
}

/// A symbol's type, from the low half of `st_info`.
fn symbol_kind(st_info: u8) -> &'static str {
    match st_info & 0xf {
        0 => "notype",
        1 => "object",
        2 => "func",
        3 => "section",
        4 => "file",
        5 => "common",
        6 => "tls",
        10 => "ifunc",
        _ => "other",
    }
}

/// A symbol's binding, from the high half of `st_info`.
fn symbol_bind(st_info: u8) -> &'static str {
    match st_info >> 4 {
        0 => "local",
        1 => "global",
        2 => "weak",
        10 => "unique",
        _ => "other",
    }
}

/// A Rust symbol's name demangled, without its hash; any other name as it is. C++
/// names stay mangled: no C++ demangler is in the tree.
pub fn demangle(name: &str) -> String {
    match rustc_demangle::try_demangle(name) {
        Ok(demangled) => format!("{demangled:#}"),
        Err(_) => name.to_string(),
    }
}

/// What an ELF file's tables hold.
pub struct Elf {
    pub symbols: DataFrame,
    pub sections: DataFrame,
    pub detail: Detail,
    /// Symbols past `limits.elf_symbols`, left out.
    pub left_out: usize,
}

/// Read the symbol and section tables of the ELF file in `data`.
pub fn read(data: &[u8]) -> std::result::Result<Elf, String> {
    if !looks_like(data) {
        return Err("not an ELF file: no \\x7fELF at the start".into());
    }
    let file = object::File::parse(data).map_err(|e| format!("not a readable ELF file: {e}"))?;

    // Each section's name, shared by its symbols rather than copied to each.
    let mut section_names: Vec<Option<Arc<str>>> = Vec::new();
    let mut section_flags: Vec<u64> = Vec::new();
    let mut sections = Builder::new(&[
        ("name", Kind::Str),
        ("addr", Kind::U64),
        ("size", Kind::U64),
        ("flags", Kind::Str),
        ("kind", Kind::Str),
        ("region", Kind::Label),
    ]);
    let (mut flash, mut ram) = (0u64, 0u64);
    for section in file.sections() {
        let index = section.index().0;
        if section_names.len() <= index {
            section_names.resize(index + 1, Some(Arc::from("")));
            section_flags.resize(index + 1, 0);
        }
        let name = section.name().unwrap_or_default();
        let flags = sh_flags(section.flags());
        let place = region(flags);
        match place {
            Some("ram") => ram = ram.saturating_add(section.size()),
            Some(_) => flash = flash.saturating_add(section.size()),
            None => {}
        }
        section_names[index] = Some(name.into());
        section_flags[index] = flags;
        sections.push([
            Cell::Str(Some(name.to_string())),
            Cell::U64(Some(section.address())),
            Cell::U64(Some(section.size())),
            Cell::Str(Some(flags_text(flags))),
            Cell::Str(Some(format!("{:?}", section.kind()).to_ascii_lowercase())),
            Cell::Label(place),
        ]);
    }

    // The static symbol table, or the dynamic one of a stripped library.
    let mut symbols: Vec<_> = file.symbols().collect();
    if symbols.is_empty() {
        symbols = file.dynamic_symbols().collect();
    }
    let total = symbols.len();
    symbols.truncate(crate::limits::get().elf_symbols);
    let rows = symbols.len();
    let mut table = Builder::new(&[
        ("name", Kind::Str),
        ("addr", Kind::U64),
        ("size", Kind::U64),
        ("kind", Kind::Label),
        ("bind", Kind::Label),
        ("section", Kind::Shared),
        ("region", Kind::Label),
    ]);
    let pseudo = |name: &str| Some(Arc::<str>::from(name));
    for symbol in &symbols {
        let st_info = match symbol.flags() {
            SymbolFlags::Elf { st_info, .. } => st_info,
            _ => 0,
        };
        let (in_section, in_region) = match symbol.section() {
            SymbolSection::Section(index) => (
                section_names.get(index.0).cloned().flatten(),
                section_flags.get(index.0).copied().and_then(region),
            ),
            SymbolSection::Undefined => (pseudo("UND"), None),
            SymbolSection::Absolute => (pseudo("ABS"), None),
            SymbolSection::Common => (pseudo("COMMON"), None),
            _ => (None, None),
        };
        table.push([
            Cell::Str(Some(demangle(symbol.name().unwrap_or_default()))),
            Cell::U64(Some(symbol.address())),
            Cell::U64(Some(symbol.size())),
            Cell::Label(Some(symbol_kind(st_info))),
            Cell::Label(Some(symbol_bind(st_info))),
            Cell::Shared(in_section),
            Cell::Label(in_region),
        ]);
    }
    let symbols = table.take().map_err(|e| e.to_string())?;
    let sections = sections.take().map_err(|e| e.to_string())?;

    let group = crate::numfmt::group_chrome;
    let mut lines = vec![
        format!(
            "Class: {}, {} endian",
            if file.is_64() { "64-bit" } else { "32-bit" },
            if file.is_little_endian() {
                "little"
            } else {
                "big"
            }
        ),
        format!("Machine: {:?}", file.architecture()),
        format!("Type: {:?}", file.kind()),
        format!("Entry: 0x{:x}", file.entry()),
        format!(
            "Flash: {} bytes in loaded, unwritten sections",
            group(usize::try_from(flash).unwrap_or(usize::MAX))
        ),
        format!(
            "RAM: {} bytes in written sections",
            group(usize::try_from(ram).unwrap_or(usize::MAX))
        ),
        format!("Symbols: {}", group(total)),
    ];
    if total > rows {
        lines.push(format!("The first {} symbols are read.", group(rows)));
    }
    let list = crate::text_formats::capped_list(
        (0..sections.height()).map(|i| {
            let get = |c: &str| sections.column(c).ok().and_then(|c| c.get(i).ok());
            let text = format!(
                "0x{:x}  {} bytes  {}",
                get("addr")
                    .and_then(|v| v.extract::<u64>())
                    .unwrap_or_default(),
                get("size")
                    .and_then(|v| v.extract::<u64>())
                    .unwrap_or_default(),
                get("flags")
                    .and_then(|v| v.get_str().map(str::to_string))
                    .unwrap_or_default()
            );
            let name = get("name")
                .and_then(|v| v.get_str().map(str::to_string))
                .unwrap_or_default();
            (name, MetaValue::Text(text))
        }),
        sections.height(),
    );
    Ok(Elf {
        symbols,
        sections,
        detail: Detail {
            tab: crate::text_formats::tab(crate::FileFormat::Elf),
            lines,
            list_title: "Sections",
            list,
            first: false,
            ..Default::default()
        },
        left_out: total - rows,
    })
}

/// The scan of an ELF file: the table `--table` names, its symbols by default.
fn scan(input: crate::readers::ScanIn<'_>) -> Result<crate::scan::Scan> {
    let path = input.path();
    let wanted = input.options.table.as_deref();
    let tables = tables();
    let picked = match wanted {
        None => SYMBOLS.to_string(),
        Some(_) => match crate::members::pick(tables.clone(), wanted, path, "")? {
            crate::sqlite::Pick::One(table) => table.name,
            crate::sqlite::Pick::Several(_) => SYMBOLS.to_string(),
        },
    };
    let bytes = crate::fixed_records::Bytes::map(path)?;
    let elf = read(bytes.as_slice()).map_err(|e| color_eyre::eyre::eyre!(e))?;
    let mut notes = Vec::new();
    if elf.left_out > 0 {
        notes.push(crate::limits::left_out(
            &format!("{} symbols", crate::numfmt::group_chrome(elf.left_out)),
            crate::limits::get().elf_symbols,
            "elf_symbols",
        ));
    }
    let df = if picked == SECTIONS {
        elf.sections
    } else {
        elf.symbols
    };
    let opened =
        crate::members::Opened::for_table(elf.detail, &tables, &picked, notes, "the symbol table");
    Ok(opened.scan(input, df.lazy()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A file that is not ELF names itself, in the one shape.
    #[test]
    fn errors_name_the_file() {
        crate::readers::bad_input::each_names_its_file(
            crate::FileFormat::Elf,
            &[
                ("text.elf", b"hello there", "Not an ELF file"),
                (
                    "cut.elf",
                    b"\x7fELF\x02\x01\x01\0",
                    "Not a readable ELF file",
                ),
            ],
        );
    }

    #[test]
    fn symbols_with_their_section_and_region() {
        let elf = read(&crate::tests::fixtures::elf()).unwrap();
        let s = &elf.symbols;
        let text = |c: &str| -> Vec<Option<String>> {
            s.column(c)
                .unwrap()
                .str()
                .unwrap()
                .iter()
                .map(|v| v.map(str::to_string))
                .collect()
        };
        let names = text("name");
        let at = |n: &str| names.iter().position(|v| v.as_deref() == Some(n)).unwrap();
        assert!(names.contains(&Some("core::fmt::write".to_string())));
        let region = text("region");
        let section = text("section");
        let kind = text("kind");
        let bind = text("bind");
        assert_eq!(region[at("main")].as_deref(), Some("flash"));
        assert_eq!(region[at("TABLE")].as_deref(), Some("flash"));
        assert_eq!(region[at("counter")].as_deref(), Some("ram"));
        assert_eq!(section[at("buffer")].as_deref(), Some(".bss"));
        assert_eq!(region[at("buffer")].as_deref(), Some("ram"));
        assert_eq!(kind[at("main")].as_deref(), Some("func"));
        assert_eq!(bind[at("buffer")].as_deref(), Some("local"));
        assert_eq!(bind[at("weak_hook")].as_deref(), Some("weak"));
        let sizes = s.column("size").unwrap().u64().unwrap();
        assert_eq!(sizes.get(at("buffer")), Some(1024));
        let flags: Vec<_> = elf
            .sections
            .column("flags")
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .map(|v| v.unwrap_or_default().to_string())
            .collect();
        assert!(flags.contains(&"AX".to_string()) && flags.contains(&"WA".to_string()));
        assert!(
            elf.detail
                .lines
                .iter()
                .any(|l| l.starts_with("RAM: 1,040 bytes")),
            "{:?}",
            elf.detail.lines
        );
    }

    #[test]
    fn garbage_is_refused() {
        assert!(read(b"\x7fELF\x02\x01\x01").is_err());
        assert!(read(b"MZ").is_err());
        let mut cut = crate::tests::fixtures::elf();
        cut.truncate(cut.len() - 100);
        assert!(read(&cut).is_err());
    }
}
