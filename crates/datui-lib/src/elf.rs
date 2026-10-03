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

use std::path::Path;
use std::sync::Arc;

use color_eyre::Result;
use color_eyre::eyre::eyre;
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

/// Symbols read; a file of more says how many were left out.
const MAX_SYMBOLS: usize = 10_000_000;

// `sh_flags` bits.
const SHF_WRITE: u64 = 0x1;
const SHF_ALLOC: u64 = 0x2;

/// Whether `head`, the first bytes of a file, begins an ELF file.
pub fn looks_like(head: &[u8]) -> bool {
    head.starts_with(MAGIC)
}

/// The tables of an ELF file, for the home screen and `--table`.
pub fn tables() -> Vec<Table> {
    let table = |name: &str, columns: &[&str]| Table {
        name: name.to_string(),
        kind: "table".to_string(),
        internal: false,
        columns: columns
            .iter()
            .map(|c| (c.to_string(), String::new()))
            .collect(),
    };
    vec![
        table(
            SYMBOLS,
            &["name", "addr", "size", "kind", "bind", "section", "region"],
        ),
        table(
            SECTIONS,
            &["name", "addr", "size", "flags", "kind", "region"],
        ),
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
    /// Symbols past [`MAX_SYMBOLS`], left out.
    pub left_out: usize,
}

/// Read the symbol and section tables of the ELF file in `data`.
pub fn read(data: &[u8]) -> std::result::Result<Elf, String> {
    if !looks_like(data) {
        return Err("not an ELF file: no \\x7fELF at the start".into());
    }
    let file = object::File::parse(data).map_err(|e| format!("not a readable ELF file: {e}"))?;

    let mut section_names: Vec<String> = Vec::new();
    let mut section_flags: Vec<u64> = Vec::new();
    let (mut s_name, mut s_addr, mut s_size, mut s_flags, mut s_kind, mut s_region) = (
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let (mut flash, mut ram) = (0u64, 0u64);
    for section in file.sections() {
        let index = section.index().0;
        if section_names.len() <= index {
            section_names.resize(index + 1, String::new());
            section_flags.resize(index + 1, 0);
        }
        let name = section.name().unwrap_or_default().to_string();
        let flags = sh_flags(section.flags());
        let place = region(flags);
        match place {
            Some("ram") => ram = ram.saturating_add(section.size()),
            Some(_) => flash = flash.saturating_add(section.size()),
            None => {}
        }
        section_names[index] = name.clone();
        section_flags[index] = flags;
        s_name.push(name);
        s_addr.push(section.address());
        s_size.push(section.size());
        s_flags.push(flags_text(flags));
        s_kind.push(format!("{:?}", section.kind()).to_ascii_lowercase());
        s_region.push(place);
    }

    // The static symbol table, or the dynamic one of a stripped library.
    let mut symbols: Vec<_> = file.symbols().collect();
    if symbols.is_empty() {
        symbols = file.dynamic_symbols().collect();
    }
    let total = symbols.len();
    symbols.truncate(MAX_SYMBOLS);
    let rows = symbols.len();
    let (mut name, mut addr, mut size, mut kind, mut bind, mut section, mut place) = (
        Vec::with_capacity(rows),
        Vec::with_capacity(rows),
        Vec::with_capacity(rows),
        Vec::with_capacity(rows),
        Vec::with_capacity(rows),
        Vec::with_capacity(rows),
        Vec::with_capacity(rows),
    );
    for symbol in &symbols {
        let st_info = match symbol.flags() {
            SymbolFlags::Elf { st_info, .. } => st_info,
            _ => 0,
        };
        name.push(demangle(symbol.name().unwrap_or_default()));
        addr.push(symbol.address());
        size.push(symbol.size());
        kind.push(symbol_kind(st_info));
        bind.push(symbol_bind(st_info));
        let (in_section, in_region) = match symbol.section() {
            SymbolSection::Section(index) => (
                section_names.get(index.0).cloned(),
                section_flags.get(index.0).copied().and_then(region),
            ),
            SymbolSection::Undefined => (Some("UND".to_string()), None),
            SymbolSection::Absolute => (Some("ABS".to_string()), None),
            SymbolSection::Common => (Some("COMMON".to_string()), None),
            _ => (None, None),
        };
        section.push(in_section);
        place.push(in_region);
    }
    let symbols = df!(
        "name" => name,
        "addr" => addr,
        "size" => size,
        "kind" => kind,
        "bind" => bind,
        "section" => section,
        "region" => place,
    )
    .map_err(|e| e.to_string())?;
    let sections = df!(
        "name" => s_name,
        "addr" => s_addr,
        "size" => s_size,
        "flags" => s_flags,
        "kind" => s_kind,
        "region" => s_region,
    )
    .map_err(|e| e.to_string())?;

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
            tab: "ELF",
            lines,
            list_title: "Sections",
            list,
            first: false,
            ..Default::default()
        },
        left_out: total - rows,
    })
}

/// Open the ELF file at `path` as the table `wanted` names: its symbols unless
/// `--table sections` says otherwise.
pub fn open(path: &Path, wanted: Option<&str>) -> Result<(LazyFrame, crate::members::Opened)> {
    let tables = tables();
    let picked = match wanted {
        None => SYMBOLS.to_string(),
        Some(_) => {
            match crate::members::pick(tables.clone(), wanted, path, crate::FileFormat::Elf, "")? {
                crate::sqlite::Pick::One(table) => table.name,
                crate::sqlite::Pick::Several(_) => SYMBOLS.to_string(),
            }
        }
    };
    let bytes =
        crate::fixed_records::Bytes::map(path).map_err(|e| eyre!("{}: {e}", path.display()))?;
    let elf = read(bytes.as_slice()).map_err(|e| eyre!("{}: {e}", path.display()))?;
    let mut notes = Vec::new();
    if elf.left_out > 0 {
        notes.push(crate::text_formats::note(
            format!(
                "{} symbols past the first {} are left out.",
                crate::numfmt::group_chrome(elf.left_out),
                crate::numfmt::group_chrome(MAX_SYMBOLS)
            ),
            "the symbol table".to_string(),
        ));
    }
    let df = if picked == SECTIONS {
        elf.sections
    } else {
        elf.symbols
    };
    Ok((
        df.lazy(),
        crate::members::Opened {
            window: None,
            detail: Some(Arc::new(elf.detail)),
            other_tables: crate::members::others(&tables, &picked),
            notes,
            units: Vec::new(),
        },
    ))
}

/// The scan of an ELF file: the table `--table` names, its symbols by default.
fn scan(input: crate::readers::ScanIn<'_>) -> Result<crate::scan::Scan> {
    let (lf, opened) = open(input.path(), input.options.table.as_deref())?;
    input.report.opened = Some(Arc::new(opened));
    Ok(lf.into())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// name, type, flags, addr, offset, size, link, info, align, entsize.
    type SectionHeader = (u32, u32, u64, u64, u64, u64, u32, u32, u64, u64);

    /// A tiny 64-bit little-endian ELF executable: `.text` (AX), `.rodata` (A),
    /// `.data` (WA), `.bss` (WA, no bits), `.symtab`, `.strtab`, `.shstrtab`, and
    /// symbols in each, one of them a mangled Rust name.
    pub(crate) fn tiny() -> Vec<u8> {
        let shstr = b"\0.text\0.rodata\0.data\0.bss\0.symtab\0.strtab\0.shstrtab\0";
        let name_at = |n: &[u8]| {
            shstr
                .windows(n.len())
                .position(|w| w == n)
                .expect("a section name") as u32
        };
        let strtab =
            b"\0main\0TABLE\0counter\0buffer\0_ZN4core3fmt5write17h0123456789abcdefE\0weak_hook\0";
        let str_at = |n: &[u8]| {
            strtab
                .windows(n.len())
                .position(|w| w == n)
                .expect("a symbol name") as u32
        };
        // (name, value, size, info, shndx)
        let syms: Vec<(u32, u64, u64, u8, u16)> = vec![
            (0, 0, 0, 0, 0),
            (str_at(b"main\0"), 0x1000, 64, 0x12, 1),
            (str_at(b"TABLE\0"), 0x2000, 256, 0x11, 2),
            (str_at(b"counter\0"), 0x3000, 4, 0x11, 3),
            (str_at(b"buffer\0"), 0x3010, 1024, 0x01, 4),
            (
                str_at(b"_ZN4core3fmt5write17h0123456789abcdefE\0"),
                0x1040,
                128,
                0x12,
                1,
            ),
            (str_at(b"weak_hook\0"), 0x10c0, 8, 0x22, 1),
        ];
        let mut symtab = Vec::new();
        for (name, value, size, info, shndx) in &syms {
            symtab.extend(name.to_le_bytes());
            symtab.push(*info);
            symtab.push(0);
            symtab.extend(shndx.to_le_bytes());
            symtab.extend(value.to_le_bytes());
            symtab.extend(size.to_le_bytes());
        }
        let text = vec![0xc3u8; 0xc8];
        let rodata = vec![1u8; 256];
        let data = vec![2u8; 16];
        // Section contents after the 64-byte header.
        let mut body = Vec::new();
        let mut place = |bytes: &[u8]| {
            let at = 64 + body.len() as u64;
            body.extend_from_slice(bytes);
            while body.len() % 8 != 0 {
                body.push(0);
            }
            at
        };
        let text_at = place(&text);
        let rodata_at = place(&rodata);
        let data_at = place(&data);
        let symtab_at = place(&symtab);
        let strtab_at = place(strtab);
        let shstr_at = place(shstr);
        let shoff = 64 + body.len() as u64;
        let sections: Vec<SectionHeader> = vec![
            (0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            (
                name_at(b".text\0"),
                1,
                0x6,
                0x1000,
                text_at,
                text.len() as u64,
                0,
                0,
                16,
                0,
            ),
            (
                name_at(b".rodata\0"),
                1,
                0x2,
                0x2000,
                rodata_at,
                256,
                0,
                0,
                8,
                0,
            ),
            (name_at(b".data\0"), 1, 0x3, 0x3000, data_at, 16, 0, 0, 8, 0),
            (
                name_at(b".bss\0"),
                8,
                0x3,
                0x3010,
                data_at + 16,
                1024,
                0,
                0,
                8,
                0,
            ),
            (
                name_at(b".symtab\0"),
                2,
                0,
                0,
                symtab_at,
                symtab.len() as u64,
                6,
                1,
                8,
                24,
            ),
            (
                name_at(b".strtab\0"),
                3,
                0,
                0,
                strtab_at,
                strtab.len() as u64,
                0,
                0,
                1,
                0,
            ),
            (
                name_at(b".shstrtab\0"),
                3,
                0,
                0,
                shstr_at,
                shstr.len() as u64,
                0,
                0,
                1,
                0,
            ),
        ];
        let mut out = Vec::new();
        out.extend(MAGIC);
        out.extend([2, 1, 1, 0]);
        out.extend([0u8; 8]);
        out.extend(2u16.to_le_bytes()); // ET_EXEC
        out.extend(62u16.to_le_bytes()); // x86-64
        out.extend(1u32.to_le_bytes());
        out.extend(0x1000u64.to_le_bytes()); // entry
        out.extend(0u64.to_le_bytes()); // phoff
        out.extend(shoff.to_le_bytes());
        out.extend(0u32.to_le_bytes());
        out.extend(64u16.to_le_bytes());
        out.extend(56u16.to_le_bytes());
        out.extend(0u16.to_le_bytes());
        out.extend(64u16.to_le_bytes());
        out.extend((sections.len() as u16).to_le_bytes());
        out.extend(7u16.to_le_bytes()); // shstrndx
        out.extend(body);
        for (name, kind, flags, addr, offset, size, link, info, align, entsize) in sections {
            out.extend(name.to_le_bytes());
            out.extend(kind.to_le_bytes());
            out.extend(flags.to_le_bytes());
            out.extend(addr.to_le_bytes());
            out.extend(offset.to_le_bytes());
            out.extend(size.to_le_bytes());
            out.extend(link.to_le_bytes());
            out.extend(info.to_le_bytes());
            out.extend(align.to_le_bytes());
            out.extend(entsize.to_le_bytes());
        }
        out
    }

    #[test]
    fn symbols_with_their_section_and_region() {
        let elf = read(&tiny()).unwrap();
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
        let mut cut = tiny();
        cut.truncate(cut.len() - 100);
        assert!(read(&cut).is_err());
    }
}
