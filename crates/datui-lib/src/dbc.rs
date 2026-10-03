//! DBC files: what the frames of a CAN bus mean.
//!
//! A DBC file names each message by its id (`BO_`) and the signals packed in its data
//! (`SG_`): start bit, length, byte order, sign, factor and offset, unit, and for a
//! multiplexed message which value of the multiplexer selects each signal. `VAL_`
//! names a signal's values, `SIG_VALTYPE_` marks a float signal, and `CM_` comments
//! on messages and signals. Parsed by hand; every count and length is bounded.
//!
//! DBC files are found on the format search path, as FIX dictionaries are: a `.dbc`
//! file applies to every interface, and a TOML file of `kind = "dbc"` names a DBC file
//! (`file`) and the interface it applies to (`[match] interface = "can1"`). `--dict FILE`
//! is read over them. A later file's message of an id takes the place of an earlier's.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::formats::SpecError;

/// The largest DBC file read.
pub const MAX_FILE: u64 = 64 << 20;
/// Messages in one file, signals in one message, and values one signal names.
const MAX_MESSAGES: usize = 65_536;
const MAX_SIGNALS: usize = 4096;
const MAX_VALUES: usize = 65_536;
/// The longest statement (`VAL_`, `CM_`) read.
const MAX_STATEMENT: usize = 1 << 20;

/// A signal's place in its message and what its value means.
#[derive(Debug, Clone, PartialEq)]
pub struct Signal {
    pub name: String,
    pub start: u32,
    pub length: u32,
    /// Motorola (big-endian) bit order; Intel otherwise.
    pub big_endian: bool,
    pub signed: bool,
    pub factor: f64,
    pub offset: f64,
    pub unit: String,
    pub mux: Mux,
    /// `SIG_VALTYPE_` 1 (a 32-bit float) or 2 (a 64-bit float); 0 for an integer.
    pub float: u8,
    /// `VAL_`: names of its values.
    pub values: Arc<std::collections::BTreeMap<i64, String>>,
    pub comment: Option<String>,
}

/// How a signal takes part in multiplexing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mux {
    Plain,
    /// The multiplexer: its value selects the multiplexed signals.
    Multiplexer,
    /// Present when the multiplexer holds this value.
    When(u64),
}

/// One message.
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    pub id: u32,
    pub extended: bool,
    pub name: String,
    pub dlc: u32,
    pub signals: Vec<Signal>,
    pub comment: Option<String>,
}

/// One DBC file and what it applies to.
#[derive(Debug, Clone, PartialEq)]
pub struct Dbc {
    pub name: String,
    pub path: Option<PathBuf>,
    /// The interface it applies to; every one when `None`.
    pub interface: Option<String>,
    pub messages: Vec<Message>,
    /// What was passed over: extended multiplexing, statements that do not parse.
    pub notes: Vec<String>,
}

impl Dbc {
    /// Whether it applies to frames on `interface`.
    pub fn applies(&self, interface: &str) -> bool {
        self.interface.as_deref().is_none_or(|i| i == interface)
    }
}

fn error(path: Option<&Path>, line: usize, message: impl Into<String>) -> SpecError {
    SpecError {
        path: path.map(Path::to_path_buf),
        line,
        column: if line > 0 { 1 } else { 0 },
        message: message.into(),
    }
}

/// Whether a TOML file is a DBC reference (`kind = "dbc"`) rather than a format spec.
pub fn is_dbc_toml(text: &str) -> bool {
    text.parse::<toml::Table>()
        .is_ok_and(|t| t.get("kind").and_then(|k| k.as_str()) == Some("dbc"))
}

/// Read the DBC file `path`, or the DBC a `kind = "dbc"` TOML file names. `Ok(None)` for
/// a TOML file of another kind.
pub fn load(path: &Path) -> Result<Option<Dbc>, SpecError> {
    let read = |path: &Path| -> Result<String, SpecError> {
        let size = std::fs::metadata(path)
            .map_err(|e| error(Some(path), 0, format!("could not read it: {e}")))?
            .len();
        if size > MAX_FILE {
            return Err(error(
                Some(path),
                0,
                format!(
                    "{} MiB is past the {} MiB a DBC dictionary may be",
                    size >> 20,
                    MAX_FILE >> 20
                ),
            ));
        }
        let bytes = std::fs::read(path)
            .map_err(|e| error(Some(path), 0, format!("could not read it: {e}")))?;
        // DBC files are often Windows-1252: read what is not UTF-8 as Latin-1.
        Ok(match String::from_utf8(bytes) {
            Ok(text) => text,
            Err(e) => e.into_bytes().iter().map(|&b| char::from(b)).collect(),
        })
    };
    let toml = path
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("toml"));
    if !toml {
        let text = read(path)?;
        let name = path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        return parse(&text, &name, Some(path)).map(Some);
    }
    let text = read(path)?;
    if !is_dbc_toml(&text) {
        return Ok(None);
    }
    let table: toml::Table = text
        .parse()
        .map_err(|e| error(Some(path), 0, format!("not TOML: {e}")))?;
    let file = table.get("file").and_then(|f| f.as_str()).ok_or_else(|| {
        error(
            Some(path),
            0,
            "kind = \"dbc\" names its .dbc file with file = \"...\"",
        )
    })?;
    let interface = match table.get("match") {
        None => None,
        Some(toml::Value::Table(m)) => match m.get("interface").or_else(|| m.get("bus")) {
            Some(toml::Value::String(s)) => Some(s.clone()),
            _ => {
                return Err(error(Some(path), 0, "[match] takes interface = \"can0\""));
            }
        },
        Some(_) => return Err(error(Some(path), 0, "match is a table: [match]")),
    };
    let dbc_path = crate::config::expand_path(file);
    let dbc_path = if dbc_path.is_relative() {
        path.parent().unwrap_or(Path::new("")).join(dbc_path)
    } else {
        dbc_path
    };
    let name = table
        .get("name")
        .and_then(|n| n.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| {
            path.file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default()
        });
    let text = read(&dbc_path)?;
    let mut dbc = parse(&text, &name, Some(&dbc_path))?;
    dbc.interface = interface;
    Ok(Some(dbc))
}

/// A token of a DBC statement.
#[derive(Debug, Clone, PartialEq)]
enum Token {
    Word(String),
    Str(String),
    Punct(char),
}

fn tokens(text: &str) -> Vec<Token> {
    let mut out = Vec::new();
    let mut chars = text.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if c.is_whitespace() {
            continue;
        }
        if c == '"' {
            let mut s = String::new();
            let mut escaped = false;
            for (_, c) in chars.by_ref() {
                if escaped {
                    s.push(c);
                    escaped = false;
                } else if c == '\\' {
                    escaped = true;
                } else if c == '"' {
                    break;
                } else if s.len() < 1 << 16 {
                    s.push(c);
                }
            }
            out.push(Token::Str(s));
        } else if ":|@(),[];".contains(c) {
            out.push(Token::Punct(c));
        } else {
            let mut end = i + c.len_utf8();
            while let Some(&(j, d)) = chars.peek() {
                if d.is_whitespace() || d == '"' || ":|@(),[];".contains(d) {
                    break;
                }
                end = j + d.len_utf8();
                chars.next();
            }
            out.push(Token::Word(text[i..end].to_string()));
        }
    }
    out
}

/// Keywords that start a statement ending in `;`.
const STATEMENTS: [&str; 7] = [
    "VAL_",
    "CM_",
    "SIG_VALTYPE_",
    "SG_MUL_VAL_",
    "BA_",
    "BA_DEF_",
    "BA_DEF_DEF_",
];

/// Parse the text of a DBC file named `name`.
pub fn parse(text: &str, name: &str, path: Option<&Path>) -> Result<Dbc, SpecError> {
    let mut dbc = Dbc {
        name: name.to_string(),
        path: path.map(Path::to_path_buf),
        interface: None,
        messages: Vec::new(),
        notes: Vec::new(),
    };
    let mut by_id: HashMap<(u32, bool), usize> = HashMap::new();
    let mut values: Vec<(u32, String, std::collections::BTreeMap<i64, String>)> = Vec::new();
    let mut floats: Vec<(u32, String, u8)> = Vec::new();
    let mut comments: Vec<(u32, Option<String>, String)> = Vec::new();
    let mut extended_mux = false;
    let mut lines = text.lines().enumerate();
    while let Some((n, line)) = lines.next() {
        let trimmed = line.trim_start();
        let keyword = trimmed.split_whitespace().next().unwrap_or("");
        match keyword {
            "BO_" => {
                let t = tokens(trimmed);
                // BO_ id name : dlc transmitter
                let (Some(Token::Word(raw)), Some(Token::Word(msg)), Some(Token::Punct(':'))) =
                    (t.get(1), t.get(2), t.get(3))
                else {
                    return Err(error(path, n + 1, "BO_ is BO_ id name: dlc sender"));
                };
                let raw: u32 = raw
                    .parse()
                    .map_err(|_| error(path, n + 1, format!("BO_ id {raw} is not a number")))?;
                let dlc = match t.get(4) {
                    Some(Token::Word(d)) => d.parse().unwrap_or(8),
                    _ => 8,
                };
                // The pseudo-message that holds signals of no message.
                if raw == 0xC000_0000 {
                    continue;
                }
                if dbc.messages.len() >= MAX_MESSAGES {
                    return Err(error(
                        path,
                        n + 1,
                        format!("more than {MAX_MESSAGES} messages"),
                    ));
                }
                let extended = raw & 0x8000_0000 != 0;
                let id = raw & 0x1FFF_FFFF;
                by_id.insert((raw, false), dbc.messages.len());
                dbc.messages.push(Message {
                    id,
                    extended,
                    name: msg.clone(),
                    dlc,
                    signals: Vec::new(),
                    comment: None,
                });
            }
            "SG_" => {
                let signal = parse_signal(&tokens(trimmed))
                    .ok_or_else(|| error(path, n + 1, "SG_ is SG_ name [M|mN] : start|length@order+/- (factor,offset) [min|max] \"unit\" receivers"))?;
                let Some(message) = dbc.messages.last_mut() else {
                    return Err(error(path, n + 1, "SG_ before any BO_"));
                };
                if message.signals.len() >= MAX_SIGNALS {
                    return Err(error(
                        path,
                        n + 1,
                        format!("more than {MAX_SIGNALS} signals in {}", message.name),
                    ));
                }
                message.signals.push(signal);
            }
            k if STATEMENTS.contains(&k) => {
                // A keyword alone on its line is the list of new symbols (NS_).
                if trimmed.split_whitespace().nth(1).is_none() {
                    continue;
                }
                let mut statement = trimmed.to_string();
                while !ends_statement(&statement) {
                    let Some((_, next)) = lines.next() else {
                        break;
                    };
                    if statement.len() > MAX_STATEMENT {
                        return Err(error(path, n + 1, format!("a {k} statement does not end")));
                    }
                    statement.push('\n');
                    statement.push_str(next);
                }
                let t = tokens(&statement);
                match k {
                    "VAL_" => {
                        if let (Some(Token::Word(id)), Some(Token::Word(sig))) =
                            (t.get(1), t.get(2))
                            && let Ok(id) = id.parse::<u32>()
                        {
                            let mut map = std::collections::BTreeMap::new();
                            let mut i = 3;
                            while let (Some(Token::Word(v)), Some(Token::Str(label))) =
                                (t.get(i), t.get(i + 1))
                            {
                                if let Ok(v) = v
                                    .parse::<i64>()
                                    .or_else(|_| v.parse::<f64>().map(|f| f as i64))
                                {
                                    map.insert(v, label.clone());
                                }
                                if map.len() >= MAX_VALUES {
                                    break;
                                }
                                i += 2;
                            }
                            values.push((id, sig.clone(), map));
                        }
                    }
                    "SIG_VALTYPE_" => {
                        if let (Some(Token::Word(id)), Some(Token::Word(sig))) =
                            (t.get(1), t.get(2))
                            && let Ok(id) = id.parse::<u32>()
                            && let Some(Token::Word(kind)) =
                                t.iter().skip(3).find(|t| matches!(t, Token::Word(_)))
                            && let Ok(kind) = kind.parse::<u8>()
                        {
                            floats.push((id, sig.clone(), kind));
                        }
                    }
                    "CM_" => match (t.get(1), t.get(2), t.get(3), t.get(4)) {
                        (
                            Some(Token::Word(w)),
                            Some(Token::Word(id)),
                            Some(Token::Word(sig)),
                            Some(Token::Str(c)),
                        ) if w == "SG_" => {
                            if let Ok(id) = id.parse() {
                                comments.push((id, Some(sig.clone()), c.clone()));
                            }
                        }
                        (Some(Token::Word(w)), Some(Token::Word(id)), Some(Token::Str(c)), _)
                            if w == "BO_" =>
                        {
                            if let Ok(id) = id.parse() {
                                comments.push((id, None, c.clone()));
                            }
                        }
                        _ => {}
                    },
                    "SG_MUL_VAL_" => extended_mux = true,
                    _ => {}
                }
            }
            _ => {}
        }
    }
    for (raw, sig, map) in values {
        if let Some(&m) = by_id.get(&(raw, false))
            && let Some(s) = dbc.messages[m].signals.iter_mut().find(|s| s.name == sig)
        {
            s.values = Arc::new(map);
        }
    }
    for (raw, sig, kind) in floats {
        if let Some(&m) = by_id.get(&(raw, false))
            && let Some(s) = dbc.messages[m].signals.iter_mut().find(|s| s.name == sig)
            && (kind == 1 && s.length == 32 || kind == 2 && s.length == 64)
        {
            s.float = kind;
        }
    }
    for (raw, sig, comment) in comments {
        if let Some(&m) = by_id.get(&(raw, false)) {
            match sig {
                Some(sig) => {
                    if let Some(s) = dbc.messages[m].signals.iter_mut().find(|s| s.name == sig) {
                        s.comment = Some(comment);
                    }
                }
                None => dbc.messages[m].comment = Some(comment),
            }
        }
    }
    if extended_mux {
        dbc.notes.push(format!(
            "{}: extended multiplexing (SG_MUL_VAL_) is not read; such signals follow their message's one multiplexer",
            dbc.name
        ));
    }
    Ok(dbc)
}

/// Whether `statement` holds a `;` outside a string.
fn ends_statement(statement: &str) -> bool {
    let mut quoted = false;
    let mut escaped = false;
    for c in statement.chars() {
        match c {
            _ if escaped => escaped = false,
            '\\' if quoted => escaped = true,
            '"' => quoted = !quoted,
            ';' if !quoted => return true,
            _ => {}
        }
    }
    false
}

fn parse_signal(t: &[Token]) -> Option<Signal> {
    let word = |i: usize| match t.get(i) {
        Some(Token::Word(w)) => Some(w.as_str()),
        _ => None,
    };
    let punct = |i: usize, c: char| t.get(i) == Some(&Token::Punct(c));
    let name = word(1)?.to_string();
    let (mux, at) = if punct(2, ':') {
        (Mux::Plain, 3)
    } else {
        let m = word(2)?;
        if !punct(3, ':') {
            return None;
        }
        let mux = if m == "M" {
            Mux::Multiplexer
        } else {
            // `m3M`: a multiplexer that is itself multiplexed; read as selected by 3.
            let v = m.strip_prefix('m')?;
            Mux::When(v.trim_end_matches('M').parse().ok()?)
        };
        (mux, 4)
    };
    let start: u32 = word(at)?.parse().ok()?;
    if !punct(at + 1, '|') {
        return None;
    }
    let length: u32 = word(at + 2)?.parse().ok()?;
    if !punct(at + 3, '@') {
        return None;
    }
    let order = word(at + 4)?;
    let big_endian = order.starts_with('0');
    let signed = order.ends_with('-');
    if !punct(at + 5, '(') || !punct(at + 7, ',') || !punct(at + 9, ')') {
        return None;
    }
    let factor: f64 = word(at + 6)?.parse().ok()?;
    let offset: f64 = word(at + 8)?.parse().ok()?;
    // [min|max], then the unit.
    let unit = t[at + 10..]
        .iter()
        .find_map(|t| match t {
            Token::Str(s) => Some(s.clone()),
            _ => None,
        })
        .unwrap_or_default();
    if length == 0 || length > 64 || start >= 512 * 8 || !factor.is_finite() || !offset.is_finite()
    {
        return None;
    }
    Some(Signal {
        name,
        start,
        length,
        big_endian,
        signed,
        factor,
        offset,
        unit,
        mux,
        float: 0,
        values: Arc::default(),
        comment: None,
    })
}

/// The raw bits of `signal` in `data`, or `None` when the data is too short to hold them.
pub fn raw(signal: &Signal, data: &[u8]) -> Option<u64> {
    let len = signal.length as usize;
    let mut value: u64 = 0;
    if signal.big_endian {
        // Motorola: the start bit is the most significant, numbered within its byte
        // from the least significant; the next is one lower, wrapping to the next
        // byte's bit 7.
        let mut pos = signal.start as usize;
        for _ in 0..len {
            let byte = *data.get(pos / 8)?;
            let bit = (byte >> (pos % 8)) & 1;
            value = (value << 1) | u64::from(bit);
            if pos.is_multiple_of(8) {
                pos += 15;
            } else {
                pos -= 1;
            }
        }
    } else {
        // Intel: the start bit is the least significant, counting up.
        for i in 0..len {
            let pos = signal.start as usize + i;
            let byte = *data.get(pos / 8)?;
            value |= u64::from((byte >> (pos % 8)) & 1) << i;
        }
    }
    Some(value)
}

/// `signal`'s value in `data` as an integer of its sign, before factor and offset.
pub fn integer(signal: &Signal, data: &[u8]) -> Option<i128> {
    let bits = raw(signal, data)?;
    let len = signal.length;
    Some(if signal.signed && len < 64 && bits >> (len - 1) & 1 == 1 {
        i128::from(bits) - (1i128 << len)
    } else if signal.signed && len == 64 {
        i128::from(bits as i64)
    } else {
        i128::from(bits)
    })
}

/// `signal`'s physical value in `data`: factor and offset applied, a float signal read
/// as one.
pub fn physical(signal: &Signal, data: &[u8]) -> Option<f64> {
    let bits = raw(signal, data)?;
    let value = match signal.float {
        1 => f64::from(f32::from_bits(bits as u32)),
        2 => f64::from_bits(bits),
        _ => integer(signal, data)? as f64,
    };
    Some(value * signal.factor + signal.offset)
}

/// Whether `signal` is present in a frame whose multiplexer reads `mux`.
pub fn present(signal: &Signal, mux: Option<u64>) -> bool {
    match signal.mux {
        Mux::When(v) => mux == Some(v),
        _ => true,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) const SAMPLE: &str = r#"VERSION ""

NS_ :
	NS_DESC_
	CM_
	BA_DEF_
	VAL_

BS_:

BU_: ECU GW

BO_ 291 ENGINE: 8 ECU
 SG_ Speed : 0|16@1+ (0.125,0) [0|8191] "rpm" GW
 SG_ Temp : 16|8@1- (1,-40) [-40|215] "degC" GW
 SG_ Gear : 24|4@1+ (1,0) [0|15] "" GW

BO_ 2566844926 BODY: 8 GW
 SG_ Pressure : 7|16@0+ (0.1,0) [0|6553.5] "kPa" ECU
 SG_ Offset : 23|12@0- (1,0) [-2048|2047] "" ECU

BO_ 512 MUXED: 8 ECU
 SG_ Page M : 0|8@1+ (1,0) [0|255] "" GW
 SG_ Volts m1 : 8|16@1+ (0.01,0) [0|655.35] "V" GW
 SG_ Amps m2 : 8|16@1- (0.1,0) [-3276.8|3276.7] "A" GW
 SG_ Ratio : 24|32@1- (1,0) [0|0] "" GW

CM_ SG_ 291 Speed "Engine speed;
over two lines";
BA_DEF_ SG_ "GenSigStartValue" INT 0 100;
VAL_ 291 Gear 0 "Neutral" 1 "First" 2 "Second" ;
SIG_VALTYPE_ 512 Ratio : 1;
"#;

    #[test]
    fn messages_signals_values_and_comments() {
        let dbc = parse(SAMPLE, "car", None).unwrap();
        assert_eq!(dbc.messages.len(), 3);
        let body = &dbc.messages[1];
        assert!(body.extended);
        assert_eq!(body.id, 0x18FEF1FE);
        let engine = &dbc.messages[0];
        assert_eq!(engine.signals[1].offset, -40.0);
        assert!(engine.signals[1].signed);
        assert_eq!(
            engine.signals[2].values.get(&1).map(String::as_str),
            Some("First")
        );
        assert_eq!(
            engine.signals[0].comment.as_deref(),
            Some("Engine speed;\nover two lines")
        );
        let muxed = &dbc.messages[2];
        assert_eq!(muxed.signals[0].mux, Mux::Multiplexer);
        assert_eq!(muxed.signals[2].mux, Mux::When(2));
        assert_eq!(muxed.signals[3].float, 1);
    }

    #[test]
    fn intel_motorola_signed_and_float() {
        let dbc = parse(SAMPLE, "car", None).unwrap();
        let engine = &dbc.messages[0];
        // Speed 0x1F40 = 8000 * 0.125 = 1000 rpm; Temp 0xF6 = -10 - 40 = -50.
        let data = [0x40, 0x1F, 0xF6, 0x02, 0, 0, 0, 0];
        assert_eq!(physical(&engine.signals[0], &data), Some(1000.0));
        assert_eq!(physical(&engine.signals[1], &data), Some(-50.0));
        assert_eq!(integer(&engine.signals[2], &data), Some(2));
        let body = &dbc.messages[1];
        // Motorola: Pressure starts at bit 7 of byte 0: 0x1234 = 4660 * 0.1.
        // Offset starts at bit 23 (byte 2's top) for 12 bits: 0xFFF = -1.
        let data = [0x12, 0x34, 0xFF, 0xF0, 0, 0, 0, 0];
        let p = physical(&body.signals[0], &data).unwrap();
        assert!((p - 466.0).abs() < 1e-9);
        assert_eq!(integer(&body.signals[1], &data), Some(-1));
        let muxed = &dbc.messages[2];
        let mut data = [1u8, 0x10, 0x27, 0, 0, 0, 0, 0];
        data[3..7].copy_from_slice(&1.5f32.to_le_bytes());
        assert_eq!(physical(&muxed.signals[3], &data), Some(1.5));
        assert!(present(&muxed.signals[1], Some(1)));
        assert!(!present(&muxed.signals[2], Some(1)));
        // Too short a frame holds no value.
        assert_eq!(raw(&engine.signals[0], &[0x40]), None);
    }

    #[test]
    fn bad_statements_are_errors_with_their_line() {
        let e = parse("BO_ x ENGINE: 8 ECU\n", "car", None).unwrap_err();
        assert_eq!(e.line, 1);
        let e = parse("BO_ 1 A: 8 X\n SG_ broken\n", "car", None).unwrap_err();
        assert_eq!(e.line, 2);
        assert!(parse(" SG_ S : 0|8@1+ (1,0) [0|0] \"\" X\n", "car", None).is_err());
        // An unterminated statement runs to the end of the file, and is bounded.
        assert!(parse("CM_ \"never ends", "car", None).is_ok());
    }
}
