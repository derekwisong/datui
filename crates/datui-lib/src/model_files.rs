//! SafeTensors and GGUF model files, read as a table of their tensors.
//!
//! Only the header is read: a 70 GB checkpoint opens as fast as a 7 MB one, because
//! the tensor data is never touched. Both parsers are hand-written over a reader that
//! knows where the header must end, and every length the file states is checked
//! against that before anything is allocated or skipped, so a corrupt or hostile
//! header is an error rather than a multi-gigabyte allocation or a panic.
//!
//! The rows are a small eager `DataFrame` (one per tensor) made lazy, as ORC and Excel
//! are. What is not a row — the file's metadata and the totals — is a
//! [`ModelSummary`], carried to the dataset for the Info panel's Model tab.

use std::io::Read;
use std::path::{Path, PathBuf};

use color_eyre::Result;
use color_eyre::eyre::eyre;
use polars::prelude::*;

use crate::FileFormat;

/// The largest SafeTensors header read: the limit the reference implementation sets.
pub const MAX_SAFETENSORS_HEADER: u64 = 100_000_000;
/// The largest `model.safetensors.index.json` read. Real ones are a few hundred KB.
const MAX_INDEX_JSON: u64 = 64 * 1024 * 1024;
/// Where a GGUF header must have ended. Real headers, vocabulary and merges included,
/// are tens of MB; a length that reaches past this is corruption.
const MAX_GGUF_HEADER: u64 = 1024 * 1024 * 1024;
/// The longest single GGUF string kept. A chat template is a few KB.
const MAX_GGUF_STRING: u64 = 16 * 1024 * 1024;
/// The most dimensions a tensor may have. GGML uses four.
const MAX_DIMS: usize = 8;
/// The most tensors or key/value pairs one GGUF file may declare. Real files have a
/// few thousand tensors and a few dozen pairs; each kept tensor costs about 150 bytes.
const MAX_GGUF_COUNT: u64 = 1 << 20;
/// How deep arrays of arrays are followed.
const MAX_ARRAY_DEPTH: u32 = 4;
/// An array is listed in full up to this many items, and summarized by length after.
const LIST_ITEMS_SHOWN: u64 = 16;
/// A string inside a listed array is cut to this many characters.
const LIST_ITEM_CHARS: usize = 120;
/// The most shard files an index or a directory may name.
const MAX_SHARDS: usize = 100_000;

/// Which of the two formats a model file is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelKind {
    SafeTensors,
    Gguf { version: u32 },
}

impl ModelKind {
    pub fn label(self) -> String {
        match self {
            ModelKind::SafeTensors => "SafeTensors".to_string(),
            ModelKind::Gguf { version } => format!("GGUF v{version}"),
        }
    }
}

/// One metadata value, as the Model tab shows it.
#[derive(Debug, Clone, PartialEq)]
pub enum MetaValue {
    /// A scalar, or a string kept whole (a chat template is shown in full).
    Text(String),
    /// An array: listed when short, otherwise only its length and what it holds.
    List {
        /// What the items are, plural: `strings`, `integers`.
        of: &'static str,
        len: u64,
        /// Every item, when the array is short enough to list; empty otherwise.
        items: Vec<String>,
    },
}

/// What a model's header says beyond its tensors, and their totals.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelSummary {
    pub kind: ModelKind,
    /// The files the tensors come from.
    pub files: usize,
    pub tensors: usize,
    /// The sum of every tensor's parameter count.
    pub params: u64,
    /// The sum of every tensor's bytes, where they are known.
    pub bytes: u64,
    /// Each dtype or quantization type, its tensors and its parameters, most
    /// parameters first.
    pub types: Vec<TypeShare>,
    /// Key and value, in the order the file has them. Across several files the first
    /// file to name a key gives its value.
    pub metadata: Vec<(String, MetaValue)>,
}

/// One dtype's share of a model.
#[derive(Debug, Clone, PartialEq)]
pub struct TypeShare {
    pub name: String,
    pub tensors: usize,
    pub params: u64,
}

/// One tensor, as a header describes it.
#[derive(Debug, Clone, PartialEq)]
pub struct Tensor {
    pub name: String,
    /// The SafeTensors dtype or the GGML type name.
    pub dtype: String,
    pub shape: Vec<u64>,
    /// `None` when the product of the shape overflows.
    pub params: Option<u64>,
    /// `None` for a GGML type datui does not know the size of.
    pub bytes: Option<u64>,
    /// SafeTensors: the start of `data_offsets`. GGUF: the offset as written, from the
    /// start of the tensor data.
    pub offset: u64,
    /// SafeTensors only: the end of `data_offsets`.
    pub offset_end: Option<u64>,
}

/// Metadata as key and value, in the order the file has them.
pub type Metadata = Vec<(String, MetaValue)>;

/// One file's header.
#[derive(Debug, Clone, PartialEq)]
pub struct Header {
    pub kind: ModelKind,
    pub tensors: Vec<Tensor>,
    pub metadata: Vec<(String, MetaValue)>,
}

/// A reader that knows where the header must end and refuses any length past it.
struct Bounded<R> {
    inner: R,
    pos: u64,
    end: u64,
    big_endian: bool,
}

impl<R: Read> Bounded<R> {
    fn left(&self) -> u64 {
        self.end.saturating_sub(self.pos)
    }

    /// Fail unless `n` more bytes fit before the end.
    fn need(&self, n: u64, what: &str) -> Result<()> {
        if n > self.left() {
            return Err(eyre!(
                "GGUF: {what} runs past the end of the header ({n} bytes, {} left)",
                self.left()
            ));
        }
        Ok(())
    }

    fn fill<const N: usize>(&mut self, what: &str) -> Result<[u8; N]> {
        self.need(N as u64, what)?;
        let mut buf = [0u8; N];
        self.inner
            .read_exact(&mut buf)
            .map_err(|e| eyre!("GGUF: reading {what}: {e}"))?;
        self.pos += N as u64;
        Ok(buf)
    }

    fn u8(&mut self, what: &str) -> Result<u8> {
        Ok(self.fill::<1>(what)?[0])
    }

    fn u16(&mut self, what: &str) -> Result<u16> {
        let b = self.fill::<2>(what)?;
        Ok(if self.big_endian {
            u16::from_be_bytes(b)
        } else {
            u16::from_le_bytes(b)
        })
    }

    fn u32(&mut self, what: &str) -> Result<u32> {
        let b = self.fill::<4>(what)?;
        Ok(if self.big_endian {
            u32::from_be_bytes(b)
        } else {
            u32::from_le_bytes(b)
        })
    }

    fn u64(&mut self, what: &str) -> Result<u64> {
        let b = self.fill::<8>(what)?;
        Ok(if self.big_endian {
            u64::from_be_bytes(b)
        } else {
            u64::from_le_bytes(b)
        })
    }

    /// A length-prefixed string, kept. Invalid UTF-8 is replaced, not refused.
    fn string(&mut self, what: &str) -> Result<String> {
        let len = self.u64(what)?;
        if len > MAX_GGUF_STRING {
            return Err(eyre!(
                "GGUF: {what} is {len} bytes, longer than datui reads"
            ));
        }
        self.need(len, what)?;
        let mut buf = Vec::new();
        (&mut self.inner)
            .take(len)
            .read_to_end(&mut buf)
            .map_err(|e| eyre!("GGUF: reading {what}: {e}"))?;
        if buf.len() as u64 != len {
            return Err(eyre!("GGUF: {what} is cut short"));
        }
        self.pos += len;
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }

    /// Read past `n` bytes. Read rather than sought: the skips are a vocabulary's
    /// strings, a few bytes each, and a seek would throw the read buffer away for each.
    fn skip(&mut self, n: u64, what: &str) -> Result<()> {
        self.need(n, what)?;
        let skipped = std::io::copy(&mut (&mut self.inner).take(n), &mut std::io::sink())
            .map_err(|e| eyre!("GGUF: reading {what}: {e}"))?;
        if skipped != n {
            return Err(eyre!("GGUF: {what} is cut short"));
        }
        self.pos += n;
        Ok(())
    }
}

/// Whether the first bytes of a file are a SafeTensors header: a little-endian length
/// a header could have, then the `{` that opens its JSON.
pub fn looks_like_safetensors(head: &[u8]) -> bool {
    if head.len() < 9 {
        return false;
    }
    let len = u64::from_le_bytes(head[..8].try_into().expect("eight bytes"));
    (2..=MAX_SAFETENSORS_HEADER).contains(&len) && head[8] == b'{'
}

/// Whether the first bytes of a file are GGUF's magic.
pub fn looks_like_gguf(head: &[u8]) -> bool {
    head.starts_with(b"GGUF")
}

/// Read one SafeTensors header from `reader`, which holds `len` bytes in all.
pub fn read_safetensors<R: Read>(reader: R, len: u64) -> Result<Header> {
    let mut reader = reader;
    let mut prefix = [0u8; 8];
    reader
        .read_exact(&mut prefix)
        .map_err(|_| eyre!("SafeTensors: the file is shorter than its header length"))?;
    let header_len = u64::from_le_bytes(prefix);
    if header_len > MAX_SAFETENSORS_HEADER {
        return Err(eyre!(
            "SafeTensors: the header is {header_len} bytes, more than the {MAX_SAFETENSORS_HEADER} allowed"
        ));
    }
    if header_len > len.saturating_sub(8) {
        return Err(eyre!(
            "SafeTensors: the header claims {header_len} bytes and the file has {}",
            len.saturating_sub(8)
        ));
    }
    let mut json = Vec::new();
    reader
        .take(header_len)
        .read_to_end(&mut json)
        .map_err(|e| eyre!("SafeTensors: reading the header: {e}"))?;
    if json.len() as u64 != header_len {
        return Err(eyre!("SafeTensors: the header is cut short"));
    }
    parse_safetensors_json(&json, len.saturating_sub(8).saturating_sub(header_len))
}

/// The header's JSON, without its length prefix. `data_len` is how many bytes of
/// tensor data follow it, which every tensor's `data_offsets` must stay inside.
///
/// Read straight into what is kept rather than through `serde_json::Value`: a hostile
/// 100 MB header of tiny arrays would be gigabytes as a `Value` tree. Fields the spec
/// does not name are passed over without being stored, and keys are seen in the order
/// the file has them, which is the order the metadata is shown in.
fn parse_safetensors_json(json: &[u8], data_len: u64) -> Result<Header> {
    let mut de = serde_json::Deserializer::from_slice(json);
    let parsed = serde::Deserializer::deserialize_map(&mut de, StHeaderVisitor)
        .and_then(|header| de.end().map(|()| header))
        .map_err(|e| eyre!("SafeTensors: the header is not valid: {e}"))?;
    let (mut tensors, metadata) = parsed;
    for t in &tensors {
        if t.offset_end.is_some_and(|end| end > data_len) {
            return Err(eyre!(
                "SafeTensors: tensor {:?} runs past the end of the file ({data_len} bytes of data)",
                t.name
            ));
        }
    }
    // The order the data is in.
    tensors.sort_by(|a, b| a.offset.cmp(&b.offset).then_with(|| a.name.cmp(&b.name)));
    Ok(Header {
        kind: ModelKind::SafeTensors,
        tensors,
        metadata,
    })
}

/// One tensor's entry. Any other field is skipped, not kept.
#[derive(serde::Deserialize)]
struct StEntry {
    dtype: String,
    shape: StShape,
    data_offsets: (u64, u64),
}

/// A shape, refused past [`MAX_DIMS`] before a longer list is stored.
struct StShape(Vec<u64>);

impl<'de> serde::Deserialize<'de> for StShape {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = StShape;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "a list of at most {MAX_DIMS} dimensions")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<StShape, A::Error> {
                let mut dims = Vec::new();
                while let Some(d) = seq.next_element::<u64>()? {
                    if dims.len() == MAX_DIMS {
                        return Err(serde::de::Error::custom("more dimensions than datui reads"));
                    }
                    dims.push(d);
                }
                Ok(StShape(dims))
            }
        }
        d.deserialize_seq(V)
    }
}

/// A `__metadata__` value. The spec says text; a number or a bool is shown as written,
/// and anything nested is passed over and named by what it is.
struct StMetaValue(String);

impl<'de> serde::Deserialize<'de> for StMetaValue {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = StMetaValue;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a metadata value")
            }
            fn visit_str<E>(self, v: &str) -> std::result::Result<StMetaValue, E> {
                Ok(StMetaValue(v.to_string()))
            }
            fn visit_string<E>(self, v: String) -> std::result::Result<StMetaValue, E> {
                Ok(StMetaValue(v))
            }
            fn visit_bool<E>(self, v: bool) -> std::result::Result<StMetaValue, E> {
                Ok(StMetaValue(v.to_string()))
            }
            fn visit_i64<E>(self, v: i64) -> std::result::Result<StMetaValue, E> {
                Ok(StMetaValue(v.to_string()))
            }
            fn visit_u64<E>(self, v: u64) -> std::result::Result<StMetaValue, E> {
                Ok(StMetaValue(v.to_string()))
            }
            fn visit_f64<E>(self, v: f64) -> std::result::Result<StMetaValue, E> {
                Ok(StMetaValue(v.to_string()))
            }
            fn visit_unit<E>(self) -> std::result::Result<StMetaValue, E> {
                Ok(StMetaValue("null".to_string()))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<StMetaValue, A::Error> {
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
                Ok(StMetaValue("[array]".to_string()))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<StMetaValue, A::Error> {
                while map
                    .next_entry::<serde::de::IgnoredAny, serde::de::IgnoredAny>()?
                    .is_some()
                {}
                Ok(StMetaValue("{object}".to_string()))
            }
        }
        d.deserialize_any(V)
    }
}

/// `__metadata__`, in the order the file has it.
struct StMetadata(Metadata);

impl<'de> serde::Deserialize<'de> for StMetadata {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = StMetadata;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an object of metadata")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<StMetadata, A::Error> {
                let mut out: Metadata = Vec::new();
                while let Some((key, StMetaValue(value))) =
                    map.next_entry::<String, StMetaValue>()?
                {
                    out.push((key, MetaValue::Text(value)));
                }
                Ok(StMetadata(out))
            }
        }
        d.deserialize_map(V)
    }
}

/// The whole header: its tensors, and `__metadata__`.
struct StHeaderVisitor;

impl<'de> serde::de::Visitor<'de> for StHeaderVisitor {
    type Value = (Vec<Tensor>, Metadata);
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a JSON object of tensors")
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(
        self,
        mut map: A,
    ) -> std::result::Result<Self::Value, A::Error> {
        use serde::de::Error;
        let mut tensors = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut metadata = None;
        while let Some(name) = map.next_key::<String>()? {
            if name == "__metadata__" {
                if metadata.is_some() {
                    return Err(A::Error::custom("__metadata__ appears twice"));
                }
                let StMetadata(m) = map
                    .next_value()
                    .map_err(|e| A::Error::custom(format!("__metadata__: {e}")))?;
                metadata = Some(m);
                continue;
            }
            let entry: StEntry = map
                .next_value()
                .map_err(|e| A::Error::custom(format!("tensor {name:?}: {e}")))?;
            if !seen.insert(name.clone()) {
                return Err(A::Error::custom(format!("tensor {name:?} appears twice")));
            }
            let (start, end) = entry.data_offsets;
            if end < start {
                return Err(A::Error::custom(format!(
                    "tensor {name:?} ends before it starts"
                )));
            }
            let shape = entry.shape.0;
            tensors.push(Tensor {
                name,
                dtype: entry.dtype,
                params: product(&shape),
                shape,
                bytes: Some(end - start),
                offset: start,
                offset_end: Some(end),
            });
        }
        Ok((tensors, metadata.unwrap_or_default()))
    }
}

/// The product of a shape; 1 for a scalar, `None` on overflow.
fn product(shape: &[u64]) -> Option<u64> {
    shape.iter().try_fold(1u64, |acc, d| acc.checked_mul(*d))
}

/// A GGML type: its name, and how many elements a block of how many bytes holds.
fn ggml_type(id: u32) -> Option<(&'static str, u64, u64)> {
    Some(match id {
        0 => ("F32", 1, 4),
        1 => ("F16", 1, 2),
        2 => ("Q4_0", 32, 18),
        3 => ("Q4_1", 32, 20),
        6 => ("Q5_0", 32, 22),
        7 => ("Q5_1", 32, 24),
        8 => ("Q8_0", 32, 34),
        9 => ("Q8_1", 32, 36),
        10 => ("Q2_K", 256, 84),
        11 => ("Q3_K", 256, 110),
        12 => ("Q4_K", 256, 144),
        13 => ("Q5_K", 256, 176),
        14 => ("Q6_K", 256, 210),
        15 => ("Q8_K", 256, 292),
        16 => ("IQ2_XXS", 256, 66),
        17 => ("IQ2_XS", 256, 74),
        18 => ("IQ3_XXS", 256, 98),
        19 => ("IQ1_S", 256, 50),
        20 => ("IQ4_NL", 32, 18),
        21 => ("IQ3_S", 256, 110),
        22 => ("IQ2_S", 256, 82),
        23 => ("IQ4_XS", 256, 136),
        24 => ("I8", 1, 1),
        25 => ("I16", 1, 2),
        26 => ("I32", 1, 4),
        27 => ("I64", 1, 8),
        28 => ("F64", 1, 8),
        29 => ("IQ1_M", 256, 56),
        30 => ("BF16", 1, 2),
        34 => ("TQ1_0", 256, 54),
        35 => ("TQ2_0", 256, 66),
        39 => ("MXFP4", 32, 17),
        _ => return None,
    })
}

/// Where tensor data starts when `general.alignment` does not say.
const GGUF_DEFAULT_ALIGNMENT: u64 = 32;

/// GGUF metadata value types.
const GGUF_STRING: u32 = 8;
const GGUF_ARRAY: u32 = 9;

/// The size of a fixed-width GGUF value type; `None` for a string or an array.
fn gguf_fixed_size(ty: u32) -> Option<u64> {
    match ty {
        0 | 1 | 7 => Some(1),
        2 | 3 => Some(2),
        4..=6 => Some(4),
        10..=12 => Some(8),
        _ => None,
    }
}

/// What an array of `ty` holds, as its summary says it.
fn gguf_items_noun(ty: u32) -> &'static str {
    match ty {
        0..=5 | 10 | 11 => "integers",
        6 | 12 => "floats",
        7 => "bools",
        GGUF_STRING => "strings",
        _ => "arrays",
    }
}

/// One fixed-width value, as text.
fn gguf_scalar<R: Read>(r: &mut Bounded<R>, ty: u32) -> Result<String> {
    let what = "a metadata value";
    Ok(match ty {
        0 => r.u8(what)?.to_string(),
        1 => (r.u8(what)? as i8).to_string(),
        2 => r.u16(what)?.to_string(),
        3 => (r.u16(what)? as i16).to_string(),
        4 => r.u32(what)?.to_string(),
        5 => (r.u32(what)? as i32).to_string(),
        6 => f32::from_bits(r.u32(what)?).to_string(),
        7 => (r.u8(what)? != 0).to_string(),
        10 => r.u64(what)?.to_string(),
        11 => (r.u64(what)? as i64).to_string(),
        12 => f64::from_bits(r.u64(what)?).to_string(),
        other => return Err(eyre!("GGUF: unknown metadata type {other}")),
    })
}

/// A value of type `ty`, read whole when it is kept and skipped where it is long.
fn gguf_value<R: Read>(r: &mut Bounded<R>, ty: u32, depth: u32) -> Result<MetaValue> {
    match ty {
        GGUF_STRING => Ok(MetaValue::Text(r.string("a metadata string")?)),
        GGUF_ARRAY => {
            if depth >= MAX_ARRAY_DEPTH {
                return Err(eyre!("GGUF: arrays nest deeper than datui reads"));
            }
            let item_ty = r.u32("an array's type")?;
            let len = r.u64("an array's length")?;
            // Every item takes at least this much, so the length is checked against
            // what is left before a single item is read.
            let least = match item_ty {
                GGUF_STRING => 8,
                GGUF_ARRAY => 12,
                t => gguf_fixed_size(t).ok_or_else(|| eyre!("GGUF: unknown array type {t}"))?,
            };
            r.need(
                len.checked_mul(least)
                    .ok_or_else(|| eyre!("GGUF: an array's length overflows"))?,
                "an array",
            )?;
            let of = gguf_items_noun(item_ty);
            let listed = len <= LIST_ITEMS_SHOWN && item_ty != GGUF_ARRAY;
            if !listed {
                skip_items(r, item_ty, len, depth)?;
                return Ok(MetaValue::List {
                    of,
                    len,
                    items: Vec::new(),
                });
            }
            let mut items = Vec::with_capacity(len as usize);
            for _ in 0..len {
                let item = if item_ty == GGUF_STRING {
                    let s = r.string("an array's string")?;
                    let cut: String = s.chars().take(LIST_ITEM_CHARS).collect();
                    if cut.len() < s.len() {
                        format!("{cut}...")
                    } else {
                        cut
                    }
                } else {
                    gguf_scalar(r, item_ty)?
                };
                items.push(item);
            }
            Ok(MetaValue::List { of, len, items })
        }
        t => Ok(MetaValue::Text(gguf_scalar(r, t)?)),
    }
}

/// Step over `len` items of `ty` without keeping them.
fn skip_items<R: Read>(r: &mut Bounded<R>, ty: u32, len: u64, depth: u32) -> Result<()> {
    match ty {
        GGUF_STRING => {
            for _ in 0..len {
                let n = r.u64("an array's string")?;
                r.skip(n, "an array's string")?;
            }
        }
        GGUF_ARRAY => {
            for _ in 0..len {
                gguf_value(r, GGUF_ARRAY, depth + 1)?;
            }
        }
        t => {
            let size = gguf_fixed_size(t).ok_or_else(|| eyre!("GGUF: unknown array type {t}"))?;
            r.skip(len.saturating_mul(size), "an array")?;
        }
    }
    Ok(())
}

/// Read one GGUF header (versions 2 and 3, either byte order) from `reader`, which
/// holds `len` bytes in all.
pub fn read_gguf<R: Read>(reader: R, len: u64) -> Result<Header> {
    let mut r = Bounded {
        inner: reader,
        pos: 0,
        end: len.min(MAX_GGUF_HEADER),
        big_endian: false,
    };
    let magic = r
        .fill::<4>("the magic number")
        .map_err(|_| eyre!("GGUF: the file is too short to be GGUF"))?;
    if &magic != b"GGUF" {
        return Err(eyre!("GGUF: the file does not start with GGUF"));
    }
    let raw = r.fill::<4>("the version")?;
    let mut version = u32::from_le_bytes(raw);
    // The magic reads the same either way; a big-endian file's version does not.
    if version & 0xFFFF == 0 {
        r.big_endian = true;
        version = u32::from_be_bytes(raw);
    }
    match version {
        2 | 3 => {}
        1 => return Err(eyre!("GGUF: version 1 files are not supported")),
        v => return Err(eyre!("GGUF: version {v} is not one datui reads (2 or 3)")),
    }
    let tensor_count = r.u64("the tensor count")?;
    let kv_count = r.u64("the metadata count")?;
    // A key/value pair takes at least 12 bytes and a tensor's entry at least 24, so
    // either count is bounded by what is left before anything is allocated for it.
    if tensor_count > MAX_GGUF_COUNT || tensor_count.saturating_mul(24) > r.left() {
        return Err(eyre!(
            "GGUF: {tensor_count} tensors cannot fit in the header"
        ));
    }
    if kv_count > MAX_GGUF_COUNT || kv_count.saturating_mul(12) > r.left() {
        return Err(eyre!(
            "GGUF: {kv_count} metadata entries cannot fit in the header"
        ));
    }
    let mut metadata = Vec::with_capacity(kv_count as usize);
    for _ in 0..kv_count {
        let key = r.string("a metadata key")?;
        let ty = r.u32("a metadata type")?;
        let value = gguf_value(&mut r, ty, 0).map_err(|e| eyre!("{e} (in {key:?})"))?;
        metadata.push((key, value));
    }
    let mut tensors = Vec::with_capacity(tensor_count as usize);
    for _ in 0..tensor_count {
        let name = r.string("a tensor name")?;
        let n_dims = r.u32("a tensor's dimension count")? as usize;
        if n_dims > MAX_DIMS {
            return Err(eyre!(
                "GGUF: tensor {name:?} has {n_dims} dimensions, more than datui reads"
            ));
        }
        let mut shape = Vec::with_capacity(n_dims);
        for _ in 0..n_dims {
            shape.push(r.u64("a tensor dimension")?);
        }
        let ty = r.u32("a tensor's type")?;
        let offset = r.u64("a tensor's offset")?;
        let params = product(&shape);
        let (dtype, bytes) = match ggml_type(ty) {
            Some((name, block, size)) => (
                name.to_string(),
                params
                    .filter(|p| p % block == 0)
                    .and_then(|p| (p / block).checked_mul(size)),
            ),
            None => (format!("type {ty}"), None),
        };
        tensors.push(Tensor {
            name,
            dtype,
            shape,
            params,
            bytes,
            offset,
            offset_end: None,
        });
    }
    // The tensor data starts at the next multiple of the alignment after the header,
    // and every tensor whose size is known must end inside the file: a download cut
    // short is an error, not a table that looks whole.
    let alignment = metadata
        .iter()
        .find(|(k, _)| k == "general.alignment")
        .and_then(|(_, v)| match v {
            MetaValue::Text(t) => t.parse::<u64>().ok(),
            MetaValue::List { .. } => None,
        })
        .filter(|a| a.is_power_of_two())
        .unwrap_or(GGUF_DEFAULT_ALIGNMENT);
    let data_start = r.pos.next_multiple_of(alignment);
    let data_len = len.saturating_sub(data_start);
    for t in &tensors {
        let end = t.bytes.and_then(|b| t.offset.checked_add(b));
        if t.bytes.is_some() && end.is_none_or(|end| end > data_len) {
            return Err(eyre!(
                "GGUF: tensor {:?} runs past the end of the file ({data_len} bytes of data)",
                t.name
            ));
        }
    }
    Ok(Header {
        kind: ModelKind::Gguf { version },
        tensors,
        metadata,
    })
}

/// Parse `bytes` as whichever model header it starts with. For the fuzz target and the
/// tests: both parsers over a slice, with no file.
pub fn parse_header(bytes: &[u8]) -> Result<Header> {
    if looks_like_gguf(bytes) {
        read_gguf(bytes, bytes.len() as u64)
    } else {
        read_safetensors(bytes, bytes.len() as u64)
    }
}

/// Read one file's header with the parser `format` names.
fn read_file(path: &Path, format: FileFormat) -> Result<Header> {
    let file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    let reader = std::io::BufReader::new(file);
    match format {
        FileFormat::Gguf => read_gguf(reader, len),
        _ => read_safetensors(reader, len),
    }
}

/// A `model.safetensors.index.json`: the shard each tensor is in, and metadata. Any
/// other field is skipped, not kept.
#[derive(serde::Deserialize)]
struct StIndex {
    #[serde(default)]
    metadata: Option<StMetadata>,
    weight_map: std::collections::BTreeMap<String, String>,
}

/// Whether `path` is a SafeTensors index: `model.safetensors.index.json`.
pub fn is_safetensors_index(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.to_ascii_lowercase().ends_with(".safetensors.index.json"))
}

/// The shards an index names, beside it and in name order, and the index's own
/// metadata.
fn read_index(path: &Path) -> Result<(Vec<PathBuf>, Metadata)> {
    let file = std::fs::File::open(path).map_err(|e| eyre!("{}: {e}", path.display()))?;
    let len = file.metadata()?.len();
    if len > MAX_INDEX_JSON {
        return Err(eyre!(
            "{}: the index is {len} bytes, more than datui reads",
            path.display()
        ));
    }
    let mut text = Vec::new();
    file.take(MAX_INDEX_JSON).read_to_end(&mut text)?;
    let index: StIndex = serde_json::from_slice(&text)
        .map_err(|e| eyre!("{}: not a SafeTensors index: {e}", path.display()))?;
    let dir = path.parent().unwrap_or(Path::new(""));
    let names: std::collections::BTreeSet<String> = index.weight_map.into_values().collect();
    if names.len() > MAX_SHARDS {
        return Err(eyre!("{}: the index names too many shards", path.display()));
    }
    let mut shards: Vec<PathBuf> = Vec::with_capacity(names.len());
    for name in names {
        // A shard is a file beside its index, never a path out of the directory.
        let named = Path::new(&name);
        if named.components().count() != 1 || named.file_name().is_none() {
            return Err(eyre!(
                "{}: the index names {name:?}, which is not a file beside it",
                path.display()
            ));
        }
        shards.push(dir.join(named));
    }
    let metadata = index.metadata.map(|StMetadata(m)| m).unwrap_or_default();
    Ok((shards, metadata))
}

/// Read `paths` — model files, or SafeTensors indexes that name them — as one table of
/// tensors, with a `file` column when there is more than one file.
pub fn read_model(paths: &[PathBuf], format: FileFormat) -> Result<(LazyFrame, ModelSummary)> {
    let mut files: Vec<PathBuf> = Vec::new();
    // Each file once, however many indexes and names reach it.
    let mut seen = std::collections::HashSet::new();
    let mut metadata: Vec<(String, MetaValue)> = Vec::new();
    for path in paths {
        if format == FileFormat::Safetensors && is_safetensors_index(path) {
            let (shards, index_meta) = read_index(path)?;
            merge_metadata(&mut metadata, index_meta);
            for shard in shards {
                if seen.insert(shard.clone()) {
                    files.push(shard);
                }
            }
        } else if seen.insert(path.clone()) {
            files.push(path.clone());
        }
    }
    if files.is_empty() {
        return Err(eyre!("No model files to read"));
    }
    let mut headers = Vec::with_capacity(files.len());
    for file in &files {
        // The open names the path it was given; of several files, say which one.
        let header = read_file(file, format).map_err(|e| match files.len() {
            1 => e,
            _ => eyre!("{}: {e}", file.display()),
        })?;
        headers.push(header);
    }
    let names: Vec<String> = files
        .iter()
        .map(|f| {
            f.file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| f.display().to_string())
        })
        .collect();
    build(&headers, &names, metadata)
}

/// Keep each key's first value.
fn merge_metadata(into: &mut Vec<(String, MetaValue)>, from: Vec<(String, MetaValue)>) {
    let mut seen: std::collections::HashSet<String> = into.iter().map(|(k, _)| k.clone()).collect();
    into.extend(from.into_iter().filter(|(key, _)| seen.insert(key.clone())));
}

/// The table and the summary for headers already read; `names` are their files.
pub fn build(
    headers: &[Header],
    names: &[String],
    mut metadata: Vec<(String, MetaValue)>,
) -> Result<(LazyFrame, ModelSummary)> {
    let kind = headers
        .first()
        .map(|h| h.kind)
        .ok_or_else(|| eyre!("No model files to read"))?;
    let safetensors = kind == ModelKind::SafeTensors;
    let many = headers.len() > 1;
    let rows: usize = headers.iter().map(|h| h.tensors.len()).sum();

    let mut file_col = Vec::with_capacity(if many { rows } else { 0 });
    let mut name = Vec::with_capacity(rows);
    let mut dtype = Vec::with_capacity(rows);
    let values: usize = headers
        .iter()
        .flat_map(|h| &h.tensors)
        .map(|t| t.shape.len())
        .sum();
    let mut shape = ListPrimitiveChunkedBuilder::<UInt64Type>::new(
        "shape".into(),
        rows,
        values,
        DataType::UInt64,
    );
    let mut params = Vec::with_capacity(rows);
    let mut bytes = Vec::with_capacity(rows);
    let mut start = Vec::with_capacity(rows);
    let mut end = Vec::with_capacity(rows);
    // By name, then into a list: a hostile header can name a type per tensor.
    let mut types: std::collections::HashMap<&str, TypeShare> = Default::default();
    let (mut total_params, mut total_bytes) = (0u64, 0u64);
    merge_metadata(
        &mut metadata,
        headers.iter().flat_map(|h| h.metadata.clone()).collect(),
    );
    for (header, file) in headers.iter().zip(names) {
        for t in &header.tensors {
            if many {
                file_col.push(file.as_str());
            }
            name.push(t.name.as_str());
            dtype.push(t.dtype.as_str());
            shape.append_slice(&t.shape);
            params.push(t.params);
            bytes.push(t.bytes);
            start.push(t.offset);
            end.push(t.offset_end);
            let p = t.params.unwrap_or(0);
            total_params = total_params.saturating_add(p);
            total_bytes = total_bytes.saturating_add(t.bytes.unwrap_or(0));
            let share = types.entry(t.dtype.as_str()).or_insert_with(|| TypeShare {
                name: t.dtype.clone(),
                tensors: 0,
                params: 0,
            });
            share.tensors += 1;
            share.params = share.params.saturating_add(p);
        }
    }
    let mut types: Vec<TypeShare> = types.into_values().collect();
    types.sort_by(|a, b| b.params.cmp(&a.params).then_with(|| a.name.cmp(&b.name)));

    let shape = shape.finish().into_series();
    let mut columns: Vec<Column> = Vec::new();
    if many {
        columns.push(Series::new("file".into(), file_col).into());
    }
    columns.push(Series::new("name".into(), name).into());
    columns.push(Series::new(if safetensors { "dtype" } else { "type" }.into(), dtype).into());
    columns.push(shape.into());
    columns.push(Series::new("params".into(), params).into());
    columns.push(Series::new("bytes".into(), bytes).into());
    if safetensors {
        columns.push(Series::new("offset_start".into(), start).into());
        columns.push(Series::new("offset_end".into(), end).into());
    } else {
        columns.push(Series::new("offset".into(), start).into());
    }
    let df = DataFrame::new(rows, columns)?;
    let summary = ModelSummary {
        kind,
        files: headers.len(),
        tensors: rows,
        params: total_params,
        bytes: total_bytes,
        types,
        metadata,
    };
    Ok((df.lazy(), summary))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A SafeTensors file: the length, the JSON, then `data` bytes of tensor data.
    pub(crate) fn safetensors_bytes(json: &str, data: usize) -> Vec<u8> {
        let mut out = (json.len() as u64).to_le_bytes().to_vec();
        out.extend_from_slice(json.as_bytes());
        out.extend(std::iter::repeat_n(0u8, data));
        out
    }

    /// Writes GGUF version 3, little-endian, as llama.cpp does.
    pub(crate) struct GgufWriter {
        pub out: Vec<u8>,
    }

    impl GgufWriter {
        pub(crate) fn new(tensors: u64, kvs: u64) -> Self {
            let mut out = b"GGUF".to_vec();
            out.extend_from_slice(&3u32.to_le_bytes());
            out.extend_from_slice(&tensors.to_le_bytes());
            out.extend_from_slice(&kvs.to_le_bytes());
            Self { out }
        }
        pub(crate) fn str(&mut self, s: &str) -> &mut Self {
            self.out.extend_from_slice(&(s.len() as u64).to_le_bytes());
            self.out.extend_from_slice(s.as_bytes());
            self
        }
        pub(crate) fn u32(&mut self, v: u32) -> &mut Self {
            self.out.extend_from_slice(&v.to_le_bytes());
            self
        }
        pub(crate) fn u64(&mut self, v: u64) -> &mut Self {
            self.out.extend_from_slice(&v.to_le_bytes());
            self
        }
        /// Pad to the default alignment, then `n` bytes of tensor data.
        pub(crate) fn data(&mut self, n: usize) -> &mut Self {
            let padded = self.out.len().next_multiple_of(32);
            self.out.resize(padded + n, 0);
            self
        }
        pub(crate) fn kv_str(&mut self, key: &str, value: &str) -> &mut Self {
            self.str(key).u32(GGUF_STRING).str(value)
        }
        pub(crate) fn kv_u32(&mut self, key: &str, value: u32) -> &mut Self {
            self.str(key).u32(4).u32(value)
        }
        pub(crate) fn kv_strings(&mut self, key: &str, items: &[&str]) -> &mut Self {
            self.str(key).u32(GGUF_ARRAY).u32(GGUF_STRING);
            self.u64(items.len() as u64);
            for item in items {
                self.str(item);
            }
            self
        }
        pub(crate) fn tensor(
            &mut self,
            name: &str,
            shape: &[u64],
            ty: u32,
            offset: u64,
        ) -> &mut Self {
            self.str(name).u32(shape.len() as u32);
            for d in shape {
                self.u64(*d);
            }
            self.u32(ty).u64(offset)
        }
    }

    #[test]
    fn safetensors_tensors_are_rows_in_data_order() {
        let json = r#"{"__metadata__":{"format":"pt"},
            "b.weight":{"dtype":"F32","shape":[2,3],"data_offsets":[8,32]},
            "a.bias":{"dtype":"BF16","shape":[4],"data_offsets":[0,8]}}"#;
        let bytes = safetensors_bytes(json, 32);
        let header = parse_header(&bytes).unwrap();
        assert_eq!(header.kind, ModelKind::SafeTensors);
        assert_eq!(
            header
                .tensors
                .iter()
                .map(|t| t.name.as_str())
                .collect::<Vec<_>>(),
            ["a.bias", "b.weight"],
            "the order the data is in"
        );
        let w = &header.tensors[1];
        assert_eq!(
            (w.params, w.bytes, w.offset, w.offset_end),
            (Some(6), Some(24), 8, Some(32))
        );
        assert_eq!(
            header.metadata,
            vec![("format".to_string(), MetaValue::Text("pt".to_string()))]
        );
    }

    #[test]
    fn a_safetensors_header_longer_than_the_file_is_refused() {
        let mut bytes = safetensors_bytes("{}", 0);
        bytes[..8].copy_from_slice(&50_000_000u64.to_le_bytes());
        let err = parse_header(&bytes).unwrap_err().to_string();
        assert!(err.contains("claims"), "{err}");
        bytes[..8].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(parse_header(&bytes).is_err());
        for bad in [
            r#"{"t":{"dtype":"F32","shape":[2],"data_offsets":[8,0]}}"#,
            r#"{"t":{"dtype":"F32","shape":[-1],"data_offsets":[0,8]}}"#,
            r#"{"t":{"shape":[2],"data_offsets":[0,8]}}"#,
            r#"[1,2]"#,
            r#"{"__metadata__":3}"#,
        ] {
            assert!(parse_header(&safetensors_bytes(bad, 8)).is_err(), "{bad}");
        }
    }

    /// The spec's rules a viewer can check from the header: one entry per name, two
    /// offsets inside the data, a shape of counts. A field it does not name is passed
    /// over, and `__metadata__` keeps the file's order.
    #[test]
    fn safetensors_entries_follow_the_spec() {
        let ok = r#"{"__metadata__":{"z":"1","a":"2","n":3},
            "t":{"dtype":"F32","shape":[2],"data_offsets":[0,8],"extra":[[1,2],{"x":1}]}}"#;
        let header = parse_header(&safetensors_bytes(ok, 8)).unwrap();
        let keys: Vec<&str> = header.metadata.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["z", "a", "n"], "the file's order");
        assert_eq!(header.metadata[2].1, MetaValue::Text("3".into()));
        for (bad, why) in [
            (
                r#"{"t":{"dtype":"F32","shape":[2],"data_offsets":[0,8]},
                    "t":{"dtype":"F32","shape":[2],"data_offsets":[0,8]}}"#,
                "appears twice",
            ),
            (
                r#"{"t":{"dtype":"F32","shape":[4],"data_offsets":[0,16]}}"#,
                "past the end",
            ),
            (
                r#"{"t":{"dtype":"F32","shape":[2],"data_offsets":[0,4,8]}}"#,
                "tensor \"t\"",
            ),
            (
                r#"{"t":{"dtype":"F32","shape":[1,1,1,1,1,1,1,1,1],"data_offsets":[0,4]}}"#,
                "dimensions",
            ),
        ] {
            let err = parse_header(&safetensors_bytes(bad, 8))
                .unwrap_err()
                .to_string();
            assert!(err.contains(why), "{bad}: {err}");
        }
    }

    #[test]
    fn gguf_tensors_and_metadata_are_read() {
        let mut w = GgufWriter::new(2, 3);
        w.kv_str("general.architecture", "llama")
            .kv_u32("llama.context_length", 4096)
            .kv_strings("tokenizer.ggml.tokens", &["a"; 40]);
        w.tensor("token_embd.weight", &[256, 4], 12, 0).tensor(
            "output_norm.weight",
            &[256],
            0,
            576,
        );
        w.data(576 + 1024);
        let header = parse_header(&w.out).unwrap();
        assert_eq!(header.kind, ModelKind::Gguf { version: 3 });
        assert_eq!(header.metadata[0].1, MetaValue::Text("llama".into()));
        assert_eq!(header.metadata[1].1, MetaValue::Text("4096".into()));
        assert_eq!(
            header.metadata[2].1,
            MetaValue::List {
                of: "strings",
                len: 40,
                items: vec![]
            },
            "a long array is its length"
        );
        let embd = &header.tensors[0];
        assert_eq!(embd.dtype, "Q4_K");
        assert_eq!((embd.params, embd.bytes), (Some(1024), Some(4 * 144)));
        assert_eq!(header.tensors[1].bytes, Some(1024));
    }

    #[test]
    fn a_big_endian_gguf_is_read() {
        let mut out = b"GGUF".to_vec();
        out.extend_from_slice(&3u32.to_be_bytes());
        out.extend_from_slice(&1u64.to_be_bytes());
        out.extend_from_slice(&0u64.to_be_bytes());
        out.extend_from_slice(&1u64.to_be_bytes());
        out.push(b'x');
        out.extend_from_slice(&1u32.to_be_bytes());
        out.extend_from_slice(&8u64.to_be_bytes());
        out.extend_from_slice(&1u32.to_be_bytes());
        out.extend_from_slice(&0u64.to_be_bytes());
        out.resize(out.len().next_multiple_of(32) + 16, 0);
        let header = parse_header(&out).unwrap();
        assert_eq!(header.tensors[0].shape, vec![8]);
        assert_eq!(header.tensors[0].dtype, "F16");
    }

    #[test]
    fn corrupt_gguf_lengths_are_errors_not_allocations() {
        // Counts no header could hold.
        let w = GgufWriter::new(u64::MAX, 0);
        assert!(parse_header(&w.out).is_err());
        let w = GgufWriter::new(0, 1 << 40);
        assert!(parse_header(&w.out).is_err());
        // A string longer than the file.
        let mut w = GgufWriter::new(0, 1);
        w.u64(u64::MAX - 3);
        assert!(parse_header(&w.out).is_err());
        // An array longer than the file.
        let mut w = GgufWriter::new(0, 1);
        w.str("k").u32(GGUF_ARRAY).u32(4).u64(1 << 60);
        assert!(parse_header(&w.out).is_err());
        // Too many dimensions.
        let mut w = GgufWriter::new(1, 0);
        w.str("t").u32(1_000_000);
        assert!(parse_header(&w.out).is_err());
        // Version 1, and a version from the future.
        let mut w = GgufWriter::new(0, 0);
        w.out[4..8].copy_from_slice(&1u32.to_le_bytes());
        assert!(parse_header(&w.out).is_err());
        w.out[4..8].copy_from_slice(&9u32.to_le_bytes());
        assert!(parse_header(&w.out).is_err());
        // Cut short anywhere.
        let mut w = GgufWriter::new(1, 1);
        w.kv_str("general.name", "tiny")
            .tensor("t", &[4, 4], 0, 0)
            .data(64);
        for cut in 0..w.out.len() {
            assert!(parse_header(&w.out[..cut]).is_err(), "cut at {cut}");
        }
        assert!(parse_header(&w.out).is_ok());
    }

    /// A GGUF whose tensor data is cut short is refused, measured from where the data
    /// starts: after the header, at `general.alignment` or 32.
    #[test]
    fn a_gguf_tensor_must_end_inside_the_file() {
        let tensor = |w: &mut GgufWriter| {
            w.tensor("t", &[4], 0, 0);
        };
        let mut w = GgufWriter::new(1, 0);
        tensor(&mut w);
        w.data(15);
        let err = parse_header(&w.out).unwrap_err().to_string();
        assert!(err.contains("past the end"), "{err}");
        w.data(16);
        assert!(parse_header(&w.out).is_ok());

        // A wider alignment moves the start of the data further on.
        let mut w = GgufWriter::new(1, 1);
        w.kv_u32("general.alignment", 256);
        tensor(&mut w);
        let header_end = w.out.len();
        w.data(16);
        w.out.truncate(header_end.next_multiple_of(256) + 15);
        assert!(parse_header(&w.out).is_err(), "measured from 256");
        w.out.resize(header_end.next_multiple_of(256) + 16, 0);
        assert!(parse_header(&w.out).is_ok());
    }

    /// A header can name a type per tensor and repeat a key many times over; the
    /// totals and the merge stay linear rather than comparing each against all.
    #[test]
    fn many_types_and_keys_build_in_one_pass() {
        let n = 50_000;
        let header = Header {
            kind: ModelKind::SafeTensors,
            tensors: (0..n)
                .map(|i| Tensor {
                    name: format!("t{i}"),
                    dtype: format!("X{i}"),
                    shape: vec![2],
                    params: Some(2),
                    bytes: Some(0),
                    offset: 0,
                    offset_end: Some(0),
                })
                .collect(),
            metadata: (0..n)
                .map(|i| (format!("k{}", i % 7), MetaValue::Text(i.to_string())))
                .collect(),
        };
        let (_, summary) = build(&[header], &["a".into()], vec![]).unwrap();
        assert_eq!(summary.types.len(), n);
        assert_eq!(summary.metadata.len(), 7, "each key once");
        assert_eq!(
            summary.metadata[0].1,
            MetaValue::Text("0".into()),
            "the first"
        );
    }

    #[test]
    fn the_frame_and_the_summary_agree() {
        let st = parse_header(&safetensors_bytes(
            r#"{"x":{"dtype":"F16","shape":[2,2],"data_offsets":[0,8]},
                "y":{"dtype":"F32","shape":[3],"data_offsets":[8,20]}}"#,
            20,
        ))
        .unwrap();
        let (lf, summary) = build(
            &[st.clone(), st],
            &["a.safetensors".into(), "b.safetensors".into()],
            vec![],
        )
        .unwrap();
        let df = lf.collect().unwrap();
        assert_eq!(
            df.get_column_names()
                .iter()
                .map(|n| n.as_str())
                .collect::<Vec<_>>(),
            [
                "file",
                "name",
                "dtype",
                "shape",
                "params",
                "bytes",
                "offset_start",
                "offset_end"
            ]
        );
        assert_eq!(df.height(), 4);
        assert_eq!((summary.files, summary.tensors), (2, 4));
        assert_eq!((summary.params, summary.bytes), (14, 40));
        assert_eq!(summary.types[0].name, "F16", "most parameters first");
    }
}
