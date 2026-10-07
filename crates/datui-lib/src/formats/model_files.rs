//! SafeTensors and GGUF model files as a table of their tensors. Only the header is
//! read, so a 70 GB checkpoint opens like a 7 MB one. Both parsers are hand-written over
//! a bounded reader, checking every stated length before allocating or skipping, so a
//! hostile header errors rather than allocating gigabytes or panicking. Rows are a
//! small eager frame made lazy; metadata and totals go to a [`ModelSummary`] for Info's
//! Model tab.

use std::io::Read;
use std::path::{Path, PathBuf};

use color_eyre::Result;
use color_eyre::eyre::eyre;
use polars::prelude::*;

use crate::FileFormat;
use crate::error_display::FileError;

/// What datui does with a SafeTensors file: see [`crate::formats::readers`].
pub(crate) const SAFETENSORS: crate::formats::readers::Reader = crate::formats::readers::Reader {
    scan,
    signatures: &[crate::formats::readers::Signature {
        says: |head, _| looks_like_safetensors(head),
        kind: crate::formats::readers::Kind::Magic,
        trusted: crate::formats::readers::EVERYWHERE,
    }],
    ..crate::formats::readers::BASE
};

/// What datui does with a GGUF file: see [`crate::formats::readers`].
pub(crate) const GGUF: crate::formats::readers::Reader = crate::formats::readers::Reader {
    scan,
    signatures: &[crate::formats::readers::Signature {
        says: |head, _| looks_like_gguf(head),
        kind: crate::formats::readers::Kind::Magic,
        trusted: crate::formats::readers::EVERYWHERE,
    }],
    ..crate::formats::readers::BASE
};

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
                "{what} runs past the end of the GGUF header ({n} bytes, {} left)",
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
            .map_err(|e| eyre!("cannot read {what} in the GGUF header: {e}"))?;
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
                "{what} is {len} bytes, longer than datui reads in a GGUF header"
            ));
        }
        self.need(len, what)?;
        let mut buf = Vec::new();
        (&mut self.inner)
            .take(len)
            .read_to_end(&mut buf)
            .map_err(|e| eyre!("cannot read {what} in the GGUF header: {e}"))?;
        if buf.len() as u64 != len {
            return Err(eyre!("{what} in the GGUF header is cut short"));
        }
        self.pos += len;
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }

    /// Read past `n` bytes. Read rather than sought: the skips are a vocabulary's
    /// strings, a few bytes each, and a seek would throw the read buffer away for each.
    fn skip(&mut self, n: u64, what: &str) -> Result<()> {
        self.need(n, what)?;
        let skipped = std::io::copy(&mut (&mut self.inner).take(n), &mut std::io::sink())
            .map_err(|e| eyre!("cannot read {what} in the GGUF header: {e}"))?;
        if skipped != n {
            return Err(eyre!("{what} in the GGUF header is cut short"));
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

/// The header length a SafeTensors file's first eight bytes state, refused when it is
/// more than the spec allows or than the `len`-byte file holds.
fn safetensors_header_len(prefix: [u8; 8], len: u64) -> Result<u64> {
    let header_len = u64::from_le_bytes(prefix);
    if header_len > MAX_SAFETENSORS_HEADER {
        return Err(eyre!(
            "the SafeTensors header is {header_len} bytes, more than the {MAX_SAFETENSORS_HEADER} allowed"
        ));
    }
    if header_len > len.saturating_sub(8) {
        return Err(eyre!(
            "the SafeTensors header claims {header_len} bytes and the file has {}",
            len.saturating_sub(8)
        ));
    }
    Ok(header_len)
}

/// Read one SafeTensors header from `reader`, which holds `len` bytes in all.
pub fn read_safetensors<R: Read>(reader: R, len: u64) -> Result<Header> {
    let mut reader = reader;
    let mut prefix = [0u8; 8];
    reader
        .read_exact(&mut prefix)
        .map_err(|_| eyre!("the file is shorter than its SafeTensors header length"))?;
    let header_len = safetensors_header_len(prefix, len)?;
    let mut json = Vec::new();
    reader
        .take(header_len)
        .read_to_end(&mut json)
        .map_err(|e| eyre!("cannot read the SafeTensors header: {e}"))?;
    if json.len() as u64 != header_len {
        return Err(eyre!("the SafeTensors header is cut short"));
    }
    parse_safetensors_json(&json, len.saturating_sub(8).saturating_sub(header_len))
}

/// The header's JSON without its length prefix; `data_len` bytes of tensor data follow,
/// which every `data_offsets` must stay inside. Deserialized straight into what is kept
/// (a hostile 100 MB header would be gigabytes as a `Value`), unknown fields skipped,
/// keys in file order (the metadata's display order).
fn parse_safetensors_json(json: &[u8], data_len: u64) -> Result<Header> {
    let mut de = serde_json::Deserializer::from_slice(json);
    let parsed = serde::Deserializer::deserialize_map(&mut de, StHeaderVisitor)
        .and_then(|header| de.end().map(|()| header))
        .map_err(|e| eyre!("the SafeTensors header is not valid: {e}"))?;
    let (mut tensors, metadata) = parsed;
    for t in &tensors {
        if t.offset_end.is_some_and(|end| end > data_len) {
            return Err(eyre!(
                "tensor \"{}\" runs past the end of the file ({data_len} bytes of data)",
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
                .map_err(|e| A::Error::custom(format!("tensor \"{name}\": {e}")))?;
            if !seen.insert(name.clone()) {
                return Err(A::Error::custom(format!("tensor \"{name}\" appears twice")));
            }
            let (start, end) = entry.data_offsets;
            if end < start {
                return Err(A::Error::custom(format!(
                    "tensor \"{name}\" ends before it starts"
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
        // Repacked Q4_0 and IQ4_NL, since removed from GGML; files written by
        // llama.cpp in late 2024 still carry them, with the same block sizes.
        31 => ("Q4_0_4_4", 32, 18),
        32 => ("Q4_0_4_8", 32, 18),
        33 => ("Q4_0_8_8", 32, 18),
        34 => ("TQ1_0", 256, 54),
        35 => ("TQ2_0", 256, 66),
        36 => ("IQ4_NL_4_4", 32, 18),
        37 => ("IQ4_NL_4_8", 32, 18),
        38 => ("IQ4_NL_8_8", 32, 18),
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
        other => return Err(eyre!("unknown GGUF metadata type {other}")),
    })
}

/// A value of type `ty`, read whole when it is kept and skipped where it is long.
fn gguf_value<R: Read>(r: &mut Bounded<R>, ty: u32, depth: u32) -> Result<MetaValue> {
    match ty {
        GGUF_STRING => Ok(MetaValue::Text(r.string("a metadata string")?)),
        GGUF_ARRAY => {
            if depth >= MAX_ARRAY_DEPTH {
                return Err(eyre!("GGUF arrays nest deeper than datui reads"));
            }
            let item_ty = r.u32("an array's type")?;
            let len = r.u64("an array's length")?;
            // Every item takes at least this much, so the length is checked against
            // what is left before a single item is read.
            let least = match item_ty {
                GGUF_STRING => 8,
                GGUF_ARRAY => 12,
                t => gguf_fixed_size(t).ok_or_else(|| eyre!("unknown GGUF array type {t}"))?,
            };
            r.need(
                len.checked_mul(least)
                    .ok_or_else(|| eyre!("a GGUF array's length overflows"))?,
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
            let size = gguf_fixed_size(t).ok_or_else(|| eyre!("unknown GGUF array type {t}"))?;
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
        .map_err(|_| eyre!("the file is too short to be GGUF"))?;
    if &magic != b"GGUF" {
        return Err(eyre!("not a GGUF file: it does not start with GGUF"));
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
        1 => return Err(eyre!("GGUF version 1 files are not supported")),
        v => return Err(eyre!("GGUF version {v} is not one datui reads (2 or 3)")),
    }
    let tensor_count = r.u64("the tensor count")?;
    let kv_count = r.u64("the metadata count")?;
    // A key/value pair takes at least 12 bytes and a tensor's entry at least 24, so
    // either count is bounded by what is left before anything is allocated for it.
    if tensor_count > MAX_GGUF_COUNT || tensor_count.saturating_mul(24) > r.left() {
        return Err(eyre!(
            "{tensor_count} tensors cannot fit in the GGUF header"
        ));
    }
    if kv_count > MAX_GGUF_COUNT || kv_count.saturating_mul(12) > r.left() {
        return Err(eyre!(
            "{kv_count} metadata entries cannot fit in the GGUF header"
        ));
    }
    let mut metadata = Vec::with_capacity(kv_count as usize);
    for _ in 0..kv_count {
        let key = r.string("a metadata key")?;
        let ty = r.u32("a metadata type")?;
        let value = gguf_value(&mut r, ty, 0).map_err(|e| eyre!("{e} (in \"{key}\")"))?;
        metadata.push((key, value));
    }
    let mut tensors = Vec::with_capacity(tensor_count as usize);
    for _ in 0..tensor_count {
        let name = r.string("a tensor name")?;
        let n_dims = r.u32("a tensor's dimension count")? as usize;
        if n_dims > MAX_DIMS {
            return Err(eyre!(
                "tensor \"{name}\" has {n_dims} dimensions, more than datui reads"
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
    // Tensor data starts at the next alignment multiple after the header, and every sized
    // tensor must end inside the file: a truncated download is an error.
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
                "tensor \"{}\" runs past the end of the file ({data_len} bytes of data)",
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

/// Parse `bytes` as whichever model header it starts with: both parsers over a slice,
/// with no file.
#[cfg(test)]
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
    let named = |e: std::io::Error| crate::error_display::in_file(path, e.into());
    let file = std::fs::File::open(path).map_err(named)?;
    let len = file.metadata().map_err(named)?.len();
    if len > MAX_INDEX_JSON {
        return Err(FileError::new(
            path,
            format!("the index is {len} bytes, more than datui reads"),
        )
        .into());
    }
    let mut text = Vec::new();
    file.take(MAX_INDEX_JSON)
        .read_to_end(&mut text)
        .map_err(named)?;
    let (names, metadata) = parse_index(&text, &path.display().to_string())?;
    let dir = path.parent().unwrap_or(Path::new(""));
    Ok((names.iter().map(|name| dir.join(name)).collect(), metadata))
}

/// An index's shard names, each once and in name order, and its metadata. `named` is
/// what errors call the index.
fn parse_index(text: &[u8], named: &str) -> Result<(Vec<String>, Metadata)> {
    let refused = |what: String| FileError::new(Path::new(named), what);
    let index: StIndex = serde_json::from_slice(text)
        .map_err(|e| refused(format!("not a SafeTensors index: {e}")))?;
    let names: std::collections::BTreeSet<String> = index.weight_map.into_values().collect();
    if names.len() > MAX_SHARDS {
        return Err(refused("the index names too many shards".into()).into());
    }
    for name in &names {
        // A shard is a file beside its index, never a path out of the directory.
        let path = Path::new(name);
        if path.components().count() != 1 || path.file_name().is_none() || name.contains('\\') {
            return Err(refused(format!(
                "the index names \"{name}\", which is not a file beside it"
            ))
            .into());
        }
    }
    let metadata = index.metadata.map(|StMetadata(m)| m).unwrap_or_default();
    Ok((names.into_iter().collect(), metadata))
}

/// Bytes of one remote object, fetched a range at a time: an HTTP server, or an object
/// in a store.
pub trait RangeSource {
    /// Bytes `start..end` of the object, fewer only where the object ends first, and
    /// the object's whole length. `start` is inside the object.
    fn get(&mut self, start: u64, end: u64) -> std::result::Result<(Vec<u8>, u64), RangeError>;
}

/// Why a ranged read stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeError {
    /// The server sent the whole file where a range was asked for: the header cannot be
    /// read on its own, and the file is downloaded instead.
    NoRanges,
    /// Anything else, as the user is told it.
    Failed(String),
}

impl From<color_eyre::Report> for RangeError {
    fn from(e: color_eyre::Report) -> Self {
        RangeError::Failed(e.to_string())
    }
}

/// The first GGUF header read; each next is twice the last up to `MAX_RANGE`, so a few
/// KB costs one request and a 5-10 MB vocabulary four or five (see
/// `a_vocabulary_sized_gguf_header_takes_a_few_ranges`).
pub const FIRST_GGUF_RANGE: u64 = 256 * 1024;
/// The first SafeTensors read: the header length and usually all its JSON (shard
/// headers are KB to tens of KB); longer takes one more request.
pub const FIRST_SAFETENSORS_RANGE: u64 = 64 * 1024;
/// The most one ranged request asks for.
const MAX_RANGE: u64 = 16 * 1024 * 1024;
/// The first read of a remote index; one larger than this takes a second.
const FIRST_INDEX_RANGE: u64 = 1024 * 1024;

/// `start..end` of `src`, checked: exactly the bytes asked for up to the object's end,
/// and the same length the first answer gave, if there was one.
fn fetch(
    src: &mut dyn RangeSource,
    start: u64,
    end: u64,
    known_len: Option<u64>,
) -> std::result::Result<(Vec<u8>, u64), RangeError> {
    let (bytes, len) = src.get(start, end)?;
    if known_len.is_some_and(|known| known != len) {
        return Err(RangeError::Failed(format!(
            "the file changed size while its header was read ({} then {len} bytes)",
            known_len.unwrap_or_default()
        )));
    }
    let want = end.min(len).saturating_sub(start);
    if bytes.len() as u64 != want {
        return Err(RangeError::Failed(format!(
            "asked for bytes {start}..{} and got {} bytes",
            end.min(len),
            bytes.len()
        )));
    }
    Ok((bytes, len))
}

/// A remote object read forward through ranged requests that grow as the read goes on,
/// and never past `limit`. One range is held at a time.
struct Ranged<'a> {
    src: &'a mut dyn RangeSource,
    len: u64,
    limit: u64,
    buf: Vec<u8>,
    buf_start: u64,
    pos: u64,
    next: u64,
    stop: &'a dyn Fn() -> bool,
}

impl Read for Ranged<'_> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.limit || out.is_empty() {
            return Ok(0);
        }
        let buf_end = self.buf_start + self.buf.len() as u64;
        if self.pos < self.buf_start || self.pos >= buf_end {
            if (self.stop)() {
                return Err(std::io::Error::other("cancelled"));
            }
            let end = self.pos.saturating_add(self.next).min(self.limit);
            let (bytes, _) = fetch(self.src, self.pos, end, Some(self.len)).map_err(|e| {
                std::io::Error::other(match e {
                    RangeError::NoRanges => "the server stopped serving byte ranges".to_string(),
                    RangeError::Failed(message) => message,
                })
            })?;
            self.buf = bytes;
            self.buf_start = self.pos;
            self.next = (self.next * 2).min(MAX_RANGE);
        }
        let at = (self.pos - self.buf_start) as usize;
        let n = out.len().min(self.buf.len() - at);
        out[..n].copy_from_slice(&self.buf[at..at + n]);
        self.pos += n as u64;
        Ok(n)
    }
}

/// Read one model header from `src` with ranged requests: SafeTensors' first
/// [`FIRST_SAFETENSORS_RANGE`] plus any remaining JSON; GGUF in growing ranges until the
/// tensor infos end. File-reader bounds all hold; `stop` is checked before each request.
pub fn read_header_ranged(
    src: &mut dyn RangeSource,
    format: FileFormat,
    stop: &dyn Fn() -> bool,
) -> std::result::Result<Header, RangeError> {
    let first = match format {
        FileFormat::Gguf => FIRST_GGUF_RANGE,
        _ => FIRST_SAFETENSORS_RANGE,
    };
    read_header_ranged_from(src, format, first, stop)
}

/// As [`read_header_ranged`] with first range `first` (at least 8 bytes): small for the
/// fuzz target and tests, to cross many ranges.
pub fn read_header_ranged_from(
    src: &mut dyn RangeSource,
    format: FileFormat,
    first: u64,
    stop: &dyn Fn() -> bool,
) -> std::result::Result<Header, RangeError> {
    if format == FileFormat::Gguf {
        let (head, len) = fetch(src, 0, first.max(1), None)?;
        let reader = Ranged {
            src,
            len,
            limit: len.min(MAX_GGUF_HEADER),
            buf: head,
            buf_start: 0,
            pos: 0,
            next: first.max(1).saturating_mul(2).min(MAX_RANGE),
            stop,
        };
        return Ok(read_gguf(reader, len)?);
    }
    let (mut head, len) = fetch(src, 0, first.max(8), None)?;
    let prefix: [u8; 8] = head
        .get(..8)
        .and_then(|prefix| prefix.try_into().ok())
        .ok_or_else(|| eyre!("the file is shorter than its SafeTensors header length"))?;
    let header_len = safetensors_header_len(prefix, len)?;
    let end = 8 + header_len;
    // The first read holds the whole JSON, or the front of it: the rest is asked for
    // once, up to its end and no further.
    if (head.len() as u64) < end {
        if stop() {
            return Err(RangeError::Failed("cancelled".to_string()));
        }
        let rest = fetch(src, head.len() as u64, end, Some(len))?.0;
        head.extend(rest);
    }
    let json = &head[8..end as usize];
    Ok(parse_safetensors_json(json, len - end)?)
}

/// A remote index: its shard names and metadata, read whole within
/// [`MAX_INDEX_JSON`].
fn read_index_ranged(
    src: &mut dyn RangeSource,
    named: &str,
) -> std::result::Result<(Vec<String>, Metadata), RangeError> {
    let (mut text, len) = fetch(src, 0, FIRST_INDEX_RANGE, None)?;
    if len > MAX_INDEX_JSON {
        return Err(RangeError::Failed(crate::error_display::file_message(
            Path::new(named),
            &format!("the index is {len} bytes, more than datui reads"),
        )));
    }
    if len > text.len() as u64 {
        text.extend(fetch(src, text.len() as u64, len, Some(len))?.0);
    }
    Ok(parse_index(&text, named)?)
}

/// A ranged source for a URL.
pub type OpenRanges<'a> =
    dyn Fn(&str) -> std::result::Result<Box<dyn RangeSource>, RangeError> + Sync + 'a;

/// How a remote model's files are reached: a source per URL and sibling file URLs.
/// `open` and `stop` are called from concurrent shard readers ([`SHARD_READS`]).
pub struct Remote<'a> {
    pub open: &'a OpenRanges<'a>,
    pub sibling: &'a dyn Fn(&str, &str) -> String,
    pub stop: &'a (dyn Fn() -> bool + Sync),
}

/// Shard headers read at once: hundreds of shards one at a time add up to minutes; a few
/// at once gets most of the gain without a burst.
pub const SHARD_READS: usize = 8;

/// The last segment of a URL, without a query: what a file's row and its errors call it.
pub fn url_file_name(url: &str) -> &str {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    path.rsplit('/').next().unwrap_or(path)
}

/// Read `urls` (remote model files, or SafeTensors indexes naming them) as one tensor
/// table, fetching only headers, as [`read_model`] does on disk.
/// [`RangeError::NoRanges`] only for one standalone file (downloadable instead).
pub fn read_remote_model(
    urls: &[String],
    format: FileFormat,
    remote: &Remote,
) -> std::result::Result<(LazyFrame, ModelSummary), RangeError> {
    let no_ranges = |url: &str| {
        RangeError::Failed(crate::error_display::file_message(
            Path::new(url),
            "the server does not serve byte ranges, which reading a sharded model's headers needs",
        ))
    };
    // Each failure names the file it came from: a shard, or the index naming them.
    let named = |url: &str, e: RangeError| match e {
        RangeError::Failed(what) => {
            RangeError::Failed(crate::error_display::file_message(Path::new(url), &what))
        }
        e => e,
    };
    let mut files: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut metadata: Metadata = Vec::new();
    for url in urls {
        if format == FileFormat::Safetensors && is_safetensors_index(Path::new(url_file_name(url)))
        {
            let (names, index_meta) = (remote.open)(url)
                .and_then(|mut src| read_index_ranged(src.as_mut(), url))
                .map_err(|e| match e {
                    RangeError::NoRanges => no_ranges(url),
                    e => named(url, e),
                })?;
            merge_metadata(&mut metadata, index_meta);
            for name in names {
                let shard = (remote.sibling)(url, &name);
                if seen.insert(shard.clone()) {
                    files.push(shard);
                }
            }
        } else if seen.insert(url.clone()) {
            files.push(url.clone());
        }
    }
    if files.is_empty() {
        return Err(RangeError::Failed("no model files to read".to_string()));
    }
    // Named on its own, a file the server sends whole is downloaded instead.
    let alone = files.len() == 1 && urls.len() == 1 && files[0] == urls[0];
    let headers = read_headers(&files, format, remote).map_err(|(file, e)| match e {
        RangeError::NoRanges if alone => RangeError::NoRanges,
        RangeError::NoRanges => no_ranges(file),
        e => named(file, e),
    })?;
    let names: Vec<String> = files.iter().map(|f| url_file_name(f).to_string()).collect();
    Ok(build(&headers, &names, metadata)?)
}

/// Each of `files`' headers in order, [`SHARD_READS`] at a time; the first failure is
/// the error, after which (or once stopped) no more requests go out.
fn read_headers<'f>(
    files: &'f [String],
    format: FileFormat,
    remote: &Remote,
) -> std::result::Result<Vec<Header>, (&'f str, RangeError)> {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let first_error: Mutex<Option<(usize, RangeError)>> = Mutex::new(None);
    let read: Vec<Mutex<Option<Header>>> = files.iter().map(|_| Mutex::new(None)).collect();
    let stop = || failed.load(Ordering::Relaxed) || (remote.stop)();
    std::thread::scope(|scope| {
        for _ in 0..SHARD_READS.min(files.len()) {
            scope.spawn(|| {
                loop {
                    let at = next.fetch_add(1, Ordering::Relaxed);
                    if at >= files.len() || stop() {
                        return;
                    }
                    match (remote.open)(&files[at])
                        .and_then(|mut src| read_header_ranged(src.as_mut(), format, &stop))
                    {
                        Ok(header) => {
                            *read[at].lock().unwrap_or_else(|e| e.into_inner()) = Some(header);
                        }
                        Err(e) => {
                            // The others stop at their next request: only the first is
                            // the reason.
                            if !failed.swap(true, Ordering::Relaxed) {
                                *first_error.lock().unwrap_or_else(|e| e.into_inner()) =
                                    Some((at, e));
                            }
                            return;
                        }
                    }
                }
            });
        }
    });
    if let Some((at, e)) = first_error.into_inner().unwrap_or_else(|e| e.into_inner()) {
        return Err((&files[at], e));
    }
    read.into_iter()
        .map(|slot| slot.into_inner().unwrap_or_else(|e| e.into_inner()))
        .collect::<Option<Vec<Header>>>()
        // Stopped before every file was read.
        .ok_or((
            files.first().map_or("", String::as_str),
            RangeError::Failed("cancelled".to_string()),
        ))
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
            _ => crate::error_display::in_file(file, e),
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

/// Each type's share of the parameters, most first: `Q4_K 87% · Q6_K 12% · F32 <1%`.
/// By tensors when no tensor has a parameter count.
fn type_mix(types: &[TypeShare], sep: &str) -> String {
    let by_params = types.iter().any(|t| t.params > 0);
    let total: u64 = if by_params {
        types.iter().map(|t| t.params).fold(0, u64::saturating_add)
    } else {
        types.iter().map(|t| t.tensors as u64).sum()
    };
    types
        .iter()
        .map(|t| {
            let part = if by_params {
                t.params
            } else {
                t.tensors as u64
            };
            let pct = if total == 0 {
                0.0
            } else {
                part as f64 * 100.0 / total as f64
            };
            if pct > 0.0 && pct < 1.0 {
                format!("{} <1%", t.name)
            } else {
                format!("{} {:.0}%", t.name, pct)
            }
        })
        .collect::<Vec<_>>()
        .join(sep)
}

/// The Model tab: the model's totals, then its metadata as key and value, every value
/// whole.
pub fn detail(model: &ModelSummary) -> crate::formats::text_formats::Detail {
    use crate::widgets::info::{count_of, group_u64, short_count};
    let sep = format!(" {} ", crate::glyphs::get().middot);
    let mut head = model.kind.label();
    head.push_str(&sep);
    head.push_str(&count_of(model.tensors as u64, "tensor", "tensors"));
    if model.files > 1 {
        head.push_str(&sep);
        head.push_str(&count_of(model.files as u64, "file", "files"));
    }
    let mut lines = vec![
        head,
        format!(
            "Parameters: {}{}{sep}Size: {}",
            group_u64(model.params),
            // The short form only where it is shorter.
            if model.params >= 1000 {
                format!(" ({})", short_count(model.params))
            } else {
                String::new()
            },
            crate::numfmt::bytes(model.bytes)
        ),
    ];
    if !model.types.is_empty() {
        lines.push(format!("Types: {}", type_mix(&model.types, &sep)));
    }
    crate::formats::text_formats::Detail {
        tab: crate::formats::text_formats::tab(crate::FileFormat::Safetensors),
        lines,
        list_title: "Metadata",
        list: model.metadata.clone(),
        // A model's schema is the same seven columns every time; what is particular
        // to it is here.
        first: true,
        own_columns: true,
        ..Default::default()
    }
}

/// What a model's header says besides its tensors, as the dataset takes it.
pub(crate) fn opened(summary: &ModelSummary) -> crate::formats::members::Opened {
    crate::formats::members::Opened {
        detail: Some(std::sync::Arc::new(detail(summary))),
        ..Default::default()
    }
}

/// The scan of model files: their tensors, with the header's totals and metadata.
fn scan(input: crate::formats::readers::ScanIn<'_>) -> Result<crate::loading::scan::Scan> {
    let (lf, summary) = read_model(input.paths, input.format)?;
    input.report.opened = Some(std::sync::Arc::new(opened(&summary)));
    Ok(lf.into())
}

#[cfg(test)]
pub(crate) mod tests;
