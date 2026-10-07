//! WAV, BWF, RF64 and AIFF audio as a table of sample frames. The file is memory-mapped
//! and a chunk walker finds the format and samples; row `i` is at
//! `data_offset + i * frame_bytes`, so any window decodes from its own bytes (a 20 GB
//! recording reads a screenful). Every stated length is checked against the file
//! before use, so a hostile header errors rather than panics or over-allocates. The row
//! count comes from the file size, letting a still-recording file grow
//! ([`AudioSource::extend`]): recorders write a placeholder data size until they stop.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use color_eyre::Result;
use color_eyre::eyre::eyre;
use memmap2::Mmap;
use polars::prelude::*;

/// What datui does with an audio file: see [`crate::readers`].
pub(crate) const READER: crate::readers::Reader = crate::readers::Reader {
    scan,
    signatures: &[crate::readers::Signature {
        says: |head, _| looks_like_audio(head),
        kind: crate::readers::Kind::Magic,
        trusted: crate::readers::EVERYWHERE,
    }],
    ..crate::readers::BASE
};

/// The most channels a file may declare; each is a column.
const MAX_CHANNELS: u16 = 1024;
/// The most chunks walked. Real files have a handful; each step moves at least 8 bytes,
/// so this bounds a file made of nothing but empty chunks.
const MAX_CHUNKS: usize = 1 << 16;
/// The most markers kept from a `cue ` or `MARK` chunk.
const MAX_MARKERS: usize = 100_000;
/// The longest text kept from one chunk (iXML, coding history, an annotation).
const MAX_TEXT: usize = 1 << 20;

/// The container a file is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Container {
    /// RIFF WAVE; with a `bext` chunk it is a Broadcast WAV.
    Wav,
    /// RF64 or BW64: WAVE with 64-bit sizes in a `ds64` chunk.
    Rf64,
    Aiff,
    /// AIFF-C, which names its sample encoding.
    Aifc,
}

impl Container {
    pub fn label(self) -> &'static str {
        match self {
            Container::Wav => "WAV",
            Container::Rf64 => "RF64",
            Container::Aiff => "AIFF",
            Container::Aifc => "AIFF-C",
        }
    }
}

/// How one sample is stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sample {
    /// Unsigned 8-bit, offset by 128 (WAV, and AIFF-C `raw `). Shown signed.
    U8,
    I8,
    I16,
    I24,
    I32,
    F32,
    F64,
}

impl Sample {
    /// Bytes one sample takes.
    pub fn bytes(self) -> usize {
        match self {
            Sample::U8 | Sample::I8 => 1,
            Sample::I16 => 2,
            Sample::I24 => 3,
            Sample::I32 | Sample::F32 => 4,
            Sample::F64 => 8,
        }
    }

    pub fn is_float(self) -> bool {
        matches!(self, Sample::F32 | Sample::F64)
    }

    /// The column's type. Integers keep an integer type wide enough for the sample
    /// (24-bit is `Int32`); normalized, they are `Float32` in [-1, 1].
    pub fn dtype(self, normalize: bool) -> DataType {
        match self {
            Sample::F32 => DataType::Float32,
            Sample::F64 => DataType::Float64,
            _ if normalize => DataType::Float32,
            Sample::U8 | Sample::I8 => DataType::Int8,
            Sample::I16 => DataType::Int16,
            Sample::I24 | Sample::I32 => DataType::Int32,
        }
    }

    /// The magnitude of full scale: the most negative integer sample, negated, or 1.0
    /// for float.
    pub fn full_scale(self) -> f64 {
        match self {
            Sample::F32 | Sample::F64 => 1.0,
            other => (1u64 << (other.bytes() * 8 - 1)) as f64,
        }
    }

    /// `16-bit integer`, `32-bit float`.
    pub fn label(self) -> String {
        let kind = if self.is_float() { "float" } else { "integer" };
        format!("{}-bit {kind}", self.bytes() * 8)
    }
}

/// A marker or region from a `cue ` (with `LIST adtl` labels) or `MARK` chunk.
#[derive(Debug, Clone, PartialEq)]
pub struct Marker {
    pub id: u32,
    /// The frame it marks.
    pub sample: u64,
    pub label: String,
    /// A region's length in frames, from an `ltxt` chunk.
    pub length: Option<u64>,
}

/// What the header says: the format, where the samples are, and everything else the
/// Info panel shows.
#[derive(Debug, Clone, PartialEq)]
pub struct AudioHeader {
    pub container: Container,
    /// A Broadcast WAV: the file has a `bext` chunk.
    pub broadcast: bool,
    /// `PCM`, `IEEE float`, `extensible PCM`, or an AIFF-C encoding.
    pub encoding: String,
    pub sample: Sample,
    pub big_endian: bool,
    pub channels: u16,
    pub sample_rate: f64,
    /// Bits that carry the signal, when fewer than the container's (extensible, AIFF).
    pub valid_bits: u16,
    /// Bytes per frame: one sample per channel, in channel order, plus any padding.
    pub frame_bytes: usize,
    /// One name per channel: from the extensible channel mask, else `ch1..chN`.
    pub channel_names: Vec<String>,
    /// Where the samples start.
    pub data_offset: u64,
    /// The data size the header states; `None` for 0 or a placeholder, which a
    /// recorder writes until it stops.
    pub data_declared: Option<u64>,
    /// Key and value, in the order found: the format, `bext`, iXML, `LIST INFO`.
    pub metadata: Vec<(String, String)>,
    pub markers: Vec<Marker>,
}

impl AudioHeader {
    /// The data bytes a file of `file_len` holds: what the header declares, cut to the
    /// file, or everything after the data offset when the header does not say.
    pub fn data_len(&self, file_len: u64) -> u64 {
        let available = file_len.saturating_sub(self.data_offset);
        self.data_declared
            .map_or(available, |declared| declared.min(available))
    }

    /// Whole frames in a file of `file_len`. A partial last frame is not one.
    pub fn frames(&self, file_len: u64) -> u64 {
        self.data_len(file_len) / self.frame_bytes as u64
    }

    /// Where frame `frame` sits, in seconds from the start.
    pub fn seconds(&self, frame: u64) -> f64 {
        frame as f64 / self.sample_rate
    }
}

/// Whether the first bytes are a WAV, RF64, BW64 or AIFF file.
pub fn looks_like_audio(head: &[u8]) -> bool {
    head.len() >= 12
        && ((matches!(&head[0..4], b"RIFF" | b"RF64" | b"BW64") && &head[8..12] == b"WAVE")
            || (&head[0..4] == b"FORM" && matches!(&head[8..12], b"AIFF" | b"AIFC")))
}

/// Read the header of a whole file's bytes.
pub fn read_header(bytes: &[u8]) -> Result<AudioHeader> {
    if bytes.len() < 12 {
        return Err(eyre!("Not an audio file: too short for a header"));
    }
    match (&bytes[0..4], &bytes[8..12]) {
        (b"RIFF" | b"RF64" | b"BW64", b"WAVE") => read_wave(bytes),
        (b"FORM", b"AIFF" | b"AIFC") => read_aiff(bytes),
        (b"RIFX", _) => Err(eyre!("Big-endian WAV (RIFX) is not supported")),
        _ => Err(eyre!("Not a WAV or AIFF file")),
    }
}

fn le16(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([b[at], b[at + 1]])
}
fn le32(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}
fn le64(b: &[u8], at: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[at..at + 8]);
    u64::from_le_bytes(a)
}
fn be16(b: &[u8], at: usize) -> u16 {
    u16::from_be_bytes([b[at], b[at + 1]])
}
fn be32(b: &[u8], at: usize) -> u32 {
    u32::from_be_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

/// Text from a fixed field or a chunk: up to the first NUL, cut to [`MAX_TEXT`],
/// control characters other than newline and tab dropped, trimmed.
fn text(raw: &[u8]) -> String {
    let raw = &raw[..raw.len().min(MAX_TEXT)];
    let end = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
    String::from_utf8_lossy(&raw[..end])
        .chars()
        .filter(|c| !c.is_control() || matches!(c, '\n' | '\t'))
        .collect::<String>()
        .replace("\r\n", "\n")
        .trim()
        .to_string()
}

/// The name of each channel the extensible mask's bits name, in bit order.
const SPEAKERS: [&str; 18] = [
    "L", "R", "C", "LFE", "BL", "BR", "FLC", "FRC", "BC", "SL", "SR", "TC", "TFL", "TFC", "TFR",
    "TBL", "TBC", "TBR",
];

/// Column names for `channels` channels: the mask's speakers in order, then `chN` for
/// any the mask does not name.
fn channel_names(channels: u16, mask: u32) -> Vec<String> {
    let mut named = SPEAKERS
        .iter()
        .enumerate()
        .filter(|(bit, _)| mask & (1 << bit) != 0)
        .map(|(_, name)| name.to_string());
    (1..=channels)
        .map(|n| named.next().unwrap_or_else(|| format!("ch{n}")))
        .collect()
}

/// The GUID tail every `KSDATAFORMAT_SUBTYPE_*` shares after its format tag.
const SUBTYPE_TAIL: [u8; 14] = [
    0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x80, 0x00, 0x00, 0xAA, 0x00, 0x38, 0x9B, 0x71,
];

/// What a format tag is called, for a refusal.
fn tag_name(tag: u16) -> String {
    match tag {
        0x0002 => "ADPCM".into(),
        0x0006 => "A-law".into(),
        0x0007 => "mu-law".into(),
        0x0011 => "IMA ADPCM".into(),
        0x0050 | 0x0055 => "MPEG".into(),
        other => format!("format tag 0x{other:04X}"),
    }
}

struct Fmt {
    sample: Sample,
    encoding: String,
    channels: u16,
    rate: u32,
    block_align: u16,
    valid_bits: u16,
    mask: u32,
}

fn read_fmt(body: &[u8]) -> Result<Fmt> {
    if body.len() < 16 {
        return Err(eyre!(
            "WAV fmt chunk is {} bytes; at least 16 needed",
            body.len()
        ));
    }
    let mut tag = le16(body, 0);
    let channels = le16(body, 2);
    let rate = le32(body, 4);
    let block_align = le16(body, 12);
    let bits = le16(body, 14);
    let mut valid_bits = bits;
    let mut mask = 0;
    let mut encoding = String::new();
    if tag == 0xFFFE {
        if body.len() < 40 {
            return Err(eyre!(
                "WAV extensible fmt chunk is {} bytes; 40 needed",
                body.len()
            ));
        }
        valid_bits = le16(body, 18);
        mask = le32(body, 20);
        if body[26..40] != SUBTYPE_TAIL {
            return Err(eyre!("WAV extensible subformat is not PCM or float"));
        }
        tag = le16(body, 24);
        encoding.push_str("extensible ");
    }
    // Plain PCM may hold each sample in a wider container than its bits round to (24
    // bits in 4 bytes); the block alignment says so, and the bits are the high ones,
    // as in an extensible file.
    let container = match block_align.checked_rem(channels) {
        Some(0) if tag == 1 && (9..=32).contains(&bits) => {
            (block_align / channels).clamp(bits.div_ceil(8), 4)
        }
        _ => bits.div_ceil(8),
    };
    let sample = match (tag, container) {
        (1, 1) => Sample::U8,
        (1, 2) => Sample::I16,
        (1, 3) => Sample::I24,
        (1, 4) => Sample::I32,
        (3, 4) if bits == 32 => Sample::F32,
        (3, 8) if bits == 64 => Sample::F64,
        (1 | 3, _) => {
            return Err(eyre!("WAV with {bits}-bit samples is not supported"));
        }
        (other, _) => {
            return Err(eyre!(
                "WAV {} audio is not supported; datui reads PCM and float",
                tag_name(other)
            ));
        }
    };
    encoding.push_str(if tag == 3 { "IEEE float" } else { "PCM" });
    if valid_bits == 0 || valid_bits > bits {
        valid_bits = bits;
    }
    Ok(Fmt {
        sample,
        encoding,
        channels,
        rate,
        block_align,
        valid_bits,
        mask,
    })
}

/// Check the shape every container shares and work out the frame size.
fn frame_bytes(channels: u16, sample: Sample, block_align: Option<u16>) -> Result<usize> {
    if channels == 0 {
        return Err(eyre!("Audio header says 0 channels"));
    }
    if channels > MAX_CHANNELS {
        return Err(eyre!(
            "Audio header says {channels} channels; datui reads up to {MAX_CHANNELS}"
        ));
    }
    let packed = channels as usize * sample.bytes();
    match block_align {
        // Padding after the samples is allowed; a frame too small to hold them is not.
        Some(align) if (align as usize) < packed => Err(eyre!(
            "Audio header's frame size ({align} bytes) cannot hold {channels} channels of {}",
            sample.label()
        )),
        Some(align) => Ok(align as usize),
        None => Ok(packed),
    }
}

fn check_rate(rate: f64) -> Result<f64> {
    if rate.is_finite() && (1.0..=1e9).contains(&rate) {
        Ok(rate)
    } else {
        Err(eyre!("Audio header's sample rate ({rate}) is not usable"))
    }
}

/// Chunks of a RIFF or AIFF file as `(id, body start, stated body length)`, stopping at
/// the file's end, an unfit header, or [`MAX_CHUNKS`]. A body overrunning the end is
/// passed as stated; the caller decides if it is running data or corruption.
fn chunks(bytes: &[u8], big_endian: bool) -> impl Iterator<Item = ([u8; 4], u64, u64)> + '_ {
    let len = bytes.len() as u64;
    let mut pos: u64 = 12;
    let mut seen = 0;
    std::iter::from_fn(move || {
        if seen >= MAX_CHUNKS || pos.checked_add(8)? > len {
            return None;
        }
        seen += 1;
        let at = pos as usize;
        let mut id = [0u8; 4];
        id.copy_from_slice(&bytes[at..at + 4]);
        let size = if big_endian {
            be32(bytes, at + 4)
        } else {
            le32(bytes, at + 4)
        } as u64;
        let body = pos + 8;
        // Chunks are padded to an even length. A size that overflows ends the walk on
        // the next step, past the end.
        pos = body.saturating_add(size).saturating_add(size & 1);
        Some((id, body, size))
    })
}

/// The body of a chunk that must lie inside the file.
fn body_of(bytes: &[u8], start: u64, size: u64) -> Option<&[u8]> {
    let end = start.checked_add(size)?;
    bytes.get(start as usize..usize::try_from(end).ok()?)
}

fn read_wave(bytes: &[u8]) -> Result<AudioHeader> {
    let len = bytes.len() as u64;
    let container = match &bytes[0..4] {
        b"RIFF" => Container::Wav,
        _ => Container::Rf64,
    };
    let mut fmt = None;
    let mut ds64_data = None;
    let mut data: Option<(u64, Option<u64>)> = None;
    let mut metadata = Vec::new();
    let mut broadcast = false;
    let mut cues: Vec<(u32, u64)> = Vec::new();
    let mut labels: Vec<(u32, String)> = Vec::new();
    let mut lengths: Vec<(u32, u64)> = Vec::new();
    for (id, start, size) in chunks(bytes, false) {
        if &id == b"data" {
            let stated = match (container, size) {
                (Container::Rf64, 0xFFFF_FFFF) => ds64_data,
                (_, 0 | 0xFFFF_FFFF) => None,
                (Container::Wav, size) => Some(unwrapped_size(size, len - start)),
                (_, size) => Some(size),
            };
            // A data size of 0 or a placeholder means it runs to the end of the file:
            // nothing after it is a chunk.
            let runs_on = stated.is_none_or(|s| start.saturating_add(s) >= len);
            data = Some((start, stated.filter(|&s| s > 0)));
            if runs_on {
                break;
            }
            continue;
        }
        let Some(body) = body_of(bytes, start, size) else {
            // A chunk that runs past the end: a corrupt or cut-short file. What came
            // before it stands; a format that never arrived is the error below.
            break;
        };
        match &id {
            b"fmt " => fmt = Some(read_fmt(body)?),
            b"ds64" if body.len() >= 24 => ds64_data = Some(le64(body, 8)),
            b"bext" => {
                broadcast = true;
                read_bext(body, &mut metadata);
            }
            b"iXML" => read_ixml(body, &mut metadata),
            b"cue " => read_cue(body, &mut cues),
            b"LIST" if body.len() >= 4 => match &body[0..4] {
                b"adtl" => read_adtl(&body[4..], &mut labels, &mut lengths),
                b"INFO" => read_info(&body[4..], &mut metadata),
                _ => {}
            },
            _ => {}
        }
    }
    let fmt = fmt.ok_or_else(|| eyre!("WAV file has no fmt chunk"))?;
    // The time reference counts samples since midnight; with the rate it is a time of
    // day, which is what a sync to picture needs.
    if let Some((_, value)) = metadata
        .iter_mut()
        .find(|(key, _)| key == "bext.time_reference")
        && let Some(samples) = value
            .strip_suffix(" samples")
            .and_then(|n| n.parse::<u64>().ok())
        && fmt.rate > 0
    {
        let ms = (samples as u128 * 1000 / fmt.rate as u128) as u64;
        *value = format!(
            "{:02}:{:02}:{:02}.{:03} ({samples} samples)",
            ms / 3_600_000,
            ms / 60_000 % 60,
            ms / 1000 % 60,
            ms % 1000
        );
    }
    let (data_offset, data_declared) = data.ok_or_else(|| eyre!("WAV file has no data chunk"))?;
    let frame_bytes = frame_bytes(fmt.channels, fmt.sample, Some(fmt.block_align))?;
    let sample_rate = check_rate(fmt.rate as f64)?;
    let markers = cues
        .into_iter()
        .map(|(id, sample)| Marker {
            id,
            sample,
            label: labels
                .iter()
                .find(|(l, _)| *l == id)
                .map(|(_, s)| s.clone())
                .unwrap_or_default(),
            length: lengths.iter().find(|(l, _)| *l == id).map(|(_, n)| *n),
        })
        .collect();
    Ok(AudioHeader {
        container,
        broadcast,
        encoding: fmt.encoding,
        sample: fmt.sample,
        big_endian: false,
        channels: fmt.channels,
        sample_rate,
        valid_bits: fmt.valid_bits,
        frame_bytes,
        channel_names: channel_names(fmt.channels, fmt.mask),
        data_offset,
        data_declared,
        metadata,
        markers,
    })
}

/// A plain RIFF data size, which is 32 bits: a writer that runs past 4 GiB without
/// switching to RF64 leaves the size modulo 2^32. With more than 4 GiB after the
/// chunk's start, the true size is the largest `stated + k * 2^32` that fits.
fn unwrapped_size(stated: u64, available: u64) -> u64 {
    if available <= u32::MAX as u64 || stated > available {
        return stated;
    }
    stated + ((available - stated) >> 32 << 32)
}

/// The Broadcast WAV fields worth reading, by their fixed offsets.
fn read_bext(body: &[u8], metadata: &mut Vec<(String, String)>) {
    let field = |from: usize, to: usize| body.get(from..to).map(text).unwrap_or_default();
    let mut put = |key: &str, value: String| {
        if !value.is_empty() {
            metadata.push((key.to_string(), value));
        }
    };
    put("bext.description", field(0, 256));
    put("bext.originator", field(256, 288));
    put("bext.originator_reference", field(288, 320));
    let date = field(320, 330);
    let time = field(330, 338);
    put(
        "bext.origination",
        format!("{date} {time}").trim().to_string(),
    );
    if body.len() >= 346 {
        let samples = le32(body, 338) as u64 | ((le32(body, 342) as u64) << 32);
        put("bext.time_reference", format!("{samples} samples"));
    }
    if body.len() >= 348 {
        put("bext.version", le16(body, 346).to_string());
    }
    if body.len() > 602 {
        put("bext.coding_history", field(602, body.len()));
    }
}

/// iXML's common fields by a plain tag search, and the document itself as raw text:
/// datui has no XML parser to spare for it.
fn read_ixml(body: &[u8], metadata: &mut Vec<(String, String)>) {
    let doc = text(body);
    for tag in ["PROJECT", "SCENE", "TAKE", "TAPE", "NOTE"] {
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        if let Some(from) = doc.find(&open).map(|i| i + open.len())
            && let Some(len) = doc[from..].find(&close)
        {
            let value = doc[from..from + len].trim();
            if !value.is_empty() {
                metadata.push((format!("ixml.{}", tag.to_lowercase()), value.to_string()));
            }
        }
    }
    if !doc.is_empty() {
        metadata.push(("ixml".to_string(), doc));
    }
}

fn read_cue(body: &[u8], cues: &mut Vec<(u32, u64)>) {
    if body.len() < 4 {
        return;
    }
    // The count is checked against the points the chunk can hold, not trusted.
    let fits = (body.len() - 4) / 24;
    let count = (le32(body, 0) as usize).min(fits).min(MAX_MARKERS);
    for i in 0..count {
        let at = 4 + i * 24;
        cues.push((le32(body, at), le32(body, at + 20) as u64));
    }
}

/// Sub-chunks of a `LIST`: `(id, body)`, each inside `body`.
fn sub_chunks(body: &[u8]) -> impl Iterator<Item = ([u8; 4], &[u8])> + '_ {
    let mut pos = 0usize;
    std::iter::from_fn(move || {
        let head = body.get(pos..pos.checked_add(8)?)?;
        let mut id = [0u8; 4];
        id.copy_from_slice(&head[0..4]);
        let size = le32(head, 4) as usize;
        let start = pos + 8;
        let sub = body.get(start..start.checked_add(size)?)?;
        pos = start + size + (size & 1);
        Some((id, sub))
    })
}

fn read_adtl(body: &[u8], labels: &mut Vec<(u32, String)>, lengths: &mut Vec<(u32, u64)>) {
    for (id, sub) in sub_chunks(body).take(MAX_MARKERS) {
        if sub.len() < 4 {
            continue;
        }
        let cue = le32(sub, 0);
        match &id {
            b"labl" => labels.push((cue, text(&sub[4..]))),
            b"ltxt" if sub.len() >= 8 => lengths.push((cue, le32(sub, 4) as u64)),
            _ => {}
        }
    }
}

fn read_info(body: &[u8], metadata: &mut Vec<(String, String)>) {
    for (id, sub) in sub_chunks(body).take(256) {
        let key = match &id {
            b"INAM" => "title",
            b"IART" => "artist",
            b"ICMT" => "comment",
            b"ICRD" => "date",
            b"ISFT" => "software",
            b"IENG" => "engineer",
            b"ICOP" => "copyright",
            b"IPRD" => "product",
            b"IGNR" => "genre",
            _ => continue,
        };
        let value = text(sub);
        if !value.is_empty() {
            metadata.push((format!("info.{key}"), value));
        }
    }
}

/// An IEEE 754 80-bit extended float, as AIFF stores its sample rate.
fn extended_to_f64(b: &[u8]) -> f64 {
    let sign_exp = u16::from_be_bytes([b[0], b[1]]);
    let mut m = [0u8; 8];
    m.copy_from_slice(&b[2..10]);
    let mantissa = u64::from_be_bytes(m);
    if mantissa == 0 {
        return 0.0;
    }
    let exponent = (sign_exp & 0x7FFF) as i32 - 16383 - 63;
    let value = mantissa as f64 * 2f64.powi(exponent);
    if sign_exp & 0x8000 != 0 {
        -value
    } else {
        value
    }
}

/// An AIFF Pascal string: a count byte then the text, padded to an even length.
/// Returns the text and the bytes taken.
fn pstring(b: &[u8]) -> Option<(String, usize)> {
    let n = *b.first()? as usize;
    let raw = b.get(1..1 + n)?;
    let taken = 1 + n + ((1 + n) & 1);
    Some((text(raw), taken))
}

fn read_aiff(bytes: &[u8]) -> Result<AudioHeader> {
    let len = bytes.len() as u64;
    let aifc = &bytes[8..12] == b"AIFC";
    let mut comm = None;
    let mut data: Option<(u64, Option<u64>)> = None;
    let mut metadata = Vec::new();
    let mut markers = Vec::new();
    for (id, start, size) in chunks(bytes, true) {
        if &id == b"SSND" {
            let Some(head) = body_of(bytes, start, 8) else {
                break;
            };
            let offset = be32(head, 0) as u64;
            let data_start = start + 8 + offset;
            // 0 is a recorder's placeholder: the data runs to the end of the file.
            let stated = (size > 0)
                .then(|| size.checked_sub(8 + offset))
                .flatten()
                .filter(|&s| s > 0);
            let runs_on = stated.is_none_or(|s| data_start.saturating_add(s) >= len);
            data = Some((data_start, stated));
            if runs_on {
                break;
            }
            continue;
        }
        let Some(body) = body_of(bytes, start, size) else {
            break;
        };
        match &id {
            b"COMM" => comm = Some(read_comm(body, aifc)?),
            b"MARK" if body.len() >= 2 => {
                let count = (be16(body, 0) as usize).min(MAX_MARKERS);
                let mut at = 2;
                for _ in 0..count {
                    let Some(head) = body.get(at..at + 6) else {
                        break;
                    };
                    let id = be16(head, 0) as u32;
                    let sample = be32(head, 2) as u64;
                    let Some((label, taken)) = body.get(at + 6..).and_then(pstring) else {
                        break;
                    };
                    markers.push(Marker {
                        id,
                        sample,
                        label,
                        length: None,
                    });
                    at += 6 + taken;
                }
            }
            b"NAME" | b"AUTH" | b"(c) " | b"ANNO" => {
                let key = match &id {
                    b"NAME" => "name",
                    b"AUTH" => "author",
                    b"(c) " => "copyright",
                    _ => "annotation",
                };
                let value = text(body);
                if !value.is_empty() {
                    metadata.push((key.to_string(), value));
                }
            }
            _ => {}
        }
    }
    let comm = comm.ok_or_else(|| eyre!("AIFF file has no COMM chunk"))?;
    let (data_offset, mut data_declared) =
        data.ok_or_else(|| eyre!("AIFF file has no SSND chunk"))?;
    let frame_bytes = frame_bytes(comm.channels, comm.sample, None)?;
    // COMM's frame count is the authority when it is set; 0 is a recorder's placeholder.
    if comm.frames > 0 {
        let by_comm = comm.frames.saturating_mul(frame_bytes as u64);
        data_declared = Some(data_declared.map_or(by_comm, |d| d.min(by_comm)));
    }
    Ok(AudioHeader {
        container: if aifc {
            Container::Aifc
        } else {
            Container::Aiff
        },
        broadcast: false,
        encoding: comm.encoding,
        sample: comm.sample,
        big_endian: comm.big_endian,
        channels: comm.channels,
        sample_rate: comm.rate,
        valid_bits: comm.valid_bits,
        frame_bytes,
        channel_names: channel_names(comm.channels, 0),
        data_offset,
        data_declared,
        metadata,
        markers,
    })
}

struct Comm {
    channels: u16,
    frames: u64,
    sample: Sample,
    big_endian: bool,
    rate: f64,
    valid_bits: u16,
    encoding: String,
}

fn read_comm(body: &[u8], aifc: bool) -> Result<Comm> {
    if body.len() < 18 {
        return Err(eyre!("AIFF COMM chunk is {} bytes; 18 needed", body.len()));
    }
    let channels = be16(body, 0);
    let frames = be32(body, 2) as u64;
    let bits = be16(body, 6);
    let rate = check_rate(extended_to_f64(&body[8..18]))?;
    let code: [u8; 4] = match body.get(18..22) {
        Some(c) if aifc => [c[0], c[1], c[2], c[3]],
        _ => *b"NONE",
    };
    let int = |bits: u16| match bits.div_ceil(8) {
        1 => Some(Sample::I8),
        2 => Some(Sample::I16),
        3 => Some(Sample::I24),
        4 => Some(Sample::I32),
        _ => None,
    };
    let (sample, big_endian) = match &code {
        b"NONE" | b"twos" => (int(bits), true),
        b"sowt" => (int(bits), false),
        b"raw " if bits <= 8 => (Some(Sample::U8), true),
        b"in24" => (Some(Sample::I24), true),
        b"in32" => (Some(Sample::I32), true),
        b"23ni" => (Some(Sample::I24), false),
        b"fl32" | b"FL32" => (Some(Sample::F32), true),
        b"fl64" | b"FL64" => (Some(Sample::F64), true),
        other => {
            return Err(eyre!(
                "AIFF-C compression \"{}\" is not supported; datui reads uncompressed audio",
                String::from_utf8_lossy(other)
            ));
        }
    };
    let sample = sample.ok_or_else(|| eyre!("AIFF with {bits}-bit samples is not supported"))?;
    let encoding = match &code {
        b"NONE" | b"twos" => "PCM".to_string(),
        b"sowt" => "PCM, little-endian".to_string(),
        b"fl32" | b"FL32" | b"fl64" | b"FL64" => "IEEE float".to_string(),
        other => format!("PCM ({})", String::from_utf8_lossy(other).trim()),
    };
    let container_bits = (sample.bytes() * 8) as u16;
    let valid_bits = if sample.is_float() || bits == 0 || bits > container_bits {
        container_bits
    } else {
        bits
    };
    Ok(Comm {
        channels,
        frames,
        sample,
        big_endian,
        rate,
        valid_bits,
        encoding,
    })
}

/// An open audio file: its header and a map of its bytes, read a window at a time.
pub struct AudioSource {
    /// The file the map is of, for [`Self::extend`]; `None` for bytes given whole.
    file: Option<File>,
    map: Mmap,
    header: AudioHeader,
    frames: u64,
    /// Integer samples as `Float32` in [-1, 1].
    normalize: bool,
}

// The in-memory engine builds the plan's frame index from row 0 up to a slice's end, so
// the window at the end of a long recording would cost 4 bytes per frame before it.
impl crate::pushdown::Windowed for AudioSource {
    fn window(&self, start: usize, len: usize) -> PolarsResult<LazyFrame> {
        Ok(AudioSource::window(self, start as u64, len as u64, None)?.lazy())
    }
}

impl std::fmt::Debug for AudioSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AudioSource")
            .field("header", &self.header)
            .field("frames", &self.frames)
            .field("normalize", &self.normalize)
            .finish()
    }
}

/// The name of the frame-number column.
pub const FRAME: &str = "frame";
/// The name of the column of each frame's time from the start, in seconds. A float
/// rather than a Duration: it charts, compares and reads as a plain number
/// (`12.345625`), where a Duration displays as mixed units.
pub const SECONDS: &str = "seconds";
/// The most frames shown: Polars counts rows in 32 bits.
const MAX_FRAMES: u64 = crate::row_index::MAX_ROWS as u64;

impl AudioSource {
    pub fn open(path: &Path, normalize: bool) -> Result<Self> {
        let file = File::open(path)?;
        // SAFETY: the map is read-only and every read goes through a bounds-checked
        // slice of it. A file truncated underneath an open map is the one thing this
        // cannot guard; it is the same exposure Polars' own mapped readers have.
        let map = unsafe { Mmap::map(&file)? };
        let header = read_header(&map)?;
        let frames = header.frames(map.len() as u64).min(MAX_FRAMES);
        Ok(Self {
            file: Some(file),
            map,
            header,
            frames,
            normalize,
        })
    }

    /// An audio file from its bytes, copied into an anonymous map: for the fuzz target,
    /// which has bytes and no file.
    pub fn from_bytes(bytes: &[u8], normalize: bool) -> Result<Self> {
        let header = read_header(bytes)?;
        let mut copy = memmap2::MmapMut::map_anon(bytes.len())?;
        copy.copy_from_slice(bytes);
        let map = copy.make_read_only()?;
        let frames = header.frames(map.len() as u64).min(MAX_FRAMES);
        Ok(Self {
            file: None,
            map,
            header,
            frames,
            normalize,
        })
    }

    pub fn header(&self) -> &AudioHeader {
        &self.header
    }

    pub fn frames(&self) -> u64 {
        self.frames
    }

    pub fn normalize(&self) -> bool {
        self.normalize
    }

    /// Fails when the file is shorter than its map: reading past it ends the process
    /// (SIGBUS), as a recording rewritten or cut while open would. Checked before each read.
    fn still_whole(&self) -> PolarsResult<()> {
        if let Some(file) = &self.file {
            let len = file.metadata()?.len();
            polars_ensure!(
                len >= self.map.len() as u64,
                ComputeError: "the file is now {len} bytes, shorter than the {} it had when it was opened; open it again",
                self.map.len()
            );
        }
        Ok(())
    }

    /// Duration of the frames on hand, in seconds.
    pub fn seconds(&self) -> f64 {
        self.header.seconds(self.frames)
    }

    /// Frames the file holds past the most datui shows, `MAX_FRAMES`.
    pub fn frames_past_limit(&self) -> u64 {
        self.header
            .frames(self.map.len() as u64)
            .saturating_sub(self.frames)
    }

    /// Bytes in the data region that are not a whole frame: a cut-short last frame.
    pub fn trailing_bytes(&self) -> u64 {
        self.header.data_len(self.map.len() as u64) % self.header.frame_bytes as u64
    }

    /// Whether the header states more data than the file holds.
    pub fn cut_short(&self) -> Option<(u64, u64)> {
        let declared = self.header.data_declared?;
        let held = self.header.data_len(self.map.len() as u64);
        (declared > held).then_some((declared, held))
    }

    pub fn schema(&self) -> Schema {
        let mut schema = Schema::with_capacity(self.header.channels as usize + 2);
        schema.insert(FRAME.into(), DataType::Int64);
        schema.insert(SECONDS.into(), DataType::Float64);
        let dtype = self.header.sample.dtype(self.normalize);
        for name in &self.header.channel_names {
            schema.insert(name.as_str().into(), dtype.clone());
        }
        schema
    }

    /// Frames `[start, start + len)` (cut to the frames on hand) as `columns`, in that
    /// order, or every column. Only those frames' bytes are read.
    pub fn window(
        &self,
        start: u64,
        len: u64,
        columns: Option<&[PlSmallStr]>,
    ) -> PolarsResult<DataFrame> {
        self.still_whole()?;
        let start = start.min(self.frames);
        let n = len.min(self.frames - start);
        let schema = self.schema();
        let all: Vec<PlSmallStr>;
        let columns = match columns {
            Some(c) => c,
            None => {
                all = schema.iter_names().cloned().collect();
                &all
            }
        };
        let mut out = Vec::with_capacity(columns.len());
        for name in columns {
            let Some(which) = self.which(name) else {
                polars_bail!(ColumnNotFound: "{name}");
            };
            out.push(self.decode(name.clone(), which, (start..start + n).map(Some)));
        }
        DataFrame::new(n as usize, out)
    }

    /// What a column of [`Self::schema`] is.
    fn which(&self, name: &str) -> Option<Which> {
        match name {
            FRAME => Some(Which::Frame),
            SECONDS => Some(Which::Seconds),
            other => self
                .header
                .channel_names
                .iter()
                .position(|c| c == other)
                .map(Which::Channel),
        }
    }

    /// The bytes of one sample, or `None` for a frame past the ones on hand.
    fn sample_at(&self, frame: u64, channel: usize) -> Option<&[u8]> {
        if frame >= self.frames {
            return None;
        }
        let h = &self.header;
        let width = h.sample.bytes();
        let at = frame
            .checked_mul(h.frame_bytes as u64)?
            .checked_add(h.data_offset)?
            .checked_add((channel * width) as u64)?;
        let at = usize::try_from(at).ok()?;
        self.map.get(at..at.checked_add(width)?)
    }

    /// One column for the frames `frames` names, in that order; a `None` frame, or one
    /// past the frames on hand, is null.
    fn decode(
        &self,
        name: PlSmallStr,
        which: Which,
        frames: impl Iterator<Item = Option<u64>>,
    ) -> Column {
        let channel = match which {
            Which::Frame => {
                return Int64Chunked::from_iter_options(name, frames.map(|f| f.map(|f| f as i64)))
                    .into_column();
            }
            Which::Seconds => {
                return Float64Chunked::from_iter_options(
                    name,
                    frames.map(|f| f.map(|f| self.header.seconds(f))),
                )
                .into_column();
            }
            Which::Channel(c) => c,
        };
        let h = &self.header;
        let be = h.big_endian;
        let scale = 1.0 / h.sample.full_scale() as f32;
        let bytes = frames.map(move |f| f.and_then(|f| self.sample_at(f, channel)));
        let sample = h.sample;
        macro_rules! int_column {
            ($chunked:ty, $t:ty) => {{
                if self.normalize {
                    Float32Chunked::from_iter_options(
                        name,
                        bytes.map(|b| b.map(|b| int_sample(sample, be, b) as f32 * scale)),
                    )
                    .into_column()
                } else {
                    <$chunked>::from_iter_options(
                        name,
                        bytes.map(|b| b.map(|b| int_sample(sample, be, b) as $t)),
                    )
                    .into_column()
                }
            }};
        }
        match sample {
            Sample::U8 | Sample::I8 => int_column!(Int8Chunked, i8),
            Sample::I16 => int_column!(Int16Chunked, i16),
            Sample::I24 | Sample::I32 => int_column!(Int32Chunked, i32),
            Sample::F32 => {
                Float32Chunked::from_iter_options(name, bytes.map(|b| b.map(|b| f32_sample(be, b))))
                    .into_column()
            }
            Sample::F64 => {
                Float64Chunked::from_iter_options(name, bytes.map(|b| b.map(|b| f64_sample(be, b))))
                    .into_column()
            }
        }
    }

    /// The frames as a lazy frame that Polars can stream, slice and prune: decoded
    /// over a frame index ([`crate::row_index`]), so a slice anywhere decodes only its
    /// own frames and a query that names one channel decodes only that channel.
    pub fn lazy(self: &Arc<Self>) -> LazyFrame {
        crate::row_index::lazy(self)
    }

    /// The lowest and highest value a channel's column can hold, in the column's own
    /// units: the most negative and most positive integer the valid bits allow, or
    /// [-1, 1] for float. A sample at either is at full scale.
    pub fn full_scale_bounds(&self) -> (f64, f64) {
        let h = &self.header;
        if h.sample.is_float() {
            return (-1.0, 1.0);
        }
        let container = (h.sample.bytes() * 8) as u32;
        let valid = (h.valid_bits as u32).clamp(1, container);
        // Valid bits are the high ones: a 20-bit sample in a 24-bit container steps by 16.
        let step = (1u64 << (container - valid)) as f64;
        let low = -h.sample.full_scale();
        let high = h.sample.full_scale() - step;
        if self.normalize {
            let scale = h.sample.full_scale();
            (low / scale, (high / scale) as f32 as f64)
        } else {
            (low, high)
        }
    }

    /// One pass over every frame, per channel, measuring clipping (runs at full scale),
    /// dropouts (runs of exact zeros) and DC offset (the mean), in flat memory. `stop` is
    /// checked every million frames; `None` when stopped.
    pub fn signal_report(
        &self,
        stop: &dyn Fn() -> bool,
    ) -> PolarsResult<Option<Vec<SignalReport>>> {
        let h = &self.header;
        let channels = h.channels as usize;
        let (low, high) = self.full_scale_bounds();
        let scale = 1.0 / h.sample.full_scale() as f32;
        let zero_run = (h.sample_rate / 100.0).round().max(16.0) as u64;
        let mut reports: Vec<SignalReport> = h
            .channel_names
            .iter()
            .map(|name| SignalReport {
                channel: name.clone(),
                frames: self.frames,
                full_scale: (low, high),
                clip_run_min: CLIP_RUN,
                zero_run_min: zero_run,
                ..SignalReport::default()
            })
            .collect();
        let mut clip_len = vec![0u64; channels];
        let mut zero_len = vec![0u64; channels];
        let mut sums = vec![0f64; channels];
        // Each sample as the column holds it, so the bounds and the drill-in's
        // predicate agree with the count: normalized, through f32 as `decode` makes it.
        let decode = |b: &[u8]| -> f64 {
            let be = h.big_endian;
            match h.sample {
                Sample::F32 => f32_sample(be, b) as f64,
                Sample::F64 => f64_sample(be, b),
                int if self.normalize => (int_sample(int, be, b) as f32 * scale) as f64,
                int => int_sample(int, be, b) as f64,
            }
        };
        let end_run =
            |len: &mut u64, min: u64, runs: &mut u64, within: &mut u64, longest: &mut u64| {
                if *len >= min {
                    *runs += 1;
                    *within += *len;
                    *longest = (*longest).max(*len);
                }
                *len = 0;
            };
        for frame in 0..self.frames {
            if frame % (1 << 20) == 0 {
                if stop() {
                    return Ok(None);
                }
                self.still_whole()?;
            }
            for c in 0..channels {
                let Some(bytes) = self.sample_at(frame, c) else {
                    continue;
                };
                let v = decode(bytes);
                let r = &mut reports[c];
                if !v.is_finite() {
                    continue;
                }
                sums[c] += v;
                if v <= low || v >= high {
                    r.at_full_scale += 1;
                    clip_len[c] += 1;
                } else {
                    end_run(
                        &mut clip_len[c],
                        CLIP_RUN,
                        &mut r.clip_runs,
                        &mut r.in_clip_runs,
                        &mut r.longest_clip,
                    );
                }
                if v == 0.0 {
                    zero_len[c] += 1;
                } else {
                    end_run(
                        &mut zero_len[c],
                        zero_run,
                        &mut r.zero_runs,
                        &mut r.in_zero_runs,
                        &mut r.longest_zeros,
                    );
                }
            }
        }
        for (c, r) in reports.iter_mut().enumerate() {
            end_run(
                &mut clip_len[c],
                CLIP_RUN,
                &mut r.clip_runs,
                &mut r.in_clip_runs,
                &mut r.longest_clip,
            );
            end_run(
                &mut zero_len[c],
                zero_run,
                &mut r.zero_runs,
                &mut r.in_zero_runs,
                &mut r.longest_zeros,
            );
            if self.frames > 0 {
                r.mean = sums[c] / self.frames as f64;
            }
        }
        Ok(Some(reports))
    }
}

impl crate::row_index::RowSource for AudioSource {
    fn height(&self) -> usize {
        self.frames as usize
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(AudioSource::schema(self))
    }

    fn decode(&self, column: usize, index: &IdxCa) -> PolarsResult<Column> {
        self.still_whole()?;
        let which = match column {
            0 => Which::Frame,
            1 => Which::Seconds,
            c => Which::Channel(c - 2),
        };
        let name = self.schema().get_at_index(column).map(|(n, _)| n.clone());
        let name = name.ok_or_else(|| polars_err!(ColumnNotFound: "column {column}"))?;
        Ok(AudioSource::decode(
            self,
            name,
            which,
            index.iter().map(|f| f.map(u64::from)),
        ))
    }
}

/// An integer sample's value from its bytes. 8-bit WAV is unsigned around 128 and is
/// shown signed; 24-bit is sign-extended. Float samples are not integers: 0.
fn int_sample(sample: Sample, be: bool, b: &[u8]) -> i32 {
    match sample {
        Sample::U8 => (b[0] ^ 0x80) as i8 as i32,
        Sample::I8 => b[0] as i8 as i32,
        Sample::I16 => {
            let a = [b[0], b[1]];
            (if be {
                i16::from_be_bytes(a)
            } else {
                i16::from_le_bytes(a)
            }) as i32
        }
        Sample::I24 => {
            let a = if be {
                [b[2], b[1], b[0], 0]
            } else {
                [b[0], b[1], b[2], 0]
            };
            // Sign-extended by shifting the top byte into place and back.
            i32::from_le_bytes(a) << 8 >> 8
        }
        Sample::I32 => {
            let a = [b[0], b[1], b[2], b[3]];
            if be {
                i32::from_be_bytes(a)
            } else {
                i32::from_le_bytes(a)
            }
        }
        Sample::F32 | Sample::F64 => 0,
    }
}

fn f32_sample(be: bool, b: &[u8]) -> f32 {
    let a = [b[0], b[1], b[2], b[3]];
    if be {
        f32::from_be_bytes(a)
    } else {
        f32::from_le_bytes(a)
    }
}

fn f64_sample(be: bool, b: &[u8]) -> f64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[..8]);
    if be {
        f64::from_be_bytes(a)
    } else {
        f64::from_le_bytes(a)
    }
}

/// The shortest run of samples at full scale that counts as clipping. One sample
/// there is a loud peak; three in a row is the waveform flattened against the limit.
pub const CLIP_RUN: u64 = 3;

/// What [`AudioSource::signal_report`] measured of one channel.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SignalReport {
    pub channel: String,
    pub frames: u64,
    /// The values at or past which a sample is at full scale, in the column's units.
    pub full_scale: (f64, f64),
    /// Samples at full scale, in runs or not.
    pub at_full_scale: u64,
    /// The shortest run counted, and the runs of at least that many samples at full
    /// scale, the samples in them, and the longest.
    pub clip_run_min: u64,
    pub clip_runs: u64,
    pub in_clip_runs: u64,
    pub longest_clip: u64,
    /// The shortest run of exact zeros counted (10 ms, and at least 16 samples), and
    /// the runs that long, the samples in them, and the longest.
    pub zero_run_min: u64,
    pub zero_runs: u64,
    pub in_zero_runs: u64,
    pub longest_zeros: u64,
    /// The channel's mean, in the column's units.
    pub mean: f64,
}

/// A column of [`AudioSource::schema`].
#[derive(Debug, Clone, Copy)]
enum Which {
    Frame,
    Seconds,
    Channel(usize),
}

/// The recording a window of the dataset reads its rows from, when the dataset is
/// one: for the quality checks that read every sample (clipping, runs of zeros, DC
/// offset).
pub(crate) fn recording(window: Arc<dyn crate::pushdown::Windowed>) -> Option<Arc<AudioSource>> {
    let any: Arc<dyn std::any::Any + Send + Sync> = window;
    any.downcast::<AudioSource>().ok()
}

/// The Audio tab: the file's format, size and length, then its metadata (`bext`, iXML,
/// `LIST INFO`, AIFF text) and markers, markers last.
pub fn detail(audio: &AudioSource) -> crate::text_formats::Detail {
    use crate::model_files::MetaValue;
    use crate::widgets::info::{clock, count_of, group_u64};
    let h = audio.header();
    let g = crate::glyphs::get();
    let sep = format!(" {} ", g.middot);
    let mut kind = h.container.label().to_string();
    if h.broadcast {
        kind.push_str(" (Broadcast WAV)");
    }
    let rate = if h.sample_rate.fract() == 0.0 {
        group_u64(h.sample_rate as u64)
    } else {
        format!("{:.3}", h.sample_rate)
    };
    let mut samples = h.sample.label();
    if !h.sample.is_float() && (h.valid_bits as usize) < h.sample.bytes() * 8 {
        samples.push_str(&format!(" ({} valid)", h.valid_bits));
    }
    let mut lines = vec![
        format!(
            "{kind}{sep}{}{sep}{rate} Hz",
            count_of(h.channels as u64, "channel", "channels"),
        ),
        format!(
            "Samples: {samples}{sep}{}{}",
            h.encoding,
            if audio.normalize() && !h.sample.is_float() {
                format!("{sep}shown as float in [-1, 1]")
            } else {
                String::new()
            }
        ),
        format!(
            "Frames: {}{sep}Length: {}",
            group_u64(audio.frames()),
            clock(audio.seconds()),
        ),
        format!(
            "Data: {}",
            crate::numfmt::bytes(audio.frames() * h.frame_bytes as u64)
        ),
    ];
    let mut warnings = Vec::new();
    if let Some((declared, held)) = audio.cut_short() {
        warnings.push(format!(
            "header says {} of samples{sep}file holds {}",
            crate::numfmt::bytes(declared),
            crate::numfmt::bytes(held)
        ));
    } else if h.data_declared.is_none() && audio.frames() > 0 {
        lines.push(format!(
            "no data size in header{sep}frames counted from file size"
        ));
    }
    let past = audio.frames_past_limit();
    if past > 0 {
        warnings.push(format!(
            "last {} frames not shown: past the table limit",
            group_u64(past)
        ));
    }
    let trailing = audio.trailing_bytes();
    if trailing > 0 {
        warnings.push(format!(
            "{trailing} bytes after the last whole frame not shown"
        ));
    }
    let mut metadata: crate::model_files::Metadata = h
        .metadata
        .iter()
        .map(|(k, v)| (k.clone(), MetaValue::Text(v.clone())))
        .collect();
    for marker in &h.markers {
        let mut value = format!(
            "{}{sep}frame {}",
            clock(marker.sample as f64 / h.sample_rate),
            group_u64(marker.sample)
        );
        if let Some(length) = marker.length {
            value.push_str(&format!(
                "{sep}{} long",
                clock(length as f64 / h.sample_rate)
            ));
        }
        if !marker.label.is_empty() {
            value.push_str(&sep);
            value.push_str(&marker.label);
        }
        metadata.push((format!("marker {}", marker.id), MetaValue::Text(value)));
    }
    crate::text_formats::Detail {
        tab: crate::text_formats::tab(crate::FileFormat::Audio),
        lines,
        warnings,
        list_title: "Metadata",
        list: metadata,
        // An audio file's columns are the frame, the time and one per channel; what is
        // particular to it is here.
        first: true,
        own_columns: true,
        ..Default::default()
    }
}

/// The scan of an audio file: its frames, read from the file where they are shown.
/// The source is the window the dataset reads them through, and what a full quality
/// run checks the signal of ([`recording`]).
fn scan(input: crate::readers::ScanIn<'_>) -> Result<crate::scan::Scan> {
    let source = Arc::new(AudioSource::open(input.path(), input.options.normalize)?);
    let lf = source.lazy();
    // The count is arithmetic on the file's size: nothing to scan for it.
    let rows = usize::try_from(source.frames()).unwrap_or(usize::MAX);
    input.report.opened = Some(Arc::new(crate::members::Opened {
        detail: Some(Arc::new(detail(&source))),
        window: Some((source, rows)),
        ..Default::default()
    }));
    Ok(lf.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn chunk(id: &[u8; 4], body: &[u8]) -> Vec<u8> {
        let mut out = id.to_vec();
        out.extend_from_slice(&(body.len() as u32).to_le_bytes());
        out.extend_from_slice(body);
        if body.len() % 2 == 1 {
            out.push(0);
        }
        out
    }

    fn fmt(tag: u16, channels: u16, rate: u32, bits: u16) -> Vec<u8> {
        let align = channels * bits.div_ceil(8);
        let mut b = Vec::new();
        b.extend_from_slice(&tag.to_le_bytes());
        b.extend_from_slice(&channels.to_le_bytes());
        b.extend_from_slice(&rate.to_le_bytes());
        b.extend_from_slice(&(rate * align as u32).to_le_bytes());
        b.extend_from_slice(&align.to_le_bytes());
        b.extend_from_slice(&bits.to_le_bytes());
        b
    }

    fn wav(chunks: &[Vec<u8>]) -> Vec<u8> {
        let body: Vec<u8> = chunks.concat();
        let mut out = b"RIFF".to_vec();
        out.extend_from_slice(&(4 + body.len() as u32).to_le_bytes());
        out.extend_from_slice(b"WAVE");
        out.extend_from_slice(&body);
        out
    }

    fn i16s(values: &[i16]) -> Vec<u8> {
        values.iter().flat_map(|v| v.to_le_bytes()).collect()
    }

    fn open(bytes: &[u8], normalize: bool) -> (tempfile::NamedTempFile, AudioSource) {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(bytes).unwrap();
        file.flush().unwrap();
        let source = AudioSource::open(file.path(), normalize).unwrap();
        (file, source)
    }

    fn ints(df: &DataFrame, name: &str) -> Vec<i64> {
        df.column(name)
            .unwrap()
            .cast(&DataType::Int64)
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect()
    }

    #[test]
    fn a_stereo_wav_is_frames_time_and_a_column_per_channel() {
        let samples = i16s(&[0, 1, -32768, 32767, 100, -100]);
        let bytes = wav(&[chunk(b"fmt ", &fmt(1, 2, 4, 16)), chunk(b"data", &samples)]);
        let (_f, source) = open(&bytes, false);
        assert_eq!(source.frames(), 3);
        let df = source.window(0, 10, None).unwrap();
        assert_eq!(
            df.get_column_names(),
            ["frame", "seconds", "ch1", "ch2"],
            "plain PCM names its channels by number"
        );
        assert_eq!(df.column("ch1").unwrap().dtype(), &DataType::Int16);
        assert_eq!(ints(&df, "ch1"), [0, -32768, 100]);
        assert_eq!(ints(&df, "ch2"), [1, 32767, -100]);
        // Four frames a second: a quarter second apart.
        let seconds: Vec<f64> = df
            .column("seconds")
            .unwrap()
            .f64()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(seconds, [0.0, 0.25, 0.5]);

        // A window deep in the file reads only its own frames, in the order asked.
        let tail = source
            .window(2, 5, Some(&["ch2".into(), "frame".into()]))
            .unwrap();
        assert_eq!(tail.get_column_names(), ["ch2", "frame"]);
        assert_eq!(
            (ints(&tail, "frame"), ints(&tail, "ch2")),
            (vec![2], vec![-100])
        );
    }

    #[test]
    fn normalized_integers_are_float_in_unit_range() {
        let samples = i16s(&[-32768, 16384]);
        let bytes = wav(&[
            chunk(b"fmt ", &fmt(1, 1, 8000, 16)),
            chunk(b"data", &samples),
        ]);
        let (_f, source) = open(&bytes, true);
        let df = source.window(0, 2, None).unwrap();
        let ch: Vec<f32> = df
            .column("ch1")
            .unwrap()
            .f32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(ch, [-1.0, 0.5]);
    }

    #[test]
    fn every_sample_width_decodes() {
        // 8-bit WAV is unsigned around 128, shown signed.
        let bytes = wav(&[
            chunk(b"fmt ", &fmt(1, 1, 8000, 8)),
            chunk(b"data", &[0, 128, 255]),
        ]);
        let (_f, s) = open(&bytes, false);
        assert_eq!(ints(&s.window(0, 3, None).unwrap(), "ch1"), [-128, 0, 127]);

        // 24-bit little-endian, sign-extended.
        let data = [0x00, 0x00, 0x80, 0xFF, 0xFF, 0x7F, 0xFF, 0xFF, 0xFF];
        let bytes = wav(&[chunk(b"fmt ", &fmt(1, 1, 8000, 24)), chunk(b"data", &data)]);
        let (_f, s) = open(&bytes, false);
        let df = s.window(0, 3, None).unwrap();
        assert_eq!(df.column("ch1").unwrap().dtype(), &DataType::Int32);
        assert_eq!(ints(&df, "ch1"), [-8_388_608, 8_388_607, -1]);

        // 32-bit float and 64-bit float.
        let data: Vec<u8> = [0.5f32, -1.0]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let bytes = wav(&[chunk(b"fmt ", &fmt(3, 1, 8000, 32)), chunk(b"data", &data)]);
        let (_f, s) = open(&bytes, true);
        let df = s.window(0, 2, None).unwrap();
        let v: Vec<f32> = df
            .column("ch1")
            .unwrap()
            .f32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(v, [0.5, -1.0], "float is never rescaled");
        let data: Vec<u8> = [0.25f64].iter().flat_map(|v| v.to_le_bytes()).collect();
        let bytes = wav(&[chunk(b"fmt ", &fmt(3, 1, 8000, 64)), chunk(b"data", &data)]);
        let (_f, s) = open(&bytes, false);
        assert_eq!(s.schema().get("ch1"), Some(&DataType::Float64));

        // 32-bit integer.
        let data: Vec<u8> = [i32::MIN, 7].iter().flat_map(|v| v.to_le_bytes()).collect();
        let bytes = wav(&[chunk(b"fmt ", &fmt(1, 1, 8000, 32)), chunk(b"data", &data)]);
        let (_f, s) = open(&bytes, false);
        assert_eq!(
            ints(&s.window(0, 2, None).unwrap(), "ch1"),
            [i32::MIN as i64, 7]
        );
    }

    #[test]
    fn the_extensible_channel_mask_names_the_columns() {
        let mut body = fmt(0xFFFE, 4, 48000, 24);
        body.extend_from_slice(&22u16.to_le_bytes());
        body.extend_from_slice(&20u16.to_le_bytes());
        // L, R, C, LFE.
        body.extend_from_slice(&0x0Fu32.to_le_bytes());
        body.extend_from_slice(&1u16.to_le_bytes());
        body.extend_from_slice(&SUBTYPE_TAIL);
        let bytes = wav(&[chunk(b"fmt ", &body), chunk(b"data", &[0; 12])]);
        let (_f, s) = open(&bytes, false);
        assert_eq!(s.header().channel_names, ["L", "R", "C", "LFE"]);
        assert_eq!(s.header().valid_bits, 20);
        assert_eq!(s.header().encoding, "extensible PCM");

        // A mask naming fewer speakers than there are channels numbers the rest.
        assert_eq!(channel_names(3, 0x4), ["C", "ch2", "ch3"]);
    }

    /// A plain PCM header whose block alignment gives each 24-bit sample 4 bytes:
    /// the samples are read 4 bytes apart, as 32-bit with 24 valid bits.
    #[test]
    fn a_wider_container_in_the_block_alignment_is_honored() {
        let mut body = fmt(1, 2, 8000, 24);
        body[12..14].copy_from_slice(&8u16.to_le_bytes());
        let samples: Vec<u8> = [0x0001_0000i32 << 8, -256]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let bytes = wav(&[chunk(b"fmt ", &body), chunk(b"data", &samples)]);
        let (_f, source) = open(&bytes, false);
        assert_eq!(source.header().sample, Sample::I32);
        assert_eq!(source.header().valid_bits, 24);
        let df = source.window(0, 1, None).unwrap();
        assert_eq!(ints(&df, "ch1"), [0x0001_0000 << 8]);
        assert_eq!(ints(&df, "ch2"), [-256]);
    }

    /// A RIFF writer that ran past 4 GiB leaves the data size modulo 2^32.
    #[test]
    fn a_data_size_that_wrapped_past_4_gib_is_unwrapped() {
        const GIB4: u64 = 1 << 32;
        assert_eq!(unwrapped_size(100, 1_000), 100, "a small file as stated");
        assert_eq!(unwrapped_size(100, GIB4 + 100), GIB4 + 100);
        assert_eq!(
            unwrapped_size(100, 2 * GIB4 + 150),
            2 * GIB4 + 100,
            "chunks after the data are not samples"
        );
        assert_eq!(
            unwrapped_size(GIB4 - 2, GIB4 + 10),
            GIB4 - 2,
            "fits as stated"
        );
    }

    #[test]
    fn a_recording_in_progress_is_counted_by_the_file() {
        // A placeholder data size: the frames are what the file holds, and a partial
        // last frame waits.
        let mut bytes = wav(&[chunk(b"fmt ", &fmt(1, 1, 8000, 16))]);
        bytes.extend_from_slice(b"data");
        bytes.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
        bytes.extend_from_slice(&i16s(&[1, 2, 3]));
        bytes.push(0x04);
        let (_file, source) = open(&bytes, false);
        assert_eq!(source.header().data_declared, None);
        assert_eq!(source.frames(), 3);
        assert_eq!(source.trailing_bytes(), 1);
    }

    /// A file cut short while it is open is an error to read, not a SIGBUS from the
    /// map's pages past its new end.
    #[test]
    fn a_file_cut_short_while_open_is_an_error_to_read() {
        let samples = i16s(&[1; 4096]);
        let bytes = wav(&[
            chunk(b"fmt ", &fmt(1, 1, 8000, 16)),
            chunk(b"data", &samples),
        ]);
        let (file, source) = open(&bytes, false);
        let source = Arc::new(source);
        assert!(source.window(4000, 10, None).is_ok());
        let cut = file.as_file().set_len(64);
        // Windows refuses to cut a file it has mapped; the rows still read.
        if cfg!(windows) {
            assert_eq!(cut.unwrap_err().raw_os_error(), Some(1224));
            assert!(source.window(4000, 10, None).is_ok());
            return;
        }
        cut.unwrap();
        let err = source.window(4000, 10, None).unwrap_err().to_string();
        assert!(err.contains("open it again"), "{err}");
        assert!(source.lazy().collect().is_err());
        assert!(source.signal_report(&|| false).is_err());
    }

    #[test]
    fn a_data_size_past_the_end_is_cut_to_the_file() {
        let mut bytes = wav(&[chunk(b"fmt ", &fmt(1, 1, 8000, 16))]);
        bytes.extend_from_slice(b"data");
        bytes.extend_from_slice(&1000u32.to_le_bytes());
        bytes.extend_from_slice(&i16s(&[1, 2]));
        let (_f, source) = open(&bytes, false);
        assert_eq!(source.frames(), 2);
        assert_eq!(source.cut_short(), Some((1000, 4)));
    }

    #[test]
    fn rf64_takes_its_sizes_from_ds64() {
        let mut ds64 = Vec::new();
        ds64.extend_from_slice(&0u64.to_le_bytes());
        ds64.extend_from_slice(&4u64.to_le_bytes());
        ds64.extend_from_slice(&2u64.to_le_bytes());
        ds64.extend_from_slice(&0u32.to_le_bytes());
        let mut body = chunk(b"ds64", &ds64);
        body.extend(chunk(b"fmt ", &fmt(1, 1, 8000, 16)));
        body.extend_from_slice(b"data");
        body.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
        body.extend_from_slice(&i16s(&[9, 8, 7]));
        let mut bytes = b"RF64".to_vec();
        bytes.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
        bytes.extend_from_slice(b"WAVE");
        bytes.extend(body);
        let (_f, source) = open(&bytes, false);
        assert_eq!(source.header().container, Container::Rf64);
        assert_eq!(source.frames(), 2, "ds64 says two frames");
    }

    #[test]
    fn bext_ixml_info_and_cue_labels_are_read() {
        let mut bext = vec![0u8; 602];
        bext[..11].copy_from_slice(b"Scene notes");
        bext[256..262].copy_from_slice(b"Mixer1");
        bext[320..330].copy_from_slice(b"2026-10-02");
        bext[330..338].copy_from_slice(b"12:00:00");
        bext[338..342].copy_from_slice(&48000u32.to_le_bytes());
        bext.extend_from_slice(b"A=PCM,F=48000\r\n");
        let ixml = b"<BWFXML><PROJECT>Film</PROJECT><SCENE>12A</SCENE><TAKE>3</TAKE></BWFXML>";
        let mut cue = 2u32.to_le_bytes().to_vec();
        for (id, at) in [(1u32, 0u32), (2, 2)] {
            cue.extend_from_slice(&id.to_le_bytes());
            cue.extend_from_slice(&at.to_le_bytes());
            cue.extend_from_slice(b"data");
            cue.extend_from_slice(&[0; 8]);
            cue.extend_from_slice(&at.to_le_bytes());
        }
        let mut labl = 1u32.to_le_bytes().to_vec();
        labl.extend_from_slice(b"Intro\0");
        let mut ltxt = 2u32.to_le_bytes().to_vec();
        ltxt.extend_from_slice(&1u32.to_le_bytes());
        ltxt.extend_from_slice(&[0; 12]);
        let mut adtl = b"adtl".to_vec();
        adtl.extend(chunk(b"labl", &labl));
        adtl.extend(chunk(b"ltxt", &ltxt));
        let mut info = b"INFO".to_vec();
        info.extend(chunk(b"INAM", b"Take three\0"));
        let bytes = wav(&[
            chunk(b"bext", &bext),
            chunk(b"iXML", ixml),
            chunk(b"fmt ", &fmt(1, 1, 4, 16)),
            chunk(b"data", &i16s(&[0, 0, 0])),
            chunk(b"cue ", &cue),
            chunk(b"LIST", &adtl),
            chunk(b"LIST", &info),
        ]);
        let (_f, source) = open(&bytes, false);
        let h = source.header();
        assert!(h.broadcast);
        let meta = |k: &str| {
            h.metadata
                .iter()
                .find(|(key, _)| key == k)
                .map(|(_, v)| v.as_str())
                .unwrap_or_else(|| panic!("no {k}: {:?}", h.metadata))
        };
        assert_eq!(meta("bext.description"), "Scene notes");
        assert_eq!(meta("bext.originator"), "Mixer1");
        assert_eq!(meta("bext.origination"), "2026-10-02 12:00:00");
        assert_eq!(
            meta("bext.time_reference"),
            "03:20:00.000 (48000 samples)",
            "a time of day at the file's rate of 4 a second"
        );
        assert_eq!(meta("bext.coding_history"), "A=PCM,F=48000");
        assert_eq!(meta("ixml.scene"), "12A");
        assert_eq!(meta("ixml.take"), "3");
        assert_eq!(meta("info.title"), "Take three");
        assert_eq!(
            h.markers,
            [
                Marker {
                    id: 1,
                    sample: 0,
                    label: "Intro".into(),
                    length: None
                },
                Marker {
                    id: 2,
                    sample: 2,
                    label: String::new(),
                    length: Some(1)
                }
            ],
            "cue points after the data are found, labeled from adtl"
        );
    }

    /// An AIFF file: big-endian chunks, an 80-bit sample rate, signed 8-bit.
    fn aiff(kind: &[u8; 4], comm: &[u8], ssnd: &[u8], extra: &[u8]) -> Vec<u8> {
        let mut body = kind.to_vec();
        let be_chunk = |id: &[u8; 4], b: &[u8]| {
            let mut out = id.to_vec();
            out.extend_from_slice(&(b.len() as u32).to_be_bytes());
            out.extend_from_slice(b);
            if b.len() % 2 == 1 {
                out.push(0);
            }
            out
        };
        body.extend(be_chunk(b"COMM", comm));
        body.extend_from_slice(extra);
        let mut ssnd_body = vec![0u8; 8];
        ssnd_body.extend_from_slice(ssnd);
        body.extend(be_chunk(b"SSND", &ssnd_body));
        let mut out = b"FORM".to_vec();
        out.extend_from_slice(&(body.len() as u32).to_be_bytes());
        out.extend(body);
        out
    }

    /// 44100 as an 80-bit extended float.
    const RATE_44100: [u8; 10] = [0x40, 0x0E, 0xAC, 0x44, 0, 0, 0, 0, 0, 0];

    fn comm(channels: u16, frames: u32, bits: u16, code: Option<&[u8; 4]>) -> Vec<u8> {
        let mut c = channels.to_be_bytes().to_vec();
        c.extend_from_slice(&frames.to_be_bytes());
        c.extend_from_slice(&bits.to_be_bytes());
        c.extend_from_slice(&RATE_44100);
        if let Some(code) = code {
            c.extend_from_slice(code);
            c.extend_from_slice(&[0, 0]);
        }
        c
    }

    #[test]
    fn aiff_is_big_endian_with_an_extended_rate() {
        let data: Vec<u8> = [1i16, -2, 300, -400]
            .iter()
            .flat_map(|v| v.to_be_bytes())
            .collect();
        let mut mark = 1u16.to_be_bytes().to_vec();
        mark.extend_from_slice(&7u16.to_be_bytes());
        mark.extend_from_slice(&1u32.to_be_bytes());
        mark.extend_from_slice(b"\x05Verse");
        let mut extra = b"MARK".to_vec();
        extra.extend_from_slice(&(mark.len() as u32).to_be_bytes());
        extra.extend(mark);
        let bytes = aiff(b"AIFF", &comm(2, 2, 16, None), &data, &extra);
        let (_f, s) = open(&bytes, false);
        assert_eq!(s.header().sample_rate, 44100.0);
        assert_eq!(s.header().container, Container::Aiff);
        let df = s.window(0, 2, None).unwrap();
        assert_eq!(ints(&df, "ch1"), [1, 300]);
        assert_eq!(ints(&df, "ch2"), [-2, -400]);
        assert_eq!(s.header().markers[0].label, "Verse");
        assert_eq!(s.header().markers[0].sample, 1);

        // AIFF-C: little-endian `sowt` and big-endian float.
        let data: Vec<u8> = [5i16, -6].iter().flat_map(|v| v.to_le_bytes()).collect();
        let bytes = aiff(b"AIFC", &comm(1, 2, 16, Some(b"sowt")), &data, &[]);
        let (_f, s) = open(&bytes, false);
        assert_eq!(ints(&s.window(0, 2, None).unwrap(), "ch1"), [5, -6]);
        let data: Vec<u8> = [0.5f32].iter().flat_map(|v| v.to_be_bytes()).collect();
        let bytes = aiff(b"AIFC", &comm(1, 1, 32, Some(b"fl32")), &data, &[]);
        let (_f, s) = open(&bytes, false);
        assert_eq!(s.header().sample, Sample::F32);

        // COMM's frame count caps the data.
        let data: Vec<u8> = [1i16, 2, 3].iter().flat_map(|v| v.to_be_bytes()).collect();
        let bytes = aiff(b"AIFF", &comm(1, 2, 16, None), &data, &[]);
        let (_f, s) = open(&bytes, false);
        assert_eq!(s.frames(), 2);

        // A compressed AIFF-C is refused by name.
        let bytes = aiff(b"AIFC", &comm(1, 1, 16, Some(b"ima4")), &[0; 34], &[]);
        let err = read_header(&bytes).unwrap_err().to_string();
        assert!(err.contains("ima4"), "{err}");
    }

    #[test]
    fn the_lazy_scan_pushes_projection_and_counts_without_decoding() {
        let samples = i16s(&[1, 2, 3, 4, 5, 6]);
        let bytes = wav(&[
            chunk(b"fmt ", &fmt(1, 2, 8000, 16)),
            chunk(b"data", &samples),
        ]);
        let (_f, source) = open(&bytes, false);
        let lf = Arc::new(source).lazy();
        let df = lf.clone().select([col("ch2")]).collect().unwrap();
        assert_eq!(ints(&df, "ch2"), [2, 4, 6]);
        let n = lf.clone().select([len()]).collect().unwrap();
        assert_eq!(n.column("len").unwrap().u32().unwrap().get(0), Some(3));
        let head = lf.limit(2).collect().unwrap();
        assert_eq!(head.height(), 2);
    }

    /// A line chart of a channel streams the plan for its envelope, map and all, and
    /// keeps a one-sample spike a sample would miss.
    #[test]
    fn a_waveform_charts_as_its_envelope_over_seconds() {
        let mut values = vec![0i16; 20_000];
        values[12_345] = 30_000;
        let bytes = wav(&[
            chunk(b"fmt ", &fmt(1, 1, 8000, 16)),
            chunk(b"data", &i16s(&values)),
        ]);
        let (_f, source) = open(&bytes, false);
        let source = Arc::new(source);
        let lf = source.lazy();
        let schema = source.schema();
        let result = crate::chart_data::prepare_chart_data(
            &lf,
            &schema,
            SECONDS,
            &["ch1".into()],
            &crate::chart_data::ChartSampling::rows(Some(1_000)),
            true,
        )
        .unwrap();
        assert_eq!(result.rows.total_rows, 20_000);
        assert_eq!(result.rows.envelope_steps, Some(500));
        let top = result.series[0]
            .iter()
            .map(|p| p.1)
            .fold(f64::MIN, f64::max);
        assert_eq!(top, 30_000.0);
        let last = result.series[0].last().unwrap().0;
        assert!(last > 2.49 && last < 2.5, "X in seconds: {last}");
    }

    /// Every way a recording is refused names the file, in the one shape.
    #[test]
    fn errors_name_the_file() {
        let no_comm = {
            let mut out = b"FORM".to_vec();
            out.extend_from_slice(&4u32.to_be_bytes());
            out.extend_from_slice(b"AIFF");
            out
        };
        crate::readers::bad_input::each_names_its_file(
            crate::FileFormat::Audio,
            &[
                ("short.wav", b"RIFF", "too short"),
                ("rifx.wav", b"RIFX\0\0\0\0WAVEfmt ", "RIFX"),
                ("other.wav", b"OggS\0\0\0\0\0\0\0\0", "Not a WAV or AIFF"),
                ("nofmt.wav", &wav(&[chunk(b"data", &[0; 4])]), "no fmt"),
                (
                    "law.wav",
                    &wav(&[chunk(b"fmt ", &fmt(7, 1, 8000, 8)), chunk(b"data", &[])]),
                    "mu-law",
                ),
                (
                    "mute.wav",
                    &wav(&[chunk(b"fmt ", &fmt(1, 0, 8000, 16)), chunk(b"data", &[])]),
                    "0 channels",
                ),
                ("nocomm.aiff", &no_comm, "no COMM"),
                (
                    "packed.aifc",
                    &aiff(b"AIFC", &comm(1, 1, 16, Some(b"ACE2")), &[0; 2], &[]),
                    "\"ACE2\"",
                ),
            ],
        );
    }

    #[test]
    fn hostile_headers_are_errors() {
        let refused = |bytes: &[u8], says: &str| {
            let err = read_header(bytes).unwrap_err().to_string();
            assert!(err.contains(says), "{says:?} in {err:?}");
        };
        refused(b"RIFF", "too short");
        refused(&wav(&[chunk(b"data", &[0; 4])]), "no fmt");
        refused(&wav(&[chunk(b"fmt ", &fmt(1, 1, 8000, 16))]), "no data");
        refused(
            &wav(&[chunk(b"fmt ", &fmt(1, 0, 8000, 16)), chunk(b"data", &[])]),
            "0 channels",
        );
        refused(
            &wav(&[chunk(b"fmt ", &fmt(1, 2000, 8000, 16)), chunk(b"data", &[])]),
            "up to 1024",
        );
        refused(
            &wav(&[chunk(b"fmt ", &fmt(1, 1, 0, 16)), chunk(b"data", &[])]),
            "sample rate",
        );
        refused(
            &wav(&[chunk(b"fmt ", &fmt(7, 1, 8000, 8)), chunk(b"data", &[])]),
            "mu-law",
        );
        refused(
            &wav(&[chunk(b"fmt ", &fmt(1, 1, 8000, 40)), chunk(b"data", &[])]),
            "40-bit",
        );
        // A frame size that cannot hold the samples.
        let mut small = fmt(1, 2, 8000, 16);
        small[12..14].copy_from_slice(&2u16.to_le_bytes());
        refused(
            &wav(&[chunk(b"fmt ", &small), chunk(b"data", &[])]),
            "cannot hold",
        );
        // A cue count far past what its chunk holds reads what is there.
        let mut cue = u32::MAX.to_le_bytes().to_vec();
        cue.extend_from_slice(&[0; 24]);
        let bytes = wav(&[
            chunk(b"cue ", &cue),
            chunk(b"fmt ", &fmt(1, 1, 8000, 16)),
            chunk(b"data", &[0; 2]),
        ]);
        assert_eq!(read_header(&bytes).unwrap().markers.len(), 1);

        // Sizes far past the file are cut to it before anything is sized by them: a
        // data chunk claiming 4 GB in a file of 50 bytes holds what the file holds, and
        // a window asking for every frame there could be returns those.
        let mut bytes = wav(&[chunk(b"fmt ", &fmt(1, 2, 8000, 16))]);
        bytes.extend_from_slice(b"data");
        bytes.extend_from_slice(&0xFFFF_FFF0u32.to_le_bytes());
        bytes.extend_from_slice(&i16s(&[1, 2, 3, 4]));
        let (_f, source) = open(&bytes, false);
        assert_eq!(source.frames(), 2);
        let df = source.window(0, u64::MAX, None).unwrap();
        assert_eq!(df.height(), 2);
        assert_eq!(source.window(u64::MAX, u64::MAX, None).unwrap().height(), 0);
        // A chunk size that overflows the walk ends it rather than wrapping.
        let mut bytes = wav(&[chunk(b"fmt ", &fmt(1, 1, 8000, 16))]);
        bytes.extend_from_slice(b"JUNK");
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        refused(&bytes, "no data");
    }

    #[test]
    fn audio_is_known_by_its_first_bytes() {
        assert!(looks_like_audio(b"RIFF\0\0\0\0WAVEfmt "));
        assert!(looks_like_audio(b"RF64\xff\xff\xff\xffWAVE"));
        assert!(looks_like_audio(b"FORM\0\0\0\0AIFC"));
        assert!(!looks_like_audio(b"RIFF\0\0\0\0AVI "));
        assert!(!looks_like_audio(b"FORM"));
    }

    /// A long recording: the count is the header's arithmetic, and a slice deep in
    /// it reads its own frames, on the streaming engine as on the table's windows.
    #[test]
    fn a_long_recording_counts_and_slices_without_reading_what_is_before() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("long.wav");
        let mut bytes = wav(&[chunk(b"fmt ", &fmt(1, 1, 48000, 16))]);
        bytes.extend_from_slice(b"data");
        bytes.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
        let header = bytes.len() as u64;
        std::fs::write(&path, &bytes).unwrap();
        // 20 million frames of silence, sparse on disk, and one sample near the end.
        let frames = 20_000_000u64;
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(header + frames * 2).unwrap();
        use std::io::{Seek, SeekFrom};
        let mut file = file;
        file.seek(SeekFrom::Start(header + (frames - 3) * 2))
            .unwrap();
        file.write_all(&1234i16.to_le_bytes()).unwrap();
        drop(file);

        let source = Arc::new(AudioSource::open(&path, false).unwrap());
        assert_eq!(source.frames(), frames);
        let lf = source.lazy();
        // The streaming engine where the build has it, as the app reads.
        let streaming = |lf: LazyFrame| crate::statistics::collect_lazy(lf, true).unwrap();
        let n = streaming(lf.clone().select([len()]));
        assert_eq!(
            n.column("len").unwrap().u32().unwrap().get(0),
            Some(frames as u32)
        );
        let tail = streaming(lf.clone().slice((frames - 4) as i64, 10));
        assert_eq!(
            ints(&tail, "frame"),
            [19_999_996, 19_999_997, 19_999_998, 19_999_999]
        );
        assert_eq!(ints(&tail, "ch1"), [0, 1234, 0, 0]);
        let window = source.window(frames - 3, 1, None).unwrap();
        assert_eq!(ints(&window, "ch1"), [1234]);
    }

    #[test]
    fn the_signal_report_counts_runs_at_full_scale_and_of_zeros_and_the_mean() {
        // 1 kHz mono: 100 samples of 1,000, a run of 4 at full scale, a single sample
        // at full scale, 20 zeros (the shortest counted run is 16 samples), 10 more.
        let mut values = vec![1000i16; 100];
        values.extend([i16::MAX; 4]);
        values.push(500);
        values.push(i16::MIN);
        values.extend([0; 20]);
        values.extend([1000; 10]);
        let bytes = wav(&[
            chunk(b"fmt ", &fmt(1, 1, 1000, 16)),
            chunk(b"data", &i16s(&values)),
        ]);
        let (_f, source) = open(&bytes, false);
        let report = &source.signal_report(&|| false).unwrap().unwrap()[0];
        assert_eq!(report.full_scale, (-32768.0, 32767.0));
        assert_eq!(report.at_full_scale, 5);
        assert_eq!(
            (report.clip_runs, report.in_clip_runs, report.longest_clip),
            (1, 4, 4)
        );
        assert_eq!(report.zero_run_min, 16);
        assert_eq!(
            (report.zero_runs, report.in_zero_runs, report.longest_zeros),
            (1, 20, 20)
        );
        let sum: f64 = values.iter().map(|&v| v as f64).sum();
        assert!((report.mean - sum / values.len() as f64).abs() < 1e-9);

        // Normalized, the bounds and the mean are in the column's units.
        let (_f, source) = open(&bytes, true);
        let report = &source.signal_report(&|| false).unwrap().unwrap()[0];
        assert_eq!(report.full_scale.0, -1.0);
        assert_eq!(report.clip_runs, 1);
        assert!(report.mean < 1.0);

        // A stop is honored.
        assert!(source.signal_report(&|| true).unwrap().is_none());

        // 32-bit at its positive limit is 1.0 once normalized to f32, as the column
        // shows it; the run counts, and the column's rows match the bounds.
        let peak: Vec<u8> = [i32::MAX; 3].iter().flat_map(|v| v.to_le_bytes()).collect();
        let bytes = wav(&[chunk(b"fmt ", &fmt(1, 1, 1000, 32)), chunk(b"data", &peak)]);
        let (_f, source) = open(&bytes, true);
        let report = &source.signal_report(&|| false).unwrap().unwrap()[0];
        assert_eq!(report.clip_runs, 1);
        let column = source.window(0, 3, None).unwrap();
        let high = report.full_scale.1 as f32;
        assert!(
            column
                .column("ch1")
                .unwrap()
                .f32()
                .unwrap()
                .into_no_null_iter()
                .all(|v| v >= high)
        );
    }
}
