//! Records that are not all one size, read from a memory map: length-prefixed records,
//! variants a type field picks, records found by a sync marker, records in compressed
//! blocks or in the payloads of a packet capture, and fixed records with fields that
//! are read one at a time (varints, NUL-terminated text, counted groups, bit fields,
//! deltas, checksums).
//!
//! A first pass walks the records and keeps where every 1024th one starts (and, for
//! delta columns, the running sums there), so the index stays small and a window
//! anywhere is read by walking from the nearest checkpoint. Fixed records with a
//! known size need no pass at all: where a record starts is arithmetic.
//!
//! A compressed block is decompressed when it is first read, and a few are kept.
//!
//! The frame is decoded over a row index ([`crate::row_index`]), as
//! [`crate::fixed_records`]' is, and a window deeper in the file is read through
//! [`FramedRecords::window`].

use crate::fixed_records::{Bytes, ColumnLayout, Physical};
use crate::formats::{
    self, Amount, Codec, Compression, Delta, Encoding, Field, Framing, HeaderValues, Spec, Type,
};
use polars::prelude::*;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::{Arc, Mutex};

/// Records between checkpoints of the index.
const CHECKPOINT: u64 = 1024;

/// The most bytes one block may decompress to.
pub const MAX_BLOCK: usize = 256 << 20;

/// Decompressed blocks kept for reading again.
const CACHED_BLOCKS: usize = 8;

/// Items one group may hold, and values one counted field may hold.
const MAX_ITEMS: u64 = 1 << 20;

/// The most rows a table holds: Polars counts rows in 32 bits.
const MAX_ROWS: usize = IdxSize::MAX as usize;

/// A size known now, or read from an earlier field of the same record.
#[derive(Debug, Clone, Copy)]
enum SizeRef {
    Given(usize),
    Slot { slot: usize, adjust: i64 },
    Rest,
}

/// How an integer's raw value is read, for fields others refer to.
#[derive(Debug, Clone, Copy)]
struct IntRead {
    signed: bool,
    big: bool,
}

/// One field of a record (or a block header, a payload header, a group item).
#[derive(Debug, Clone)]
struct FieldPlan {
    name: String,
    slot: usize,
    kind: Kind,
    /// Values per cell: `None` for one; a size for an Array (given) or a List.
    count: Option<SizeRef>,
    /// The columns its values go to: one, or one per value when flattened.
    outs: Vec<usize>,
    /// Bit fields: column, lowest bit, width.
    bits: Vec<(usize, u32, u32)>,
    delta: Delta,
    /// The index of its running sum, for a delta.
    delta_index: usize,
    /// The stored value that means null, for a delta (checked before summing).
    sentinel: Option<i128>,
}

#[derive(Debug, Clone)]
enum Kind {
    /// A fixed-width value: an integer, a float, a bool, or text and bytes of a size
    /// known before the record is read.
    Fixed {
        width: usize,
        int: Option<IntRead>,
    },
    /// Text or bytes whose size is read from the record. `encoding` is `None` for bytes.
    Sized {
        size: SizeRef,
        encoding: Option<Encoding>,
    },
    /// Text to its NUL, within `max` bytes when given (which it then always takes).
    Strz {
        max: Option<SizeRef>,
        encoding: Encoding,
    },
    Var {
        signed: bool,
    },
    Pad {
        size: SizeRef,
    },
    /// An offset into a section, where NUL-terminated text is.
    StringAt {
        width: usize,
        big: bool,
        section: Range<usize>,
    },
    Group {
        items: Vec<FieldPlan>,
        slots: usize,
    },
}

/// One variant of the records.
#[derive(Debug, Clone)]
struct VariantPlan {
    name: Arc<str>,
    ints: Vec<i128>,
    texts: Vec<String>,
    fields: Vec<FieldPlan>,
    size: Option<SizeRef>,
}

/// What a column collects while records are read.
#[derive(Debug, Clone)]
enum Sink {
    /// Fixed-width cells, decoded together by the reader of fixed records.
    Packed {
        layout: ColumnLayout,
        cell: usize,
        buf: Vec<u8>,
        valid: Vec<bool>,
    },
    Text(Vec<Option<String>>),
    Binary(Vec<Option<Vec<u8>>>),
    Bits {
        width: u32,
        labels: Option<Arc<std::collections::BTreeMap<i64, String>>>,
        values: Vec<Option<u64>>,
    },
    Flag(Vec<Option<bool>>),
    Label(Vec<Option<Arc<str>>>),
    Time(Vec<Option<i64>>),
    List {
        inner: Box<Sink>,
        offsets: Vec<i64>,
        valid: Vec<bool>,
    },
    Struct {
        names: Vec<PlSmallStr>,
        fields: Vec<Sink>,
        len: usize,
    },
    /// A column the read does not want.
    Skip,
}

impl Sink {
    fn push_null(&mut self) {
        match self {
            Self::Packed {
                cell, buf, valid, ..
            } => {
                buf.resize(buf.len() + *cell, 0);
                valid.push(false);
            }
            Self::Text(v) => v.push(None),
            Self::Label(v) => v.push(None),
            Self::Binary(v) => v.push(None),
            Self::Bits { values, .. } => values.push(None),
            Self::Flag(v) => v.push(None),
            Self::Time(v) => v.push(None),
            Self::List { offsets, valid, .. } => {
                offsets.push(*offsets.last().unwrap_or(&0));
                valid.push(false);
            }
            Self::Struct { fields, len, .. } => {
                for f in fields {
                    f.push_null();
                }
                *len += 1;
            }
            Self::Skip => {}
        }
    }

    fn push_bytes(&mut self, bytes: &[u8]) {
        if let Self::Packed { buf, valid, .. } = self {
            buf.extend_from_slice(bytes);
            valid.push(true);
        }
    }

    fn finish(self, name: PlSmallStr) -> PolarsResult<Series> {
        Ok(match self {
            Self::Packed {
                layout,
                cell,
                buf,
                valid,
            } => {
                let rows = valid.len();
                // The cells sit side by side in `buf`, whatever the record's stride was:
                // a delta's running sum is wider than the value stored.
                let layout = ColumnLayout {
                    start: 0,
                    stride: cell,
                    ..layout
                };
                let column = crate::fixed_records::decode(&buf, &layout, rows)?;
                let series = column
                    .as_materialized_series()
                    .clone()
                    .with_name(name.clone());
                if valid.iter().all(|v| *v) {
                    series
                } else {
                    let mask: BooleanChunked = valid.into_iter().collect();
                    let nulls = Series::full_null(PlSmallStr::EMPTY, rows, series.dtype());
                    series.zip_with(&mask, &nulls)?.with_name(name)
                }
            }
            Self::Text(v) => StringChunked::from_iter_options(name, v.into_iter()).into_series(),
            Self::Label(v) => {
                // Few labels, many rows: each label is cast once and the rows gathered,
                // where casting every row's text took most of a variant column's time.
                let mut distinct: Vec<Arc<str>> = Vec::new();
                let mut by_text = std::collections::HashMap::new();
                let codes: IdxCa = v
                    .iter()
                    .map(|label| {
                        let label = label.as_ref()?;
                        // A variant's name is one shared string.
                        if let Some(i) =
                            distinct.iter().take(16).position(|d| Arc::ptr_eq(d, label))
                        {
                            return Some(i as IdxSize);
                        }
                        Some(*by_text.entry(label.clone()).or_insert_with(|| {
                            distinct.push(label.clone());
                            (distinct.len() - 1) as IdxSize
                        }))
                    })
                    .collect();
                StringChunked::from_iter_values(name, distinct.iter().map(|d| &**d))
                    .into_series()
                    .cast(&DataType::from_categories(Categories::global()))?
                    .take(&codes)?
            }
            Self::Binary(v) => BinaryChunked::from_iter_options(name, v.into_iter()).into_series(),
            Self::Bits {
                width,
                labels,
                values,
            } => match (labels, width) {
                (Some(labels), _) => StringChunked::from_iter_options(
                    name,
                    values.into_iter().map(|v| {
                        v.map(|v| {
                            i64::try_from(v)
                                .ok()
                                .and_then(|k| labels.get(&k).cloned())
                                .unwrap_or_else(|| v.to_string())
                        })
                    }),
                )
                .into_series(),
                (None, 1) => BooleanChunked::from_iter_options(
                    name,
                    values.into_iter().map(|v| v.map(|v| v != 0)),
                )
                .into_series(),
                (None, w) => {
                    let wide =
                        UInt64Chunked::from_iter_options(name, values.into_iter()).into_series();
                    let dtype = match w {
                        2..=8 => DataType::UInt8,
                        9..=16 => DataType::UInt16,
                        17..=32 => DataType::UInt32,
                        _ => DataType::UInt64,
                    };
                    wide.strict_cast(&dtype)?
                }
            },
            Self::Flag(v) => BooleanChunked::from_iter_options(name, v.into_iter()).into_series(),
            Self::Time(v) => Int64Chunked::from_iter_options(name, v.into_iter())
                .into_datetime(TimeUnit::Nanoseconds, None)
                .into_series(),
            Self::List {
                inner,
                offsets,
                valid,
            } => {
                let values = inner.finish(PlSmallStr::from_static("item"))?.rechunk();
                list_series(name, values, offsets, valid)?
            }
            Self::Struct { names, fields, len } => {
                let series = names
                    .into_iter()
                    .zip(fields)
                    .map(|(n, f)| f.finish(n))
                    .collect::<PolarsResult<Vec<_>>>()?;
                StructChunked::from_series(name, len, series.iter())?.into_series()
            }
            Self::Skip => Series::new_empty(name, &DataType::Null),
        })
    }
}

/// A List column of `values`, row `i` holding `offsets[i]..offsets[i + 1]`.
fn list_series(
    name: PlSmallStr,
    values: Series,
    offsets: Vec<i64>,
    valid: Vec<bool>,
) -> PolarsResult<Series> {
    use polars_arrow::array::ListArray;
    use polars_arrow::bitmap::Bitmap;
    use polars_arrow::offset::OffsetsBuffer;
    let inner_dtype = values.dtype().clone();
    let array = values.to_arrow(0, CompatLevel::newest());
    let mut all = Vec::with_capacity(offsets.len() + 1);
    all.push(0i64);
    all.extend(offsets);
    let offsets = OffsetsBuffer::<i64>::try_from(all)?;
    let validity = (!valid.iter().all(|v| *v)).then(|| Bitmap::from_iter(valid));
    let dtype = ListArray::<i64>::default_datatype(array.dtype().clone());
    let list = ListArray::<i64>::try_new(dtype, offsets, array, validity)?;
    Series::from_arrow(name, Box::new(list))?.cast(&DataType::List(Box::new(inner_dtype)))
}

/// A column of the table and what collects it.
#[derive(Debug, Clone)]
struct OutColumn {
    name: PlSmallStr,
    dtype: DataType,
    proto: Sink,
}

/// Where a run of records is.
#[derive(Debug, Clone)]
enum ChunkSource {
    /// Bytes of the file.
    Map(Range<usize>),
    /// A block body of the file, compressed.
    Block {
        body: Range<usize>,
        codec: Compression,
        uncompressed: Option<usize>,
    },
}

#[derive(Debug, Clone)]
struct Chunk {
    source: ChunkSource,
    /// Records in it, when its header (or the index) says.
    records: Option<u64>,
    /// A packet's capture time.
    time_ns: Option<i64>,
}

/// Where the reading of rows can start: the `row`th record, at `pos` in `chunk`.
#[derive(Debug, Clone)]
struct Checkpoint {
    row: u64,
    chunk: u32,
    pos: u32,
    /// Records of the chunk before `pos`, for a chunk that counts its records.
    taken: u32,
    acc: Box<[i128]>,
}

/// Where the records are.
#[derive(Debug)]
enum Index {
    /// Records all `size` bytes from `start`; row `i` is record `(ring + i) % rows`.
    Stride {
        start: usize,
        size: usize,
        ring: usize,
    },
    Walk(Vec<Checkpoint>),
}

/// The variant tag of a row read field by field: one cut short, or of no variant.
const WALK: u8 = u8::MAX;

/// Where each row's record starts and which variant it is, kept from the walk that
/// opens the file, so every column of a query reads from the same starts rather than
/// walking the records again (#662). One run of the map only: a block's records are
/// in its decompressed copy, and a packet's in its payload.
#[derive(Debug)]
struct RowTable {
    /// Where each row's record starts, its sync marker included.
    starts: crate::indexed::Offsets,
    /// Each row's variant (0 without variants), or [`WALK`].
    tags: Vec<u8>,
}

/// A file's walk, kept for the next open of it with the same spec: the walk is the one
/// read of the file an open makes, and a file opened again (its variants, `H`) is not
/// walked again. Kept by [`crate::indexed::keep`], which bounds what it keeps by the
/// size of the files.
struct KeptWalk {
    spec: Spec,
    data: Range<usize>,
    index: Arc<Index>,
    table: Option<Arc<RowTable>>,
    rows: usize,
    notes: Vec<String>,
}

/// Where one column's value is in a record of one variant.
#[derive(Debug, Clone)]
enum Source {
    /// The variant has no such field.
    Null,
    /// At a fixed place from the record's start, so read without a walk.
    At { offset: usize, field: FieldPlan },
    /// The variant's name.
    Label,
    /// Behind a field whose size the record says: the record is walked.
    Walk,
    /// A running sum, which needs every record before it: walked from a checkpoint.
    Summed,
}

/// The compiled spec: how to walk one record.
#[derive(Debug)]
struct Plan {
    framing: Framing,
    common: Vec<FieldPlan>,
    /// The slot of the type field and whether it is text.
    type_slot: Option<(usize, bool)>,
    variants: Vec<VariantPlan>,
    type_out: Option<usize>,
    /// The record's size, when the spec gives one or a field holds it.
    size: Option<SizeRef>,
    /// The width and byte order of a length repeated after the record.
    suffix: Option<(usize, bool)>,
    align: usize,
    sync: Vec<u8>,
    checksum: Option<ChecksumPlan>,
    /// Payload header fields, and the slot counting its records.
    chunk_header: Vec<FieldPlan>,
    chunk_count: Option<usize>,
    chunk_slots: usize,
    time_out: Option<usize>,
    slots: usize,
    deltas: Vec<Delta>,
    columns: Vec<OutColumn>,
    /// The variant read alone.
    only: Option<usize>,
    /// Per column, per variant (one entry without variants): where its value is.
    sources: Vec<Vec<Source>>,
}

#[derive(Debug, Clone)]
struct ChecksumPlan {
    algo: formats::ChecksumAlgo,
    field: usize,
    from: Option<usize>,
    to: usize,
    out: usize,
}

/// A record walk's per-record state: where each field started and the integers read.
struct Frame {
    starts: Vec<Option<usize>>,
    ends: Vec<Option<usize>>,
    ints: Vec<Option<i128>>,
}

impl Frame {
    fn new(slots: usize) -> Self {
        Self {
            starts: vec![None; slots],
            ends: vec![None; slots],
            ints: vec![None; slots],
        }
    }

    /// Forget slots `range`.
    fn clear(&mut self, range: Range<usize>) {
        for s in range {
            self.starts[s] = None;
            self.ends[s] = None;
            self.ints[s] = None;
        }
    }
}

/// Why a record could not be read.
enum Stop {
    /// The bytes ran out before the record did.
    Truncated,
    /// Anything else: said as it is.
    Said(String),
}

/// Records read through a spec, by walking them.
pub struct FramedRecords {
    bytes: Arc<Bytes>,
    plan: Arc<Plan>,
    chunks: Vec<Chunk>,
    index: Arc<Index>,
    /// Built by the walk at open, when the records are one run of the map.
    table: Option<Arc<RowTable>>,
    rows: usize,
    schema: SchemaRef,
    cache: Mutex<VecDeque<(usize, Arc<Vec<u8>>)>>,
}

impl std::fmt::Debug for FramedRecords {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FramedRecords")
            .field("rows", &self.rows)
            .field("chunks", &self.chunks.len())
            .finish()
    }
}

/// Whether `spec` needs records walked rather than the fixed reader.
pub fn needed(spec: &Spec) -> bool {
    fn walked(field: &Field) -> bool {
        !field.is_fixed_width()
            || field.size.as_ref().is_some_and(|s| !s.is_fixed())
            || field.count.as_ref().is_some_and(|s| !s.is_fixed())
            || field.delta != Delta::None
            || !field.bits.is_empty()
            || field.string_at.is_some()
    }
    let r = &spec.records;
    r.framing != Framing::Fixed
        || !r.variants.is_empty()
        || r.checksum.is_some()
        || r.ring.is_some()
        || spec.blocks.is_some()
        || spec.capture.is_some()
        || formats::all_fields(r).any(walked)
}

/// What compiling the spec builds: columns, slots and running sums.
struct Compiler<'a> {
    spec: &'a Spec,
    header: &'a HeaderValues,
    data: &'a [u8],
    slots: usize,
    deltas: Vec<Delta>,
}

/// Names of a scope's fields and their slots.
#[derive(Default, Clone)]
struct Scope(Vec<(String, usize)>);

impl Scope {
    fn slot(&self, name: &str) -> Option<usize> {
        self.0
            .iter()
            .rev()
            .find(|(n, _)| n == name)
            .map(|(_, s)| *s)
    }
}

impl Compiler<'_> {
    fn size(&self, amount: &Amount, scope: &Scope, what: &str) -> Result<SizeRef, String> {
        Ok(match amount {
            Amount::Record { field, adjust } => SizeRef::Slot {
                slot: scope
                    .slot(field)
                    .ok_or_else(|| format!("{what}: `{field}` is not an earlier field"))?,
                adjust: *adjust,
            },
            Amount::Rest => SizeRef::Rest,
            other => SizeRef::Given(
                usize::try_from(self.header.resolve(other, what)?)
                    .map_err(|_| format!("{what}: too large"))?,
            ),
        })
    }

    /// The fields' plans; their columns are added to `columns`.
    fn fields(
        &mut self,
        fields: &[Field],
        scope: &mut Scope,
        columns: &mut Vec<OutColumn>,
        shared: bool,
    ) -> Result<Vec<FieldPlan>, String> {
        let mut out = Vec::new();
        for field in fields {
            out.push(self.field(field, scope, columns, shared)?);
        }
        Ok(out)
    }

    /// The column named `name`, new or (for fields variants share) the one there is.
    fn column(
        columns: &mut Vec<OutColumn>,
        name: &str,
        dtype: DataType,
        proto: Sink,
        shared: bool,
    ) -> usize {
        if shared && let Some(i) = columns.iter().position(|c| c.name == name) {
            return i;
        }
        columns.push(OutColumn {
            name: name.into(),
            dtype,
            proto,
        });
        columns.len() - 1
    }

    fn field(
        &mut self,
        field: &Field,
        scope: &mut Scope,
        columns: &mut Vec<OutColumn>,
        shared: bool,
    ) -> Result<FieldPlan, String> {
        let name = field.name.clone().unwrap_or_default();
        let slot = self.slots;
        self.slots += 1;
        let big = field.endian.unwrap_or(self.spec.endian) == formats::Endian::Big;
        let count = field
            .count
            .as_ref()
            .map(|c| self.size(c, scope, "count"))
            .transpose()?;
        let given_count = match count {
            Some(SizeRef::Given(n)) => {
                if n as u64 > MAX_ITEMS {
                    return Err(format!(
                        "field `{name}`: {n} values is more than {MAX_ITEMS}"
                    ));
                }
                Some(n)
            }
            _ => None,
        };
        let mut plan = FieldPlan {
            name: name.clone(),
            slot,
            kind: Kind::Pad {
                size: SizeRef::Given(0),
            },
            count,
            outs: Vec::new(),
            bits: Vec::new(),
            delta: field.delta,
            delta_index: 0,
            sentinel: None,
        };
        let layout = |width: usize, cells: usize| -> Result<ColumnLayout, String> {
            let mut layout = formats::layout_of(
                self.spec,
                field,
                &name,
                formats::Place {
                    start: 0,
                    stride: None,
                    width,
                    count: cells,
                },
                self.header,
            )?;
            layout.big_endian = big;
            Ok(layout)
        };
        let packed = |layout: ColumnLayout| {
            let cell = layout.width * layout.count;
            Sink::Packed {
                layout,
                cell,
                buf: Vec::new(),
                valid: Vec::new(),
            }
        };
        // A fixed value, possibly counted: an Array, flattened columns, or a List.
        let mut fixed_out = |this: &mut Self,
                             plan: &mut FieldPlan,
                             mut layout: ColumnLayout|
         -> Result<(), String> {
            let _ = this;
            match (count, given_count) {
                (None, _) => {
                    let dtype = layout.dtype();
                    plan.outs = vec![Self::column(columns, &name, dtype, packed(layout), shared)];
                }
                (Some(_), Some(n)) if field.flatten => {
                    layout.count = 1;
                    for i in 0..n {
                        let n_name = format!("{name}_{i}");
                        let mut l = layout.clone();
                        l.name = n_name.as_str().into();
                        let dtype = l.dtype();
                        plan.outs
                            .push(Self::column(columns, &n_name, dtype, packed(l), shared));
                    }
                }
                (Some(_), Some(n)) => {
                    layout.count = n.max(1);
                    let dtype = layout.dtype();
                    plan.outs = vec![Self::column(columns, &name, dtype, packed(layout), shared)];
                }
                (Some(_), None) => {
                    layout.count = 1;
                    let item = layout.dtype();
                    let sink = Sink::List {
                        inner: Box::new(packed(layout)),
                        offsets: Vec::new(),
                        valid: Vec::new(),
                    };
                    plan.outs = vec![Self::column(
                        columns,
                        &name,
                        DataType::List(Box::new(item)),
                        sink,
                        shared,
                    )];
                }
            }
            Ok(())
        };
        match field.ty {
            Type::Pad => {
                let size = field
                    .size
                    .as_ref()
                    .map_or(Ok(SizeRef::Given(0)), |s| self.size(s, scope, "size"))?;
                plan.kind = Kind::Pad { size };
            }
            Type::Group => {
                let mut inner_scope = Scope::default();
                let mut inner_columns = Vec::new();
                let first = self.slots;
                let items =
                    self.fields(&field.group, &mut inner_scope, &mut inner_columns, false)?;
                let slots = self.slots - first;
                let names: Vec<PlSmallStr> = inner_columns.iter().map(|c| c.name.clone()).collect();
                let struct_dtype = DataType::Struct(
                    inner_columns
                        .iter()
                        .map(|c| polars::prelude::Field::new(c.name.clone(), c.dtype.clone()))
                        .collect(),
                );
                let sink = Sink::List {
                    inner: Box::new(Sink::Struct {
                        names,
                        fields: inner_columns.into_iter().map(|c| c.proto).collect(),
                        len: 0,
                    }),
                    offsets: Vec::new(),
                    valid: Vec::new(),
                };
                plan.outs = vec![Self::column(
                    columns,
                    &name,
                    DataType::List(Box::new(struct_dtype)),
                    sink,
                    shared,
                )];
                plan.kind = Kind::Group { items, slots };
            }
            Type::VarU | Type::VarS => {
                let signed = field.ty == Type::VarS;
                let mut l = layout(8, 1)?;
                if field.delta != Delta::None {
                    l.physical = Physical::Signed(8);
                    plan.sentinel = sentinel(field, 64, signed);
                    l.null = None;
                }
                plan.kind = Kind::Var { signed };
                fixed_out(self, &mut plan, l)?;
            }
            Type::Strz => {
                let max = field
                    .size
                    .as_ref()
                    .map(|s| self.size(s, scope, "size"))
                    .transpose()?;
                plan.kind = Kind::Strz {
                    max,
                    encoding: field.encoding,
                };
                plan.outs = vec![Self::column(
                    columns,
                    &name,
                    DataType::String,
                    Sink::Text(Vec::new()),
                    shared,
                )];
            }
            Type::Str | Type::Bytes if !field.size.as_ref().is_some_and(Amount::is_fixed) => {
                let size = self.size(
                    field.size.as_ref().expect("checked at parse"),
                    scope,
                    "size",
                )?;
                let text = field.ty == Type::Str;
                plan.kind = Kind::Sized {
                    size,
                    encoding: text.then_some(field.encoding),
                };
                let (dtype, sink) = if text {
                    (DataType::String, Sink::Text(Vec::new()))
                } else {
                    (DataType::Binary, Sink::Binary(Vec::new()))
                };
                plan.outs = vec![Self::column(columns, &name, dtype, sink, shared)];
            }
            _ => {
                let width = match (field.ty.width(), &field.size) {
                    (Some(w), _) => w as usize,
                    (None, Some(amount)) => usize::try_from(self.header.resolve(amount, "size")?)
                        .map_err(|_| "size: too large".to_string())?,
                    (None, None) => 0,
                };
                if width == 0 {
                    return Err(format!("field `{name}` takes no bytes"));
                }
                let int = match field.ty {
                    Type::Unsigned(_) => Some(IntRead { signed: false, big }),
                    Type::Signed(_) => Some(IntRead { signed: true, big }),
                    _ => None,
                };
                if let Some(section) = &field.string_at {
                    let section = self.section(section)?;
                    plan.kind = Kind::StringAt {
                        width,
                        big,
                        section,
                    };
                    plan.outs = vec![Self::column(
                        columns,
                        &name,
                        DataType::String,
                        Sink::Text(Vec::new()),
                        shared,
                    )];
                } else {
                    let mut l = layout(width, 1)?;
                    if field.delta != Delta::None {
                        l.physical = Physical::Signed(8);
                        l.width = 8;
                        plan.sentinel =
                            sentinel(field, width as u32 * 8, int.is_some_and(|i| i.signed));
                        l.null = None;
                    }
                    plan.kind = Kind::Fixed { width, int };
                    fixed_out(self, &mut plan, l)?;
                }
            }
        }
        if field.delta != Delta::None {
            plan.delta_index = self.deltas.len();
            self.deltas.push(field.delta);
        }
        for bit in &field.bits {
            let sink = Sink::Bits {
                width: bit.width,
                labels: bit.labels.clone(),
                values: Vec::new(),
            };
            let dtype = match (&bit.labels, bit.width) {
                (Some(_), _) => DataType::String,
                (None, 1) => DataType::Boolean,
                (None, 2..=8) => DataType::UInt8,
                (None, 9..=16) => DataType::UInt16,
                (None, 17..=32) => DataType::UInt32,
                _ => DataType::UInt64,
            };
            let col = Self::column(columns, &bit.name, dtype, sink, shared);
            plan.bits.push((col, bit.bit, bit.width));
        }
        if field.name.is_some() {
            scope.0.push((name, slot));
        }
        Ok(plan)
    }

    /// The bytes of section `name`, bounded by the file.
    fn section(&self, name: &str) -> Result<Range<usize>, String> {
        let section = self
            .spec
            .sections
            .iter()
            .find(|s| s.name == name)
            .ok_or_else(|| format!("no section named {name}"))?;
        let offset = self.header.resolve_any(&section.offset, "section offset")?;
        let size = self.header.resolve_any(&section.size, "section size")?;
        let end = offset.saturating_add(size);
        if end > self.data.len() as u64 {
            return Err(format!(
                "section {name} runs from byte {offset} to {end}, past the file's {} bytes",
                self.data.len()
            ));
        }
        Ok(offset as usize..end as usize)
    }
}

/// The raw value a delta field's `null` stands for.
fn sentinel(field: &Field, bits: u32, signed: bool) -> Option<i128> {
    use crate::fixed_records::Null;
    let bits = bits.clamp(1, 64);
    match field.null? {
        Null::Min if signed => Some(-(1i128 << (bits - 1))),
        Null::Min => Some(0),
        Null::Max if signed => Some((1i128 << (bits - 1)) - 1),
        Null::Max => Some((1i128 << bits) - 1),
        Null::Value(v) => Some(v),
        Null::NaN => None,
    }
}

/// An integer of `bytes`.
fn int_of(bytes: &[u8], read: IntRead) -> i128 {
    if read.signed {
        i128::from(crate::fixed_records::read_signed(bytes, read.big))
    } else {
        i128::from(crate::fixed_records::read_unsigned(bytes, read.big))
    }
}

/// A LEB128 value at the front of `bytes`, and the bytes it took.
fn leb128(bytes: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0u64;
    for (i, b) in bytes.iter().take(10).enumerate() {
        let part = u64::from(b & 0x7f);
        if i == 9 && part > 1 {
            return None;
        }
        value |= part << (7 * i);
        if b & 0x80 == 0 {
            return Some((value, i + 1));
        }
    }
    None
}

/// Text of `bytes` in `encoding`, trimmed as a fixed field's is.
fn decode_text(bytes: &[u8], encoding: Encoding) -> String {
    match encoding {
        Encoding::Utf8 => crate::fixed_records::text(bytes),
        Encoding::Latin1 => crate::fixed_records::latin1(bytes),
        Encoding::Utf16Le => crate::fixed_records::utf16(bytes, false),
        Encoding::Utf16Be => crate::fixed_records::utf16(bytes, true),
    }
}

/// Where the NUL unit ends text at the front of `bytes`, if there is one.
fn nul_at(bytes: &[u8], unit: usize) -> Option<usize> {
    if unit == 1 {
        memchr::memchr(0, bytes)
    } else {
        bytes
            .chunks_exact(unit)
            .position(|c| c.iter().all(|b| *b == 0))
            .map(|i| i * unit)
    }
}

/// What reading at a place found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Got {
    /// A row.
    Row,
    /// A record of a variant not read.
    Skipped,
    /// No record left.
    None,
}

/// What a walk pushes to: the sinks of one level's columns, and which it filled.
struct Out<'s> {
    sinks: &'s mut [Sink],
    filled: &'s mut [bool],
}

impl Out<'_> {
    /// Nulls in every column not filled, and a fresh start for the next row.
    fn finish_row(&mut self) {
        for (sink, filled) in self.sinks.iter_mut().zip(self.filled.iter_mut()) {
            if !*filled {
                sink.push_null();
            }
            *filled = false;
        }
    }
}

/// Reads records out of one run of bytes.
struct Walker<'a> {
    plan: &'a Plan,
    /// The whole file, for sections.
    file: &'a [u8],
    frame: Frame,
    acc: Vec<i128>,
    /// The end of what the current record (or group item) may take.
    end: usize,
    /// Whether `end` is the record's own end, so a field past it is null.
    bounded: bool,
    /// A field did not fit, so the rest of the record is null.
    short: bool,
    record_start: usize,
    /// The field that holds the record's size, and what to add to it.
    size_slot: Option<(usize, i64)>,
    /// Bytes skipped looking for sync markers.
    skipped: u64,
    /// The variant of the record read last, if it had one.
    variant: Option<usize>,
}

impl<'a> Walker<'a> {
    fn new(plan: &'a Plan, file: &'a [u8]) -> Self {
        Self {
            plan,
            file,
            frame: Frame::new(plan.slots),
            acc: vec![0; plan.deltas.len()],
            end: 0,
            bounded: false,
            short: false,
            record_start: 0,
            size_slot: None,
            skipped: 0,
            variant: None,
        }
    }

    fn size_of(&self, s: SizeRef, pos: usize, what: &str) -> Result<usize, Stop> {
        match s {
            SizeRef::Given(n) => Ok(n),
            SizeRef::Rest => Ok(self.end.saturating_sub(pos)),
            SizeRef::Slot { slot, adjust } => {
                let v = self.frame.ints[slot].ok_or(Stop::Truncated)? + i128::from(adjust);
                usize::try_from(v)
                    .ok()
                    .filter(|n| *n as u64 <= formats::MAX_SIZE)
                    .ok_or_else(|| {
                        Stop::Said(format!(
                            "`{what}` at byte {pos} is {v}, outside 0 to {}",
                            formats::MAX_SIZE
                        ))
                    })
            }
        }
    }

    fn take(&self, pos: &mut usize, n: usize) -> Result<Range<usize>, Stop> {
        let start = *pos;
        let stop = start.checked_add(n).ok_or(Stop::Truncated)?;
        if stop > self.end {
            return Err(Stop::Truncated);
        }
        *pos = stop;
        Ok(start..stop)
    }

    /// Read `fields` from `data` at `pos`, pushing to `out`. A field that does not fit
    /// is null once the record's end is known, and truncates the record before then.
    fn walk(
        &mut self,
        fields: &[FieldPlan],
        data: &[u8],
        pos: &mut usize,
        mut out: Option<&mut Out<'_>>,
    ) -> Result<(), Stop> {
        for f in fields {
            let res = if self.short {
                Err(Stop::Truncated)
            } else {
                self.field(f, data, pos, out.as_deref_mut())
            };
            match res {
                Ok(()) => {}
                Err(Stop::Truncated) if self.bounded => {
                    self.short = true;
                    if let Some(out) = out.as_deref_mut() {
                        null_outs(f, out);
                    }
                }
                Err(e) => return Err(e),
            }
            if let Some((slot, adjust)) = self.size_slot
                && slot == f.slot
                && !self.bounded
            {
                let start = self.record_start;
                let len = self.frame.ints[slot].ok_or(Stop::Truncated)? + i128::from(adjust);
                let total = usize::try_from(len)
                    .ok()
                    .filter(|t| *t as u64 <= formats::MAX_SIZE && *t >= *pos - start)
                    .ok_or_else(|| {
                        Stop::Said(format!(
                            "the record at byte {start} gives its size as {len}, outside {} to {}",
                            *pos - start,
                            formats::MAX_SIZE
                        ))
                    })?;
                let rec_end = start + total;
                if rec_end > self.end {
                    return Err(Stop::Truncated);
                }
                self.end = rec_end;
                self.bounded = true;
            }
        }
        Ok(())
    }

    fn field(
        &mut self,
        f: &FieldPlan,
        data: &[u8],
        pos: &mut usize,
        out: Option<&mut Out<'_>>,
    ) -> Result<(), Stop> {
        self.frame.starts[f.slot] = Some(*pos);
        let count = match f.count {
            None => None,
            Some(c) => {
                let n = self.size_of(c, *pos, &f.name)?;
                if n as u64 > MAX_ITEMS {
                    return Err(Stop::Said(format!(
                        "`{}` at byte {} counts {n} values, more than {MAX_ITEMS}",
                        f.name, *pos
                    )));
                }
                Some(n)
            }
        };
        match &f.kind {
            Kind::Pad { size } => {
                let n = self.size_of(*size, *pos, "pad")?;
                self.take(pos, n)?;
            }
            Kind::Fixed { width, int } => {
                let cells = count.unwrap_or(1);
                let range = self.take(pos, width.checked_mul(cells).ok_or(Stop::Truncated)?)?;
                let bytes = &data[range];
                let raw = int.and_then(|r| (cells > 0).then(|| int_of(&bytes[..*width], r)));
                if count.is_none() {
                    self.frame.ints[f.slot] = raw;
                }
                self.value(f, raw, Some((bytes, *width)), count, out);
            }
            Kind::Var { signed } => {
                let cells = count.unwrap_or(1);
                let mut bytes = Vec::with_capacity(cells.min(64) * 8);
                let mut first = None;
                for _ in 0..cells {
                    let (v, n) =
                        leb128(&data[(*pos).min(self.end)..self.end]).ok_or(Stop::Truncated)?;
                    *pos += n;
                    let v = if *signed {
                        i128::from(((v >> 1) as i64) ^ -((v & 1) as i64))
                    } else {
                        i128::from(v)
                    };
                    first.get_or_insert(v);
                    if *signed {
                        bytes.extend((v as i64).to_le_bytes());
                    } else {
                        bytes.extend((v as u64).to_le_bytes());
                    }
                }
                if count.is_none() {
                    self.frame.ints[f.slot] = first;
                }
                self.value(f, first, Some((&bytes, 8)), count, out);
            }
            Kind::Sized { size, encoding } => {
                let n = self.size_of(*size, *pos, &f.name)?;
                let range = self.take(pos, n)?;
                if let Some(out) = out {
                    let col = f.outs[0];
                    out.filled[col] = true;
                    match (&mut out.sinks[col], encoding) {
                        (Sink::Text(v), Some(enc)) => {
                            v.push(Some(decode_text(&data[range], *enc)));
                        }
                        (Sink::Binary(v), None) => v.push(Some(data[range].to_vec())),
                        _ => {}
                    }
                }
            }
            Kind::Strz { max, encoding } => {
                let unit = encoding.unit();
                let text = match max {
                    Some(m) => {
                        let n = self.size_of(*m, *pos, &f.name)?;
                        let range = self.take(pos, n)?;
                        let bytes = &data[range];
                        let stop = nul_at(bytes, unit).unwrap_or(bytes.len());
                        decode_text(&bytes[..stop], *encoding)
                    }
                    None => {
                        let rest = &data[(*pos).min(self.end)..self.end];
                        let stop = nul_at(rest, unit).ok_or(Stop::Truncated)?;
                        let text = decode_text(&rest[..stop], *encoding);
                        *pos += stop + unit;
                        text
                    }
                };
                if let Some(out) = out {
                    let col = f.outs[0];
                    out.filled[col] = true;
                    if let Sink::Text(v) = &mut out.sinks[col] {
                        v.push(Some(text));
                    }
                }
            }
            Kind::StringAt {
                width,
                big,
                section,
            } => {
                let range = self.take(pos, *width)?;
                let offset = crate::fixed_records::read_unsigned(&data[range], *big);
                self.frame.ints[f.slot] = Some(i128::from(offset));
                if let Some(out) = out {
                    let col = f.outs[0];
                    out.filled[col] = true;
                    let heap = &self.file[section.clone()];
                    let text = usize::try_from(offset)
                        .ok()
                        .filter(|o| *o < heap.len())
                        .map(|o| {
                            let rest = &heap[o..];
                            let stop = memchr::memchr(0, rest).unwrap_or(rest.len());
                            crate::fixed_records::text(&rest[..stop])
                        });
                    if let Sink::Text(v) = &mut out.sinks[col] {
                        v.push(text);
                    }
                }
            }
            Kind::Group { items, slots } => {
                let n = count.unwrap_or(0);
                let first = items.first().map_or(0, |i| i.slot);
                let saved = (
                    self.end,
                    self.bounded,
                    self.short,
                    self.record_start,
                    self.size_slot,
                );
                self.size_slot = None;
                // Walked once to see the whole group fits, then again into the sinks, so a
                // group cut short leaves no half-pushed items.
                let start = *pos;
                let mut probe = start;
                let mut fits = Ok(());
                for _ in 0..n {
                    self.frame.clear(first..first + slots);
                    self.bounded = true;
                    self.short = false;
                    self.record_start = probe;
                    if let Err(e) = self.walk(items, data, &mut probe, None) {
                        fits = Err(e);
                        break;
                    }
                    if self.short {
                        fits = Err(Stop::Truncated);
                        break;
                    }
                }
                let (end, bounded, short, record_start, size_slot) = saved;
                self.end = end;
                if let Err(e) = fits {
                    self.bounded = bounded;
                    self.short = short;
                    self.record_start = record_start;
                    self.size_slot = size_slot;
                    return Err(e);
                }
                *pos = probe;
                if let Some(out) = out {
                    let col = f.outs[0];
                    out.filled[col] = true;
                    if let Sink::List {
                        inner,
                        offsets,
                        valid,
                    } = &mut out.sinks[col]
                        && let Sink::Struct { fields, len, .. } = inner.as_mut()
                    {
                        let mut at = start;
                        let mut filled = vec![false; fields.len()];
                        for _ in 0..n {
                            self.frame.clear(first..first + slots);
                            self.bounded = true;
                            self.short = false;
                            self.record_start = at;
                            let mut item_out = Out {
                                sinks: fields,
                                filled: &mut filled,
                            };
                            // It fitted above, and reads the same again.
                            let _ = self.walk(items, data, &mut at, Some(&mut item_out));
                            item_out.finish_row();
                            *len += 1;
                        }
                        offsets.push(offsets.last().copied().unwrap_or(0) + n as i64);
                        valid.push(true);
                    }
                }
                self.end = end;
                self.bounded = bounded;
                self.short = short;
                self.record_start = record_start;
                self.size_slot = size_slot;
            }
        }
        self.frame.ends[f.slot] = Some(*pos);
        Ok(())
    }

    /// Push a number (and its bits, and its running sum) to `out`, or only keep the sum.
    fn value(
        &mut self,
        f: &FieldPlan,
        raw: Option<i128>,
        bytes: Option<(&[u8], usize)>,
        count: Option<usize>,
        out: Option<&mut Out<'_>>,
    ) {
        let summed = if f.delta == Delta::None {
            None
        } else {
            let raw = raw.unwrap_or(0);
            if Some(raw) == f.sentinel {
                Some(None)
            } else {
                let sum = self.acc[f.delta_index].wrapping_add(raw);
                self.acc[f.delta_index] = sum;
                Some(i64::try_from(sum).ok())
            }
        };
        let Some(out) = out else { return };
        match (summed, bytes) {
            (Some(sum), _) => {
                let col = f.outs[0];
                out.filled[col] = true;
                match sum {
                    Some(v) => out.sinks[col].push_bytes(&v.to_le_bytes()),
                    None => out.sinks[col].push_null(),
                }
            }
            (None, Some((bytes, width))) => push_fixed(f, bytes, width, count, out),
            (None, None) => {}
        }
        push_bits(f, raw, out);
    }

    /// Read one record at `pos`, before `chunk_end`; `pos` ends after it.
    fn record(
        &mut self,
        data: &[u8],
        pos: &mut usize,
        chunk_end: usize,
        out: Option<&mut Out<'_>>,
        time: Option<i64>,
    ) -> Result<Got, Stop> {
        let Some(only) = self.plan.only else {
            return Ok(match self.record_inner(data, pos, chunk_end, out, time)? {
                None => Got::None,
                Some(_) => Got::Row,
            });
        };
        // One variant read alone: the record is walked to learn its variant, and walked
        // again into the columns when it is the one.
        let (start, acc, skipped) = (*pos, self.acc.clone(), self.skipped);
        match self.record_inner(data, pos, chunk_end, None, time)? {
            None => Ok(Got::None),
            Some(Some(v)) if v == only => {
                if out.is_some() {
                    *pos = start;
                    self.acc = acc;
                    self.skipped = skipped;
                    self.record_inner(data, pos, chunk_end, out, time)?;
                }
                Ok(Got::Row)
            }
            Some(_) => Ok(Got::Skipped),
        }
    }

    /// Read one record: `None` when there is none left (for sync framing, no marker),
    /// else the index of its variant, if it has one.
    fn record_inner(
        &mut self,
        data: &[u8],
        pos: &mut usize,
        chunk_end: usize,
        mut out: Option<&mut Out<'_>>,
        time: Option<i64>,
    ) -> Result<Option<Option<usize>>, Stop> {
        let plan = self.plan;
        if *pos >= chunk_end {
            return Ok(None);
        }
        if !plan.sync.is_empty() {
            let rest = &data[*pos..chunk_end];
            if !rest.starts_with(&plan.sync) {
                match memchr::memmem::find(rest, &plan.sync) {
                    Some(at) => {
                        self.skipped += at as u64;
                        *pos += at;
                    }
                    None => {
                        self.skipped += rest.len() as u64;
                        *pos = chunk_end;
                        return Ok(None);
                    }
                }
            }
            *pos += plan.sync.len();
        }
        let first = plan.chunk_slots;
        self.frame.clear(first..plan.slots);
        self.record_start = *pos;
        self.end = chunk_end;
        self.bounded = false;
        self.short = false;
        self.size_slot = None;
        match plan.size {
            Some(SizeRef::Given(n)) => {
                let end = pos.checked_add(n).ok_or(Stop::Truncated)?;
                if end > chunk_end {
                    return Err(Stop::Truncated);
                }
                self.end = end;
                self.bounded = true;
            }
            Some(SizeRef::Slot { slot, adjust }) => self.size_slot = Some((slot, adjust)),
            _ => {}
        }
        let start = *pos;
        let mut p = start;
        self.walk(&plan.common, data, &mut p, out.as_deref_mut())?;
        let mut label = None;
        let mut chosen = None;
        if let Some((slot, text)) = plan.type_slot {
            let int = self.frame.ints[slot];
            let shown = if text {
                match (self.frame.starts[slot], self.frame.ends[slot]) {
                    (Some(a), Some(b)) => Some(crate::fixed_records::text(&data[a..b])),
                    _ => None,
                }
            } else {
                None
            };
            let variant = plan
                .variants
                .iter()
                .enumerate()
                .find(|(_, v)| match (&shown, int) {
                    (Some(t), _) => v.texts.iter().any(|w| w == t),
                    (None, Some(i)) => v.ints.contains(&i),
                    _ => false,
                });
            match variant {
                Some((index, variant)) => {
                    chosen = Some(index);
                    if let Some(size) = variant.size
                        && !self.bounded
                    {
                        let n = self.size_of(size, p, "size")?;
                        let end = start.checked_add(n).ok_or(Stop::Truncated)?;
                        if end > chunk_end || end < p {
                            return Err(if end > chunk_end {
                                Stop::Truncated
                            } else {
                                Stop::Said(format!(
                                    "variant {} at byte {start} is {n} bytes, less than its common fields",
                                    variant.name
                                ))
                            });
                        }
                        self.end = end;
                        self.bounded = true;
                    }
                    self.walk(&variant.fields, data, &mut p, out.as_deref_mut())?;
                    label = Some(variant.name.clone());
                }
                None => {
                    let shown = shown.or_else(|| int.map(|i| i.to_string()));
                    if !self.bounded {
                        return Err(Stop::Said(format!(
                            "the record at byte {start} has type {}, which no variant names",
                            shown.as_deref().unwrap_or("(none)")
                        )));
                    }
                    label = shown.map(|s| Arc::from(format!("?{s}")));
                }
            }
        }
        self.variant = chosen;
        *pos = if self.bounded { self.end } else { p };
        if let Some((width, big)) = plan.suffix {
            let range = self.take_at(*pos, width, chunk_end)?;
            let again = crate::fixed_records::read_unsigned(&data[range], big);
            let (slot, _) = self.size_slot.unwrap_or((usize::MAX, 0));
            let first = self.frame.ints.get(slot).copied().flatten();
            if first != Some(i128::from(again)) {
                return Err(Stop::Said(format!(
                    "the record at byte {start} ends with length {again}, not the {} it starts with",
                    first.map_or_else(|| "?".to_string(), |v| v.to_string())
                )));
            }
            *pos += width;
        }
        if let Some(out) = out {
            if let (Some(col), Some(label)) = (plan.type_out, label)
                && let Sink::Label(v) = &mut out.sinks[col]
            {
                v.push(Some(label));
                out.filled[col] = true;
            }
            if let Some(check) = &plan.checksum {
                let from = check.from.map_or(Some(start), |s| self.frame.starts[s]);
                let to = self.frame.starts[check.to];
                let stored = self.frame.ints[check.field];
                let ok = match (from, to, stored) {
                    (Some(a), Some(b), Some(v)) if a <= b => {
                        Some(i128::from(check.algo.compute(&data[a..b])) == v)
                    }
                    _ => None,
                };
                if let Sink::Flag(v) = &mut out.sinks[check.out] {
                    v.push(ok);
                    out.filled[check.out] = true;
                }
            }
            if let Some(col) = plan.time_out
                && let Sink::Time(v) = &mut out.sinks[col]
            {
                v.push(time);
                out.filled[col] = true;
            }
            out.finish_row();
        }
        Ok(Some(chosen))
    }

    fn take_at(&self, pos: usize, n: usize, end: usize) -> Result<Range<usize>, Stop> {
        let stop = pos.checked_add(n).ok_or(Stop::Truncated)?;
        if stop > end {
            return Err(Stop::Truncated);
        }
        Ok(pos..stop)
    }
}

/// The bit fields of `f`'s value `raw` to their columns.
fn push_bits(f: &FieldPlan, raw: Option<i128>, out: &mut Out<'_>) {
    for (col, bit, width) in &f.bits {
        out.filled[*col] = true;
        match (raw, &mut out.sinks[*col]) {
            (Some(raw), Sink::Bits { values, .. }) => {
                let mask = if *width >= 64 {
                    u64::MAX
                } else {
                    (1u64 << width) - 1
                };
                values.push(Some(((raw as u64) >> bit) & mask));
            }
            (_, sink) => sink.push_null(),
        }
    }
}

/// Nulls in every column of `f`.
fn null_outs(f: &FieldPlan, out: &mut Out<'_>) {
    for col in f.outs.iter().chain(f.bits.iter().map(|(c, _, _)| c)) {
        if !out.filled[*col] {
            out.sinks[*col].push_null();
            out.filled[*col] = true;
        }
    }
}

fn push_fixed(f: &FieldPlan, bytes: &[u8], width: usize, count: Option<usize>, out: &mut Out<'_>) {
    if f.outs.len() == 1 {
        let col = f.outs[0];
        out.filled[col] = true;
        match (&mut out.sinks[col], count) {
            (
                Sink::List {
                    inner,
                    offsets,
                    valid,
                },
                Some(n),
            ) => {
                for i in 0..n {
                    inner.push_bytes(&bytes[i * width..(i + 1) * width]);
                }
                offsets.push(offsets.last().copied().unwrap_or(0) + n as i64);
                valid.push(true);
            }
            (sink, _) => sink.push_bytes(bytes),
        }
    } else {
        for (i, col) in f.outs.iter().enumerate() {
            out.filled[*col] = true;
            out.sinks[*col].push_bytes(&bytes[i * width..(i + 1) * width]);
        }
    }
}

/// Bytes of one chunk: a range of the map, or a decompressed block.
enum ChunkData {
    Map(Range<usize>),
    Owned(Arc<Vec<u8>>),
}

/// Where a read is: in which chunk, where, and how many of its records it has taken.
struct Cursor {
    pos: usize,
    data_start: usize,
    end: usize,
    taken: u64,
    limit: Option<u64>,
    time: Option<i64>,
}

/// The rows a read wants, and how many it has.
struct Want {
    start: u64,
    len: usize,
    produced: usize,
}

impl Want {
    /// A null row at `row`, if it is one wanted.
    fn null_row(&mut self, row: u64, out: &mut Out<'_>) {
        if row >= self.start && self.produced < self.len {
            out.finish_row();
            self.produced += 1;
        }
    }
}

/// What opening found to say: trailing bytes, skipped bytes, records cut short.
#[derive(Default)]
struct Found {
    notes: Vec<String>,
    skipped: u64,
}

impl FramedRecords {
    /// Read `spec`'s records from `bytes[data]`, with `header` read already.
    pub fn open(
        spec: &Spec,
        bytes: Arc<Bytes>,
        header: &HeaderValues,
        data: Range<usize>,
        named: &str,
        path: Option<&std::path::Path>,
    ) -> Result<(Self, Vec<String>), String> {
        let file = bytes.as_slice();
        let mut compiler = Compiler {
            spec,
            header,
            data: file,
            slots: 0,
            deltas: Vec::new(),
        };
        let mut columns = Vec::new();
        let mut chunk_scope = Scope::default();
        let mut discard = Vec::new();
        let chunk_header = match &spec.capture {
            Some(capture) => {
                compiler.fields(&capture.header, &mut chunk_scope, &mut discard, false)?
            }
            None => Vec::new(),
        };
        let chunk_count = spec
            .capture
            .as_ref()
            .and_then(|c| c.count.as_ref())
            .and_then(|n| chunk_scope.slot(n));
        let chunk_slots = compiler.slots;
        let time_out = spec
            .capture
            .as_ref()
            .and_then(|c| c.time.as_ref())
            .map(|name| {
                Compiler::column(
                    &mut columns,
                    name,
                    DataType::Datetime(TimeUnit::Nanoseconds, None),
                    Sink::Time(Vec::new()),
                    false,
                )
            });
        let records = &spec.records;
        let mut scope = Scope::default();
        let common = compiler.fields(&records.fields, &mut scope, &mut columns, false)?;
        let type_slot = records.type_field.as_ref().map(|name| {
            let field = records
                .fields
                .iter()
                .find(|f| f.name.as_deref() == Some(name.as_str()))
                .expect("checked at parse");
            (
                scope.slot(name).expect("a common field"),
                field.ty.is_text(),
            )
        });
        let only = match &spec.variant {
            Some(name) => Some(
                records
                    .variants
                    .iter()
                    .position(|v| &v.name == name)
                    .ok_or_else(|| format!("no variant named {name}"))?,
            ),
            None => None,
        };
        let type_out = type_slot.filter(|_| only.is_none()).map(|_| {
            Compiler::column(
                &mut columns,
                "type",
                DataType::from_categories(Categories::global()),
                Sink::Label(Vec::new()),
                false,
            )
        });
        let size = records
            .size
            .as_ref()
            .map(|s| compiler.size(s, &scope, "record size"))
            .transpose()?;
        let mut variants = Vec::new();
        let mut unread = Vec::new();
        for (i, variant) in records.variants.iter().enumerate() {
            let mut vscope = scope.clone();
            let into = if only.is_some_and(|o| o != i) {
                &mut unread
            } else {
                &mut columns
            };
            let fields = compiler.fields(&variant.fields, &mut vscope, into, true)?;
            let size = variant
                .size
                .as_ref()
                .map(|s| compiler.size(s, &vscope, "variant size"))
                .transpose()?;
            if matches!(size, Some(SizeRef::Slot { .. })) {
                return Err(format!(
                    "variant {}: its size comes from the header or is written in the spec",
                    variant.name
                ));
            }
            let mut ints = Vec::new();
            let mut texts = Vec::new();
            for when in &variant.when {
                match when {
                    formats::Expected::Int(v) => ints.push(*v),
                    // Text fields read with their padding trimmed: `"fmt "` is `fmt`.
                    formats::Expected::Text(t) => {
                        texts.push(t.trim_end_matches(['\0', ' ']).to_string())
                    }
                }
            }
            variants.push(VariantPlan {
                name: Arc::from(variant.name.as_str()),
                ints,
                texts,
                fields,
                size,
            });
        }
        let suffix = if records.length_suffix {
            let Some(Amount::Record { field, .. }) = &records.size else {
                return Err("length_suffix: needs size from a field".into());
            };
            let target = records
                .fields
                .iter()
                .find(|f| f.name.as_deref() == Some(field.as_str()))
                .ok_or_else(|| format!("length_suffix: `{field}` is not a common field"))?;
            let width = target
                .ty
                .width()
                .ok_or("length_suffix: the length field is fixed-width")?;
            let big = target.endian.unwrap_or(spec.endian) == formats::Endian::Big;
            Some((width as usize, big))
        } else {
            None
        };
        let checksum = records
            .checksum
            .as_ref()
            .map(|c| -> Result<ChecksumPlan, String> {
                // A field of a variant has a slot of its own in each scope; the checksum
                // names one by name, so look in every variant's.
                let slot_of = |name: &str| -> Option<usize> {
                    scope.slot(name).or_else(|| {
                        variants
                            .iter()
                            .find_map(|v| v.fields.iter().find(|f| f.name == name).map(|f| f.slot))
                    })
                };
                let field = slot_of(&c.field).ok_or("checksum: no such field")?;
                let to =
                    c.to.as_deref()
                        .map_or(Some(field), slot_of)
                        .ok_or("checksum: no such field")?;
                let from = c
                    .from
                    .as_deref()
                    .map(slot_of)
                    .map(|s| s.ok_or("checksum: no such field"))
                    .transpose()?;
                let out = Compiler::column(
                    &mut columns,
                    "checksum_ok",
                    DataType::Boolean,
                    Sink::Flag(Vec::new()),
                    false,
                );
                Ok(ChecksumPlan {
                    algo: c.algo,
                    field,
                    from,
                    to,
                    out,
                })
            })
            .transpose()?;
        if columns.is_empty() {
            return Err("the records have no named field".into());
        }
        // Record size known up front: written, or what fixed fields take.
        let fixed_size = match size {
            Some(SizeRef::Given(n)) => Some(n),
            None if variants.is_empty() => common.iter().try_fold(0usize, |sum, f| {
                let width = match (&f.kind, f.count) {
                    (Kind::Fixed { width, .. }, None) => *width,
                    (Kind::Fixed { width, .. }, Some(SizeRef::Given(n))) => width.checked_mul(n)?,
                    (
                        Kind::Pad {
                            size: SizeRef::Given(n),
                        },
                        None,
                    ) => *n,
                    _ => return None,
                };
                sum.checked_add(width)
            }),
            _ => None,
        };
        let size = match (size, fixed_size, records.framing) {
            (None, Some(n), Framing::Fixed)
                if !common.iter().any(|f| matches!(f.kind, Kind::Group { .. })) =>
            {
                Some(SizeRef::Given(n))
            }
            (size, _, _) => size,
        };
        if matches!(size, Some(SizeRef::Given(0))) && records.sync.is_empty() {
            return Err("a record takes no bytes".into());
        }
        let plan = Plan {
            framing: records.framing,
            common,
            type_slot,
            variants,
            type_out,
            size,
            suffix,
            align: records.align as usize,
            sync: records.sync.clone(),
            checksum,
            chunk_header,
            chunk_count,
            chunk_slots,
            time_out,
            slots: compiler.slots,
            deltas: compiler.deltas,
            columns,
            only,
            sources: Vec::new(),
        };
        let plan = Plan {
            sources: sources(&plan),
            ..plan
        };
        let mut found = Found::default();
        let chunks = if let Some(capture) = &spec.capture {
            let _ = capture;
            crate::framed_records::capture::packets(file, &mut found.notes)?
        } else if let Some(blocks) = &spec.blocks {
            list_blocks(spec, blocks, header, file, data.clone(), &mut found.notes)?
        } else {
            vec![Chunk {
                source: ChunkSource::Map(data.clone()),
                records: None,
                time_ns: None,
            }]
        };
        let schema: Schema = plan
            .columns
            .iter()
            .map(|c| polars::prelude::Field::new(c.name.clone(), c.dtype.clone()))
            .collect();
        let mut records_read = Self {
            bytes,
            plan: Arc::new(plan),
            chunks,
            index: Arc::new(Index::Walk(Vec::new())),
            table: None,
            rows: 0,
            schema: Arc::new(schema),
            cache: Mutex::new(VecDeque::new()),
        };
        let ring = records
            .ring
            .as_ref()
            .map(|r| header.resolve_any(r, "ring"))
            .transpose()?;
        let count = records
            .count
            .as_ref()
            .map(|c| header.resolve_any(c, "count"))
            .transpose()?;
        // The walk of this file with this spec, kept from an earlier open of it.
        let kept = path
            .and_then(crate::indexed::peek::<KeptWalk>)
            .filter(|k| k.spec == *spec && k.spec.variant == spec.variant && k.data == data);
        if let Some(kept) = kept {
            records_read.index = kept.index.clone();
            records_read.table = kept.table.clone();
            records_read.rows = kept.rows;
            found.notes.extend(kept.notes.iter().cloned());
            return Ok((records_read, found.notes));
        }
        let before = found.notes.len();
        records_read.build_index(named, ring, count, &mut found)?;
        if found.skipped > 0 {
            found.notes.push(format!(
                "{} {} skipped between records, to the next sync marker",
                found.skipped,
                if found.skipped == 1 { "byte" } else { "bytes" }
            ));
        }
        if let Some(path) = path
            && matches!(*records_read.index, Index::Walk(_))
        {
            crate::indexed::keep(
                path,
                Arc::new(KeptWalk {
                    spec: spec.clone(),
                    data,
                    index: records_read.index.clone(),
                    table: records_read.table.clone(),
                    rows: records_read.rows,
                    notes: found.notes[before..].to_vec(),
                }),
            );
        }
        Ok((records_read, found.notes))
    }

    /// Whether a stride of `size` bytes finds every record: one run of fixed records.
    fn stride(&self) -> Option<usize> {
        let plan = &self.plan;
        match (plan.size, &self.chunks[..]) {
            (Some(SizeRef::Given(n)), [chunk])
                if plan.framing == Framing::Fixed
                    && plan.only.is_none()
                    && plan.sync.is_empty()
                    && plan.suffix.is_none()
                    // A running sum needs every record before the one read.
                    && plan.deltas.is_empty()
                    && matches!(chunk.source, ChunkSource::Map(_))
                    && plan.chunk_header.is_empty() =>
            {
                let align = plan.align.max(1);
                Some(n.div_ceil(align) * align)
            }
            _ => None,
        }
    }

    fn build_index(
        &mut self,
        named: &str,
        ring: Option<u64>,
        limit: Option<u64>,
        found: &mut Found,
    ) -> Result<(), String> {
        let file = self.bytes.as_slice();
        if let Some(size) = self.stride() {
            let ChunkSource::Map(range) = self.chunks[0].source.clone() else {
                unreachable!("a stride is over the map")
            };
            let len = range.len();
            let mut rows = len / size;
            // The last record need not be padded out to the alignment.
            let unpadded = self.plan.size.map_or(size, |s| match s {
                SizeRef::Given(n) => n,
                _ => size,
            });
            if len % size >= unpadded {
                rows += 1;
            }
            if let Some(count) = limit {
                if count < rows as u64 {
                    rows = count as usize;
                } else if count > rows as u64 {
                    found.notes.push(format!(
                        "header says {count} records {} {rows} whole ones shown",
                        crate::glyphs::get().middot
                    ));
                }
            } else {
                let used = (rows * size).min(len);
                let trailing = len - used;
                if trailing > 0 && len % size < unpadded {
                    found
                        .notes
                        .push(trailing_note(named, &file[range.end - trailing..range.end]));
                }
            }
            let rows = rows.min(MAX_ROWS);
            let ring = match ring {
                Some(r) if rows > 0 => {
                    if r >= rows as u64 {
                        found.notes.push(format!(
                            "ring's oldest record {r} past the {rows} records {} read from the first",
                            crate::glyphs::get().middot
                        ));
                        0
                    } else {
                        r as usize
                    }
                }
                _ => 0,
            };
            self.index = Arc::new(Index::Stride {
                start: range.start,
                size,
                ring,
            });
            self.rows = rows;
            return Ok(());
        }
        let plan = self.plan.clone();
        let mut walker = Walker::new(&plan, file);
        let mut checkpoints = Vec::new();
        let mut rows: u64 = 0;
        let max_rows = MAX_ROWS as u64;
        let needs_walk_everything = plan.deltas.contains(&Delta::All);
        // Read once: the limits sit behind a lock, and this loop runs per record.
        let indexed_records = crate::limits::get().indexed_records;
        let mut table = (self.chunks.len() == 1
            && matches!(self.chunks[0].source, ChunkSource::Map(_))
            && plan.chunk_header.is_empty()
            && plan.variants.len() < usize::from(WALK))
        .then(|| RowTable {
            starts: crate::indexed::Offsets::for_file(file.len()),
            tags: Vec::new(),
        });
        'chunks: for ci in 0..self.chunks.len() {
            if limit.is_some_and(|l| rows >= l) || rows >= max_rows {
                break;
            }
            reset_block_sums(&plan, &mut walker.acc);
            if let Some(n) = self.chunks[ci].records
                && !needs_walk_everything
            {
                checkpoints.push(Checkpoint {
                    row: rows,
                    chunk: ci as u32,
                    pos: u32::MAX,
                    taken: 0,
                    acc: walker.acc.clone().into_boxed_slice(),
                });
                rows = rows
                    .saturating_add(n)
                    .min(limit.unwrap_or(u64::MAX))
                    .min(max_rows);
                continue;
            }
            let (data, mut cursor) = match self.enter(ci, &mut walker) {
                Ok(c) => c,
                Err(e) => {
                    found.notes.push(format!("block {ci}: {e}; left out"));
                    continue;
                }
            };
            let chunk_bytes = self.slice(&data, file);
            loop {
                if cursor.limit.is_some_and(|l| cursor.taken >= l)
                    || limit.is_some_and(|l| rows >= l)
                    || rows >= max_rows
                {
                    break;
                }
                if cursor.taken.is_multiple_of(CHECKPOINT) {
                    checkpoints.push(Checkpoint {
                        row: rows,
                        chunk: ci as u32,
                        pos: cursor.pos as u32,
                        taken: cursor.taken as u32,
                        acc: walker.acc.clone().into_boxed_slice(),
                    });
                }
                let before = cursor.pos;
                match walker.record(chunk_bytes, &mut cursor.pos, cursor.end, None, None) {
                    Ok(got @ (Got::Row | Got::Skipped)) => {
                        if got == Got::Row {
                            rows += 1;
                            if let Some(t) = table.as_mut() {
                                if t.tags.len() >= indexed_records {
                                    table = None;
                                } else {
                                    let tag = match (walker.short, plan.type_slot) {
                                        (true, _) => WALK,
                                        (false, None) => 0,
                                        (false, Some(_)) => walker
                                            .variant
                                            .and_then(|v| u8::try_from(v).ok())
                                            .unwrap_or(WALK),
                                    };
                                    t.starts.push(walker.record_start - plan.sync.len());
                                    t.tags.push(tag);
                                }
                            }
                        }
                        cursor.taken += 1;
                        align(&mut cursor, plan.align);
                        if cursor.pos <= before {
                            found.notes.push(format!(
                                "zero-length record at byte {before} {} rest left out",
                                crate::glyphs::get().middot
                            ));
                            break 'chunks;
                        }
                    }
                    Ok(Got::None) => {
                        // A checkpoint pushed for a record that is not there.
                        if checkpoints.last().is_some_and(|c: &Checkpoint| {
                            c.row == rows && c.chunk == ci as u32 && c.pos == before as u32
                        }) {
                            checkpoints.pop();
                        }
                        break;
                    }
                    Err(stop) => {
                        if checkpoints.last().is_some_and(|c: &Checkpoint| {
                            c.row == rows && c.chunk == ci as u32 && c.pos == before as u32
                        }) {
                            checkpoints.pop();
                        }
                        let what = if self.chunks.len() > 1 {
                            format!("{named}, block {ci},")
                        } else {
                            named.to_string()
                        };
                        match stop {
                            Stop::Truncated => {
                                let rest = &chunk_bytes[before..cursor.end];
                                found.notes.push(trailing_note(&what, rest));
                            }
                            Stop::Said(said) => {
                                found.notes.push(format!(
                                    "{said} {} {} bytes from there left out",
                                    crate::glyphs::get().middot,
                                    cursor.end - before
                                ));
                            }
                        }
                        if self.chunks.len() == 1 {
                            break 'chunks;
                        }
                        break;
                    }
                }
                if cursor.pos >= cursor.end {
                    break;
                }
            }
            // A chunk that counts its records and holds fewer is filled with nulls.
            if let Some(l) = cursor.limit
                && cursor.taken < l
                && self.chunks[ci].records.is_some()
            {
                found.notes.push(format!(
                    "block {ci}: {l} records declared, {} found",
                    cursor.taken
                ));
            }
        }
        if let Some(l) = limit
            && rows < l
        {
            found.notes.push(format!(
                "header says {l} records {} {rows} shown",
                crate::glyphs::get().middot
            ));
        }
        found.skipped += walker.skipped;
        self.rows = rows as usize;
        self.index = Arc::new(Index::Walk(checkpoints));
        self.table = table.filter(|t| t.tags.len() == self.rows).map(|mut t| {
            t.starts.shrink();
            t.tags.shrink_to_fit();
            Arc::new(t)
        });
        Ok(())
    }

    fn slice<'b>(&'b self, data: &'b ChunkData, file: &'b [u8]) -> &'b [u8] {
        match data {
            ChunkData::Map(_) => file,
            ChunkData::Owned(v) => v,
        }
    }

    /// The bytes of chunk `i`: its range of the map, or its block decompressed.
    fn chunk_data(&self, i: usize) -> Result<ChunkData, String> {
        match &self.chunks[i].source {
            ChunkSource::Map(range) => Ok(ChunkData::Map(range.clone())),
            ChunkSource::Block {
                body,
                codec,
                uncompressed,
            } => {
                if let Ok(cache) = self.cache.lock()
                    && let Some((_, data)) = cache.iter().find(|(k, _)| *k == i)
                {
                    return Ok(ChunkData::Owned(data.clone()));
                }
                self.bytes.still_whole().map_err(|e| e.to_string())?;
                let raw = &self.bytes.as_slice()[body.clone()];
                let data = Arc::new(decompress(raw, *codec, *uncompressed)?);
                if let Ok(mut cache) = self.cache.lock() {
                    cache.push_front((i, data.clone()));
                    cache.truncate(CACHED_BLOCKS);
                }
                Ok(ChunkData::Owned(data))
            }
        }
    }

    /// Chunk `i`'s bytes, and a cursor at its start, after its payload header.
    fn enter(&self, i: usize, walker: &mut Walker<'_>) -> Result<(ChunkData, Cursor), String> {
        let data = self.chunk_data(i)?;
        let (start, end) = match &data {
            ChunkData::Map(r) => (r.start, r.end),
            ChunkData::Owned(v) => (0, v.len()),
        };
        let mut cursor = Cursor {
            pos: start,
            data_start: start,
            end,
            taken: 0,
            limit: self.chunks[i].records,
            time: self.chunks[i].time_ns,
        };
        if !self.plan.chunk_header.is_empty() {
            let file = self.bytes.as_slice();
            let bytes = self.slice(&data, file);
            walker.frame.clear(0..self.plan.chunk_slots);
            walker.end = end;
            walker.bounded = false;
            walker.short = false;
            walker.size_slot = None;
            walker.record_start = start;
            let mut p = start;
            match walker.walk(&self.plan.chunk_header, bytes, &mut p, None) {
                Ok(()) => {
                    cursor.pos = p;
                    cursor.data_start = p;
                    if let Some(slot) = self.plan.chunk_count {
                        cursor.limit = walker.frame.ints[slot].map(|v| v.max(0) as u64);
                    }
                }
                Err(_) => {
                    // Too short for its header: nothing in it.
                    cursor.pos = end;
                    cursor.limit = Some(0);
                }
            }
        }
        Ok((data, cursor))
    }

    /// Rows `[start, start + len)`, of the columns `wanted` names, decoded now.
    fn decode(&self, start: usize, len: usize, wanted: &[bool]) -> PolarsResult<DataFrame> {
        self.bytes.still_whole()?;
        let start = start.min(self.rows);
        let len = len.min(self.rows - start);
        let plan = &*self.plan;
        let mut sinks: Vec<Sink> = plan
            .columns
            .iter()
            .zip(wanted)
            .map(|(c, w)| if *w { c.proto.clone() } else { Sink::Skip })
            .collect();
        let mut filled = vec![false; sinks.len()];
        let file = self.bytes.as_slice();
        let mut walker = Walker::new(plan, file);
        match &*self.index {
            Index::Stride {
                start: base,
                size,
                ring,
            } => {
                let mut out = Out {
                    sinks: &mut sinks,
                    filled: &mut filled,
                };
                let ChunkSource::Map(range) = &self.chunks[0].source else {
                    unreachable!("a stride is over the map")
                };
                for row in start..start + len {
                    let k = (ring + row) % self.rows.max(1);
                    let mut pos = base + k * size;
                    let end = (pos + size).min(range.end);
                    if walker
                        .record(file, &mut pos, end, Some(&mut out), None)
                        .is_err()
                    {
                        out.finish_row();
                    }
                }
            }
            Index::Walk(checkpoints) => {
                let at = checkpoints.partition_point(|c| c.row <= start as u64);
                let mut want = Want {
                    start: start as u64,
                    len,
                    produced: 0,
                };
                let mut out = Out {
                    sinks: &mut sinks,
                    filled: &mut filled,
                };
                if let Some(cp) = at.checked_sub(1).map(|i| &checkpoints[i]) {
                    self.read_from(cp, &mut walker, &mut want, &mut out);
                }
                // Rows the index counted that a read now does not find: a file changed
                // under the map, or a block that no longer decompresses.
                for _ in want.produced..len {
                    out.finish_row();
                }
            }
        }
        let columns = sinks
            .into_iter()
            .zip(&plan.columns)
            .zip(wanted)
            .filter(|(_, w)| **w)
            .map(|((sink, c), _)| sink.finish(c.name.clone()).map(Column::from))
            .collect::<PolarsResult<Vec<_>>>()?;
        DataFrame::new(len, columns)
    }

    /// From checkpoint `cp`, skip to the row `want` starts at and read its rows.
    fn read_from(
        &self,
        cp: &Checkpoint,
        walker: &mut Walker<'_>,
        want: &mut Want,
        out: &mut Out<'_>,
    ) {
        let file = self.bytes.as_slice();
        let plan = &*self.plan;
        walker.acc.copy_from_slice(&cp.acc);
        let mut row = cp.row;
        let mut ci = cp.chunk as usize;
        let mut first = true;
        while want.produced < want.len && ci < self.chunks.len() {
            if !first {
                reset_block_sums(plan, &mut walker.acc);
            }
            let (data, mut cursor) = match self.enter(ci, walker) {
                Ok(c) => c,
                Err(_) => {
                    // A block that will not decompress now: its promised rows are null.
                    let n = self.chunks[ci].records.unwrap_or(0);
                    for _ in 0..n {
                        want.null_row(row, out);
                        row += 1;
                    }
                    ci += 1;
                    first = false;
                    continue;
                }
            };
            if first && cp.pos != u32::MAX {
                cursor.pos = cp.pos as usize;
                cursor.taken = u64::from(cp.taken);
            }
            first = false;
            let bytes = self.slice(&data, file);
            while want.produced < want.len {
                if cursor.limit.is_some_and(|l| cursor.taken >= l) || cursor.pos >= cursor.end {
                    break;
                }
                let reading = row >= want.start;
                let got = walker.record(
                    bytes,
                    &mut cursor.pos,
                    cursor.end,
                    if reading { Some(&mut *out) } else { None },
                    cursor.time,
                );
                match got {
                    Ok(Got::Row) => {
                        if reading {
                            want.produced += 1;
                        }
                        row += 1;
                        cursor.taken += 1;
                        align(&mut cursor, plan.align);
                    }
                    Ok(Got::Skipped) => {
                        cursor.taken += 1;
                        align(&mut cursor, plan.align);
                    }
                    Ok(Got::None) | Err(_) => break,
                }
            }
            // A chunk that promised more records than it held: nulls for the rest.
            if let Some(l) = self.chunks[ci].records {
                while cursor.taken < l && want.produced < want.len {
                    want.null_row(row, out);
                    row += 1;
                    cursor.taken += 1;
                }
            }
            ci += 1;
        }
    }

    /// Column `column` of `rows`, each read where the row table says its record starts.
    fn decode_from(
        &self,
        table: &RowTable,
        column: usize,
        rows: &[IdxSize],
    ) -> PolarsResult<Column> {
        self.bytes.still_whole()?;
        let plan = &*self.plan;
        let file = self.bytes.as_slice();
        let ChunkSource::Map(range) = &self.chunks[0].source else {
            unreachable!("a row table is over the map")
        };
        let mut sinks: Vec<Sink> = vec![Sink::Skip; plan.columns.len()];
        sinks[column] = plan.columns[column].proto.clone();
        let mut filled = vec![false; sinks.len()];
        let mut out = Out {
            sinks: &mut sinks,
            filled: &mut filled,
        };
        let mut walker = Walker::new(plan, file);
        if plan.type_out == Some(column) {
            // Each row's variant is its tag: the names are cast once and the rows
            // gathered by tag. A row read field by field says its own label.
            let mut labels: Vec<Option<Arc<str>>> =
                plan.variants.iter().map(|v| Some(v.name.clone())).collect();
            let codes: IdxCa = rows
                .iter()
                .map(|&row| {
                    let row = row as usize;
                    let tag = table.tags[row];
                    if tag != WALK {
                        return Some(IdxSize::from(tag));
                    }
                    let mut pos = table.starts.get(row);
                    let _ = walker.record(file, &mut pos, range.end, Some(&mut out), None);
                    let Sink::Label(said) = &mut out.sinks[column] else {
                        return None;
                    };
                    let label = said.pop().flatten()?;
                    labels.push(Some(label));
                    Some((labels.len() - 1) as IdxSize)
                })
                .collect();
            let name = plan.columns[column].name.clone();
            let labels = Sink::Label(labels).finish(name)?;
            return Ok(labels.take(&codes)?.into_column());
        }
        let sources = &plan.sources[column];
        for &row in rows {
            let row = row as usize;
            let start = table.starts.get(row);
            let tag = table.tags[row];
            let source = match tag {
                WALK => &Source::Walk,
                v => &sources[usize::from(v)],
            };
            match source {
                Source::Null => out.finish_row(),
                Source::Label => {
                    if let Sink::Label(v) = &mut out.sinks[column] {
                        v.push(plan.variants.get(usize::from(tag)).map(|v| v.name.clone()));
                    }
                    out.filled[column] = true;
                    out.finish_row();
                }
                Source::At { offset, field } => {
                    let Kind::Fixed { width, int } = field.kind else {
                        unreachable!("a value at a place is fixed")
                    };
                    let cells = match field.count {
                        Some(SizeRef::Given(n)) => Some(n),
                        _ => None,
                    };
                    let at = start + offset;
                    let bytes = at
                        .checked_add(width * cells.unwrap_or(1))
                        .filter(|end| *end <= range.end)
                        .map(|end| &file[at..end]);
                    if let Some(bytes) = bytes {
                        let raw = int
                            .and_then(|r| (cells != Some(0)).then(|| int_of(&bytes[..width], r)));
                        push_fixed(field, bytes, width, cells, &mut out);
                        push_bits(field, raw, &mut out);
                    }
                    out.finish_row();
                }
                Source::Walk | Source::Summed => {
                    let mut pos = start;
                    // A row the walk does not finish (the file changed under the map)
                    // is null rather than missing.
                    if !matches!(
                        walker.record(file, &mut pos, range.end, Some(&mut out), None),
                        Ok(Got::Row)
                    ) {
                        out.finish_row();
                    }
                }
            }
        }
        let name = plan.columns[column].name.clone();
        Ok(sinks.swap_remove(column).finish(name)?.into_column())
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn sources(&self) -> &[Arc<Bytes>] {
        std::slice::from_ref(&self.bytes)
    }
}

impl FramedRecords {
    /// The frame: decoded over a row index, only what a query reaches.
    pub fn lazy(self: &Arc<Self>) -> LazyFrame {
        crate::row_index::lazy(self)
    }

    /// Rows `[start, start + len)` as a frame of their own, read from the nearest
    /// checkpoint before them.
    pub fn window(&self, start: usize, len: usize) -> PolarsResult<LazyFrame> {
        let all = vec![true; self.plan.columns.len()];
        Ok(self.decode(start, len, &all)?.lazy())
    }

    /// The first `rows` rows of every column, decoded now.
    pub fn collect(&self, rows: usize) -> PolarsResult<DataFrame> {
        let all = vec![true; self.plan.columns.len()];
        self.decode(0, rows, &all)
    }
}

impl crate::row_index::RowSource for FramedRecords {
    fn height(&self) -> usize {
        self.rows
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    // Each column is decoded on its own, so a query that names one column decodes only
    // that one; the streaming engine asks for a morsel at a time. From the row table,
    // a column reads each row where its record starts, as every other column does;
    // without one, each column walks the span its rows cover.
    fn decode(&self, column: usize, index: &IdxCa) -> PolarsResult<Column> {
        let rows = crate::row_index::checked(index, self.rows)?;
        if let Some(table) = &self.table
            && let Some(sources) = self.plan.sources.get(column)
            && !sources.iter().any(|s| matches!(s, Source::Summed))
        {
            return self.decode_from(table, column, &rows);
        }
        let mut wanted = vec![false; self.plan.columns.len()];
        *wanted
            .get_mut(column)
            .ok_or_else(|| polars_err!(OutOfBounds: "no column {column}"))? = true;
        let (Some(&lo), Some(&hi)) = (rows.iter().min(), rows.iter().max()) else {
            return Ok(self.decode(0, 0, &wanted)?.columns()[0].clone());
        };
        let (lo, span) = (lo as usize, (hi - lo) as usize + 1);
        let values = self.decode(lo, span, &wanted)?.columns()[0].clone();
        let contiguous = rows.len() == span && rows.windows(2).all(|w| w[1] == w[0] + 1);
        if contiguous {
            return Ok(values);
        }
        let at = IdxCa::from_vec(
            PlSmallStr::EMPTY,
            rows.iter().map(|&r| r - lo as IdxSize).collect(),
        );
        values.take(&at)
    }
}

impl crate::pushdown::Windowed for FramedRecords {
    fn window(&self, start: usize, len: usize) -> PolarsResult<LazyFrame> {
        FramedRecords::window(self, start, len)
    }
}

/// Put `cursor` at the next multiple of `align` from the chunk's start.
fn align(cursor: &mut Cursor, align: usize) {
    if align > 1 {
        let offset = cursor.pos - cursor.data_start;
        let rounded = offset.div_ceil(align).saturating_mul(align);
        cursor.pos = cursor.data_start.saturating_add(rounded).min(cursor.end);
    }
}

/// Running sums that start again with each block.
fn reset_block_sums(plan: &Plan, acc: &mut [i128]) {
    for (sum, delta) in acc.iter_mut().zip(&plan.deltas) {
        if *delta == Delta::Block {
            *sum = 0;
        }
    }
}

/// A warning naming the bytes at the end that are not a whole record.
fn trailing_note(what: &str, bytes: &[u8]) -> String {
    formats::trailing_note(what, bytes)
}

impl Plan {
    /// A plan of `fields` alone, to read a block header or an index entry.
    fn bare(fields: Vec<FieldPlan>, slots: usize) -> Self {
        Self {
            framing: Framing::Fixed,
            common: fields,
            type_slot: None,
            variants: Vec::new(),
            type_out: None,
            size: None,
            suffix: None,
            align: 1,
            sync: Vec::new(),
            checksum: None,
            chunk_header: Vec::new(),
            chunk_count: None,
            chunk_slots: 0,
            time_out: None,
            slots,
            deltas: Vec::new(),
            columns: Vec::new(),
            only: None,
            sources: Vec::new(),
        }
    }
}

/// Where each column's value is in a record of each variant: at a fixed place while
/// every field before it in the record is fixed in size, else found by a walk.
fn sources(plan: &Plan) -> Vec<Vec<Source>> {
    let variants = plan.variants.len().max(1);
    let mut out = vec![vec![Source::Null; variants]; plan.columns.len()];
    // Read alone, the other variants' fields fill no column of this table.
    for v in (0..variants).filter(|v| plan.only.is_none_or(|only| only == *v)) {
        let fields = plan
            .common
            .iter()
            .chain(plan.variants.get(v).into_iter().flat_map(|p| &p.fields));
        // A row's start is its sync marker's.
        let mut offset = Some(plan.sync.len());
        for f in fields {
            let width = match (&f.kind, f.count) {
                (Kind::Fixed { width, .. }, None) => Some(*width),
                (Kind::Fixed { width, .. }, Some(SizeRef::Given(n))) => width.checked_mul(n),
                (
                    Kind::Pad {
                        size: SizeRef::Given(n),
                    },
                    None,
                ) => Some(*n),
                _ => None,
            };
            let source = match (offset, &f.kind, width) {
                (Some(offset), Kind::Fixed { .. }, Some(_)) if f.delta == Delta::None => {
                    Source::At {
                        offset,
                        field: f.clone(),
                    }
                }
                _ => Source::Walk,
            };
            for &c in &f.outs {
                out[c][v] = if f.delta == Delta::None {
                    source.clone()
                } else {
                    Source::Summed
                };
            }
            for (c, _, _) in &f.bits {
                out[*c][v] = source.clone();
            }
            offset = offset.zip(width).and_then(|(o, w)| o.checked_add(w));
        }
    }
    if let Some(c) = plan.type_out {
        out[c].fill(Source::Label);
    }
    for c in plan.checksum.iter().map(|c| c.out).chain(plan.time_out) {
        out[c].fill(Source::Walk);
    }
    out
}

/// Fields read once, for their values: a block header, an index entry.
struct Struct {
    plan: Plan,
    scope: Scope,
}

impl Struct {
    fn compile(
        spec: &Spec,
        header: &HeaderValues,
        file: &[u8],
        fields: &[Field],
    ) -> Result<Self, String> {
        let mut compiler = Compiler {
            spec,
            header,
            data: file,
            slots: 0,
            deltas: Vec::new(),
        };
        let mut scope = Scope::default();
        let mut columns = Vec::new();
        let plans = compiler.fields(fields, &mut scope, &mut columns, false)?;
        Ok(Self {
            plan: Plan::bare(plans, compiler.slots),
            scope,
        })
    }

    /// Read the fields at `pos`, before `end`: the frame, and where they end.
    fn read(&self, file: &[u8], pos: usize, end: usize) -> Result<(Frame, usize), Stop> {
        let mut walker = Walker::new(&self.plan, file);
        walker.end = end;
        walker.record_start = pos;
        let mut p = pos;
        walker.walk(&self.plan.common, file, &mut p, None)?;
        Ok((walker.frame, p))
    }

    fn value(&self, frame: &Frame, name: &str) -> Option<i128> {
        frame.ints[self.scope.slot(name)?]
    }
}

/// The blocks of `data`: from the index the file keeps, or by walking their headers.
fn list_blocks(
    spec: &Spec,
    blocks: &formats::Blocks,
    header: &HeaderValues,
    file: &[u8],
    data: Range<usize>,
    notes: &mut Vec<String>,
) -> Result<Vec<Chunk>, String> {
    let head = Struct::compile(spec, header, file, &blocks.header)?;
    let size = {
        let compiler = Compiler {
            spec,
            header,
            data: file,
            slots: 0,
            deltas: Vec::new(),
        };
        compiler.size(&blocks.size, &head.scope, "block size")?
    };
    let codec_of = |frame: &Frame| -> Result<Compression, String> {
        match &blocks.codec {
            Codec::Fixed(c) => Ok(*c),
            Codec::ByField { field, values } => {
                let code = head
                    .value(frame, field)
                    .ok_or("the block header has no codec")?;
                i64::try_from(code)
                    .ok()
                    .and_then(|c| values.get(&c).copied())
                    .ok_or_else(|| format!("codec {code} is not one compression names"))
            }
        }
    };
    let mut chunks = Vec::new();
    let read_block = |at: usize,
                      end: usize,
                      rows: Option<u64>,
                      chunks: &mut Vec<Chunk>,
                      notes: &mut Vec<String>|
     -> Result<Option<usize>, String> {
        let (frame, body_start) = match head.read(file, at, end) {
            Ok(x) => x,
            Err(_) => {
                notes.push(format!(
                    "the block header at byte {at} runs past the data; the rest is left out"
                ));
                return Ok(None);
            }
        };
        let body_len = match size {
            SizeRef::Given(n) => n,
            SizeRef::Slot { slot, adjust } => {
                let v = frame.ints[slot].unwrap_or(0) + i128::from(adjust);
                usize::try_from(v)
                    .ok()
                    .filter(|n| *n <= MAX_BLOCK)
                    .ok_or_else(|| {
                        format!(
                            "the block at byte {at} gives its size as {v}, outside 0 to {MAX_BLOCK}"
                        )
                    })?
            }
            SizeRef::Rest => end - body_start,
        };
        let body_end = body_start.checked_add(body_len).filter(|e| *e <= end);
        let Some(body_end) = body_end else {
            notes.push(format!(
                "the block at byte {at} is {body_len} bytes and runs past the data; the rest is left out"
            ));
            return Ok(None);
        };
        let codec = match codec_of(&frame) {
            Ok(c) => c,
            Err(e) => {
                notes.push(format!("the block at byte {at}: {e}; left out"));
                return Ok(Some(body_end));
            }
        };
        let records = rows.or_else(|| {
            blocks
                .records
                .as_ref()
                .and_then(|r| head.value(&frame, r))
                .map(|v| v.max(0) as u64)
        });
        let uncompressed = blocks
            .uncompressed
            .as_ref()
            .and_then(|u| head.value(&frame, u))
            .and_then(|v| usize::try_from(v).ok());
        chunks.push(Chunk {
            source: if codec == Compression::None {
                ChunkSource::Map(body_start..body_end)
            } else {
                ChunkSource::Block {
                    body: body_start..body_end,
                    codec,
                    uncompressed,
                }
            },
            records,
            time_ns: None,
        });
        Ok(Some(body_end))
    };
    match &blocks.index {
        Some(index) => {
            let entry = Struct::compile(spec, header, file, &index.fields)?;
            let at = usize::try_from(header.resolve_any(&index.at, "index at")?)
                .map_err(|_| "index: too far")?;
            let count = header.resolve_any(&index.count, "index count")?;
            let width = formats::fields_width(&index.fields).unwrap_or(1).max(1) as usize;
            let fits = file.len().saturating_sub(at) / width;
            if count > fits as u64 {
                return Err(format!(
                    "the block index at byte {at} says {count} entries; the file has room for {fits}"
                ));
            }
            let mut pos = at;
            for _ in 0..count {
                let (frame, next) = head_or(entry.read(file, pos, file.len()))?;
                pos = next;
                let offset = entry.value(&frame, "offset").unwrap_or(-1);
                let rows = entry.value(&frame, "rows").map(|v| v.max(0) as u64);
                let Some(offset) = usize::try_from(offset).ok().filter(|o| *o < file.len()) else {
                    notes.push(format!(
                        "an index entry points at byte {offset}, outside the file; left out"
                    ));
                    continue;
                };
                read_block(offset, file.len(), rows, &mut chunks, notes)?;
            }
        }
        None => {
            let mut pos = data.start;
            while pos < data.end {
                match read_block(pos, data.end, None, &mut chunks, notes)? {
                    Some(next) if next > pos => pos = next,
                    Some(_) => {
                        notes.push(format!(
                            "zero-length block at byte {pos} {} rest left out",
                            crate::glyphs::get().middot
                        ));
                        break;
                    }
                    None => break,
                }
            }
        }
    }
    Ok(chunks)
}

fn head_or(read: Result<(Frame, usize), Stop>) -> Result<(Frame, usize), String> {
    read.map_err(|_| "an index entry runs past the end of the file".to_string())
}

/// `raw` decompressed by `codec`, at most [`MAX_BLOCK`] bytes.
pub fn decompress(
    raw: &[u8],
    codec: Compression,
    uncompressed: Option<usize>,
) -> Result<Vec<u8>, String> {
    use std::io::Read;
    let read_all = |mut reader: Box<dyn Read + '_>| -> Result<Vec<u8>, String> {
        let mut out = Vec::new();
        reader
            .by_ref()
            .take(MAX_BLOCK as u64 + 1)
            .read_to_end(&mut out)
            .map_err(|e| format!("{} block: {e}", codec.name()))?;
        if out.len() > MAX_BLOCK {
            return Err(format!(
                "a block decompresses to more than {MAX_BLOCK} bytes"
            ));
        }
        Ok(out)
    };
    match codec {
        Compression::None => Ok(raw.to_vec()),
        Compression::Gzip => read_all(Box::new(flate2::read::MultiGzDecoder::new(raw))),
        Compression::Deflate => read_all(Box::new(flate2::read::DeflateDecoder::new(raw))),
        Compression::Zlib => read_all(Box::new(flate2::read::ZlibDecoder::new(raw))),
        Compression::Zstd => read_all(Box::new(
            zstd::Decoder::new(raw).map_err(|e| format!("zstd block: {e}"))?,
        )),
        Compression::Lz4 => read_all(Box::new(
            lz4::Decoder::new(raw).map_err(|e| format!("lz4 block: {e}"))?,
        )),
        Compression::Lz4Block => {
            let size = uncompressed.filter(|n| *n <= MAX_BLOCK).ok_or_else(|| {
                format!("an lz4 block needs its decompressed size, at most {MAX_BLOCK}")
            })?;
            lz4::block::decompress(raw, Some(size as i32)).map_err(|e| format!("lz4 block: {e}"))
        }
        Compression::Snappy => {
            let size = snap::raw::decompress_len(raw).map_err(|e| format!("snappy block: {e}"))?;
            if size > MAX_BLOCK {
                return Err(format!(
                    "a block decompresses to more than {MAX_BLOCK} bytes"
                ));
            }
            snap::raw::Decoder::new()
                .decompress_vec(raw)
                .map_err(|e| format!("snappy block: {e}"))
        }
        Compression::SnappyFramed => read_all(Box::new(snap::read::FrameDecoder::new(raw))),
        Compression::Brotli => read_all(Box::new(brotli::Decompressor::new(raw, 4096))),
        Compression::Bzip2 => read_all(Box::new(bzip2::read::BzDecoder::new(raw))),
        Compression::Xz => read_all(Box::new(xz2::read::XzDecoder::new(raw))),
    }
}

/// Packet captures (pcap and pcapng): the UDP payload of each packet, with its time.
pub mod capture {
    use super::{Chunk, ChunkSource};

    /// The most packets one capture is read for.
    const MAX_PACKETS: usize = 64 << 20;

    fn u16_at(b: &[u8], at: usize, big: bool) -> Option<u16> {
        let raw: [u8; 2] = b.get(at..at + 2)?.try_into().ok()?;
        Some(if big {
            u16::from_be_bytes(raw)
        } else {
            u16::from_le_bytes(raw)
        })
    }

    fn u32_at(b: &[u8], at: usize, big: bool) -> Option<u32> {
        let raw: [u8; 4] = b.get(at..at + 4)?.try_into().ok()?;
        Some(if big {
            u32::from_be_bytes(raw)
        } else {
            u32::from_le_bytes(raw)
        })
    }

    /// Whether `head` starts a pcap or pcapng file.
    pub fn is_capture(head: &[u8]) -> bool {
        matches!(
            head.get(..4),
            Some(
                [0xd4, 0xc3, 0xb2, 0xa1]
                    | [0xa1, 0xb2, 0xc3, 0xd4]
                    | [0x4d, 0x3c, 0xb2, 0xa1]
                    | [0xa1, 0xb2, 0x3c, 0x4d]
                    | [0x0a, 0x0d, 0x0d, 0x0a]
            )
        )
    }

    /// Where the UDP payload of a frame of `link` type is in `frame`, relative to it.
    pub fn udp_payload(frame: &[u8], link: u32) -> Option<std::ops::Range<usize>> {
        // The link layer: where the IP packet starts and which protocol it is.
        let (mut at, mut ethertype) = match link {
            // Ethernet.
            1 => (14, u16_at(frame, 12, true)?),
            // Raw IP.
            101 | 12 | 14 => (0, 0),
            228 => (0, 0x0800),
            229 => (0, 0x86dd),
            // Linux cooked captures.
            113 => (16, u16_at(frame, 14, true)?),
            276 => (20, u16_at(frame, 0, true)?),
            // BSD loopback: a four-byte address family in host order.
            0 | 108 => {
                let family = u32_at(frame, 0, false)?;
                let family = if family > 0xffff {
                    family.swap_bytes()
                } else {
                    family
                };
                (4, if family == 2 { 0x0800 } else { 0x86dd })
            }
            _ => return None,
        };
        // VLAN tags.
        while ethertype == 0x8100 || ethertype == 0x88a8 {
            ethertype = u16_at(frame, at + 2, true)?;
            at += 4;
        }
        if ethertype == 0 {
            ethertype = match frame.get(at)? >> 4 {
                4 => 0x0800,
                6 => 0x86dd,
                _ => return None,
            };
        }
        let (udp, ip_end) = match ethertype {
            0x0800 => {
                let ihl = usize::from(frame.get(at)? & 0x0f) * 4;
                let total = usize::from(u16_at(frame, at + 2, true)?);
                let flags = u16_at(frame, at + 6, true)?;
                // Fragments other than a whole datagram are not read.
                if ihl < 20 || *frame.get(at + 9)? != 17 || flags & 0x3fff != 0 {
                    return None;
                }
                (at + ihl, (at + total).min(frame.len()))
            }
            0x86dd => {
                if *frame.get(at + 6)? != 17 {
                    return None;
                }
                let payload = usize::from(u16_at(frame, at + 4, true)?);
                (at + 40, (at + 40 + payload).min(frame.len()))
            }
            _ => return None,
        };
        let len = usize::from(u16_at(frame, udp + 4, true)?);
        let start = udp + 8;
        let end = (udp + len).min(ip_end);
        (len >= 8 && start <= end).then_some(start..end)
    }

    /// The UDP payloads of the capture in `file`, each with its time.
    pub(super) fn packets(file: &[u8], notes: &mut Vec<String>) -> Result<Vec<Chunk>, String> {
        let mut out = Vec::new();
        let mut other = 0u64;
        let magic = file
            .get(..4)
            .ok_or("the capture is shorter than its header")?;
        if magic == [0x0a, 0x0d, 0x0d, 0x0a] {
            pcapng(file, &mut out, &mut other)?;
        } else {
            pcap(file, &mut out, &mut other)?;
        }
        if other > 0 {
            notes.push(format!(
                "{other} {} in the capture {} not UDP, left out",
                if other == 1 { "packet" } else { "packets" },
                if other == 1 { "is" } else { "are" }
            ));
        }
        Ok(out)
    }

    fn push(
        out: &mut Vec<Chunk>,
        base: usize,
        payload: std::ops::Range<usize>,
        time_ns: Option<i64>,
    ) {
        out.push(Chunk {
            source: ChunkSource::Map(base + payload.start..base + payload.end),
            records: None,
            time_ns,
        });
    }

    fn pcap(file: &[u8], out: &mut Vec<Chunk>, other: &mut u64) -> Result<(), String> {
        let (big, nanos) = match file.get(..4) {
            Some([0xd4, 0xc3, 0xb2, 0xa1]) => (false, false),
            Some([0xa1, 0xb2, 0xc3, 0xd4]) => (true, false),
            Some([0x4d, 0x3c, 0xb2, 0xa1]) => (false, true),
            Some([0xa1, 0xb2, 0x3c, 0x4d]) => (true, true),
            _ => return Err("not a pcap or pcapng capture: its magic is not one".into()),
        };
        let link =
            u32_at(file, 20, big).ok_or("the capture is shorter than its header")? & 0x0fff_ffff;
        let mut at = 24usize;
        while at + 16 <= file.len() && out.len() < MAX_PACKETS {
            let secs = i64::from(u32_at(file, at, big).unwrap_or(0));
            let frac = i64::from(u32_at(file, at + 4, big).unwrap_or(0));
            let caplen = u32_at(file, at + 8, big).unwrap_or(0) as usize;
            let start = at + 16;
            let Some(end) = start.checked_add(caplen).filter(|e| *e <= file.len()) else {
                break;
            };
            let time = secs
                .checked_mul(1_000_000_000)
                .and_then(|s| s.checked_add(if nanos { frac } else { frac * 1000 }));
            match udp_payload(&file[start..end], link) {
                Some(payload) => push(out, start, payload, time),
                None => *other += 1,
            }
            at = end;
        }
        Ok(())
    }

    fn pcapng(file: &[u8], out: &mut Vec<Chunk>, other: &mut u64) -> Result<(), String> {
        let mut at = 0usize;
        let mut big = false;
        // Each interface's link type and ticks per second.
        let mut interfaces: Vec<(u32, u64)> = Vec::new();
        while at + 12 <= file.len() && out.len() < MAX_PACKETS {
            let kind = u32_at(file, at, big).unwrap_or(0);
            if kind == 0x0a0d_0d0a {
                big = match file.get(at + 8..at + 12) {
                    Some([0x1a, 0x2b, 0x3c, 0x4d]) => true,
                    Some([0x4d, 0x3c, 0x2b, 0x1a]) => false,
                    _ => return Err("a pcapng section header has no byte-order magic".into()),
                };
                interfaces.clear();
            }
            let len = u32_at(file, at + 4, big).unwrap_or(0) as usize;
            if len < 12 || !len.is_multiple_of(4) || at + len > file.len() {
                break;
            }
            let body = &file[at + 8..at + len - 4];
            match kind {
                1 => {
                    let link = u32::from(u16_at(body, 0, big).unwrap_or(0));
                    let mut ticks = 1_000_000u64;
                    // Options: if_tsresol (9) sets the resolution.
                    let mut o = 8;
                    while o + 4 <= body.len() {
                        let code = u16_at(body, o, big).unwrap_or(0);
                        let olen = usize::from(u16_at(body, o + 2, big).unwrap_or(0));
                        if code == 0 {
                            break;
                        }
                        if code == 9 && olen >= 1 {
                            let r = body[o + 4];
                            ticks = if r & 0x80 != 0 {
                                1u64.checked_shl(u32::from(r & 0x7f)).unwrap_or(1_000_000)
                            } else {
                                10u64.checked_pow(u32::from(r)).unwrap_or(1_000_000)
                            };
                        }
                        o += 4 + olen.div_ceil(4) * 4;
                    }
                    interfaces.push((link, ticks.max(1)));
                }
                6 => {
                    let iface = u32_at(body, 0, big).unwrap_or(0) as usize;
                    let high = u64::from(u32_at(body, 4, big).unwrap_or(0));
                    let low = u64::from(u32_at(body, 8, big).unwrap_or(0));
                    let caplen = u32_at(body, 12, big).unwrap_or(0) as usize;
                    let Some(frame) = body.get(20..20 + caplen) else {
                        at += len;
                        continue;
                    };
                    let (link, ticks) = interfaces.get(iface).copied().unwrap_or((1, 1_000_000));
                    let stamp = (high << 32) | low;
                    let time =
                        i64::try_from(u128::from(stamp) * 1_000_000_000 / u128::from(ticks)).ok();
                    match udp_payload(frame, link) {
                        Some(payload) => push(out, at + 8 + 20, payload, time),
                        None => *other += 1,
                    }
                }
                3 => {
                    let (link, _) = interfaces.first().copied().unwrap_or((1, 1_000_000));
                    let frame = &body[4.min(body.len())..];
                    match udp_payload(frame, link) {
                        Some(payload) => push(out, at + 8 + 4, payload, None),
                        None => *other += 1,
                    }
                }
                _ => {}
            }
            at += len;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
