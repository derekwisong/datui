use super::*;

/// A SafeTensors file: the length, the JSON, then `data` bytes of tensor data.
pub(crate) fn safetensors_bytes(json: &str, data: usize) -> Vec<u8> {
    let mut out = (json.len() as u64).to_le_bytes().to_vec();
    out.extend_from_slice(json.as_bytes());
    out.extend(std::iter::repeat_n(0u8, data));
    out
}

/// Every way a model file is refused names the file, in the one shape.
#[test]
fn errors_name_the_file() {
    let past = r#"{"t":{"dtype":"F32","shape":[4],"data_offsets":[0,16]}}"#;
    crate::readers::bad_input::each_names_its_file(
        FileFormat::Safetensors,
        &[
            (
                "claims.safetensors",
                &[0xff, 0, 0, 0, 0, 0, 0, 0, b'{'],
                "claims",
            ),
            (
                "json.safetensors",
                &safetensors_bytes("{nope", 0),
                "not valid",
            ),
            (
                "past.safetensors",
                &safetensors_bytes(past, 8),
                "Tensor \"t\"",
            ),
            (
                "model.safetensors.index.json",
                br#"{"weight_map":{"a":"../b.safetensors"}}"#,
                "not a file beside it",
            ),
        ],
    );
    crate::readers::bad_input::each_names_its_file(
        FileFormat::Gguf,
        &[
            ("short.gguf", b"GG", "too short"),
            ("magic.gguf", b"GGML\x03\0\0\0", "does not start with GGUF"),
            ("v1.gguf", b"GGUF\x01\0\0\0", "version 1"),
        ],
    );
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
    pub(crate) fn tensor(&mut self, name: &str, shape: &[u64], ty: u32, offset: u64) -> &mut Self {
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
    w.tensor("token_embd.weight", &[256, 4], 12, 0)
        .tensor("output_norm.weight", &[256], 0, 576);
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

/// Bytes served by range, counting what is asked of them.
#[derive(Clone, Default)]
pub(crate) struct Served {
    pub files: std::collections::BTreeMap<String, Vec<u8>>,
    /// Each request: the file, and the range.
    pub asked: std::sync::Arc<std::sync::Mutex<Vec<(String, u64, u64)>>>,
    pub no_ranges: bool,
    /// How long each request takes, and how many are under way: now and at most.
    pub wait: std::time::Duration,
    pub busy: std::sync::Arc<(
        std::sync::atomic::AtomicUsize,
        std::sync::atomic::AtomicUsize,
    )>,
}

struct ServedFile {
    served: Served,
    url: String,
}

impl RangeSource for ServedFile {
    fn get(&mut self, start: u64, end: u64) -> std::result::Result<(Vec<u8>, u64), RangeError> {
        let bytes = self
            .served
            .files
            .get(&self.url)
            .ok_or_else(|| RangeError::Failed(format!("{}: 404", self.url)))?;
        if self.served.no_ranges {
            return Err(RangeError::NoRanges);
        }
        self.served
            .asked
            .lock()
            .unwrap()
            .push((self.url.clone(), start, end));
        if !self.served.wait.is_zero() {
            use std::sync::atomic::Ordering::SeqCst;
            let (now, most) = &*self.served.busy;
            most.fetch_max(now.fetch_add(1, SeqCst) + 1, SeqCst);
            std::thread::sleep(self.served.wait);
            now.fetch_sub(1, SeqCst);
        }
        let len = bytes.len() as u64;
        let (from, to) = (start.min(len) as usize, end.min(len) as usize);
        Ok((bytes[from..to].to_vec(), len))
    }
}

impl Served {
    fn bytes(&self) -> u64 {
        self.asked
            .lock()
            .unwrap()
            .iter()
            .map(|(_, a, b)| b - a)
            .sum()
    }

    /// The one GGUF file `url`, its header read from a first range of `first`.
    fn read_gguf_from(&self, url: &str, first: u64) -> Header {
        let mut src = ServedFile {
            served: self.clone(),
            url: url.to_string(),
        };
        read_header_ranged_from(&mut src, FileFormat::Gguf, first, &|| false).unwrap()
    }

    fn read(
        &self,
        urls: &[&str],
        format: FileFormat,
    ) -> std::result::Result<(LazyFrame, ModelSummary), RangeError> {
        let open = |url: &str| -> std::result::Result<Box<dyn RangeSource>, RangeError> {
            Ok(Box::new(ServedFile {
                served: self.clone(),
                url: url.to_string(),
            }))
        };
        let sibling = |url: &str, name: &str| {
            format!("{}/{name}", url.rsplit_once('/').map_or(url, |(d, _)| d))
        };
        let urls: Vec<String> = urls.iter().map(|u| u.to_string()).collect();
        read_remote_model(
            &urls,
            format,
            &Remote {
                open: &open,
                sibling: &sibling,
                stop: &|| false,
            },
        )
    }
}

fn ranged(bytes: &[u8], format: FileFormat, first: u64) -> std::result::Result<Header, RangeError> {
    let served = Served {
        files: [("f".to_string(), bytes.to_vec())].into(),
        ..Default::default()
    };
    let mut src = ServedFile {
        served,
        url: "f".to_string(),
    };
    read_header_ranged_from(&mut src, format, first, &|| false)
}

fn sample_gguf() -> Vec<u8> {
    let mut w = GgufWriter::new(2, 3);
    w.kv_str("general.architecture", "llama")
        .kv_u32("llama.context_length", 4096)
        .kv_strings("tokenizer.ggml.tokens", &["token"; 300]);
    w.tensor("token_embd.weight", &[256, 4], 12, 0)
        .tensor("output_norm.weight", &[256], 0, 576);
    w.data(576 + 1024);
    w.out
}

/// Read by range, a header is the same header the file reader finds, however small
/// the ranges; and so is the error, for a header cut short anywhere.
#[test]
fn a_ranged_read_finds_what_the_file_reader_finds() {
    let st = safetensors_bytes(
        r#"{"__metadata__":{"format":"pt"},"x":{"dtype":"F16","shape":[2,2],"data_offsets":[0,8]}}"#,
        8,
    );
    let gguf = sample_gguf();
    for first in [1, 3, 64, FIRST_GGUF_RANGE] {
        assert_eq!(
            ranged(&gguf, FileFormat::Gguf, first).unwrap(),
            parse_header(&gguf).unwrap(),
            "first range {first}"
        );
    }
    assert_eq!(
        ranged(&st, FileFormat::Safetensors, 1).unwrap(),
        parse_header(&st).unwrap()
    );
    for cut in 1..gguf.len() / 4 {
        assert!(
            ranged(&gguf[..cut], FileFormat::Gguf, 7).is_err(),
            "cut at {cut}"
        );
    }
    for cut in 1..st.len() {
        assert!(
            ranged(&st[..cut], FileFormat::Safetensors, 7).is_err(),
            "cut at {cut}"
        );
    }
}

/// SafeTensors asks for its first 64 KiB, which holds most headers whole, and
/// for a longer header the rest of its JSON: never past it. A GGUF header stops
/// being read where its tensor infos end, give or take the last range.
#[test]
fn only_the_header_is_fetched() {
    let url = "s3://b/m.safetensors";
    let json = r#"{"x":{"dtype":"F32","shape":[1024],"data_offsets":[0,4096]}}"#;
    let served = Served {
        files: [(url.to_string(), safetensors_bytes(json, 4096))].into(),
        ..Default::default()
    };
    assert!(served.read(&[url], FileFormat::Safetensors).is_ok());
    assert_eq!(
        *served.asked.lock().unwrap(),
        [(url.to_string(), 0, FIRST_SAFETENSORS_RANGE)],
        "one request"
    );

    // A header of 3,000 tensors, past the first read, before a gigabyte of data.
    let tensors: Vec<String> = (0..3000)
        .map(|i| {
            format!(
                r#""layer.{i}.weight":{{"dtype":"F32","shape":[1],"data_offsets":[{},{}]}}"#,
                i * 4,
                i * 4 + 4
            )
        })
        .collect();
    let json = format!("{{{}}}", tensors.join(","));
    let end = 8 + json.len() as u64;
    assert!(end > FIRST_SAFETENSORS_RANGE);
    let mut st = safetensors_bytes(&json, 3000 * 4);
    st.resize(st.len() + (1 << 20), 0);
    let served = Served {
        files: [(url.to_string(), st)].into(),
        ..Default::default()
    };
    let (_, summary) = served.read(&[url], FileFormat::Safetensors).unwrap();
    assert_eq!(summary.tensors, 3000);
    assert_eq!(
        *served.asked.lock().unwrap(),
        [
            (url.to_string(), 0, FIRST_SAFETENSORS_RANGE),
            (url.to_string(), FIRST_SAFETENSORS_RANGE, end)
        ],
        "the rest of the JSON, and no data"
    );

    // Megabytes of data after a header of a few KB.
    let mut gguf = sample_gguf();
    gguf.resize(gguf.len() + 8 * 1024 * 1024, 0);
    let served = Served {
        files: [("https://h/m.gguf".to_string(), gguf)].into(),
        ..Default::default()
    };
    assert!(served.read(&["https://h/m.gguf"], FileFormat::Gguf).is_ok());
    assert_eq!(
        served.asked.lock().unwrap().len(),
        1,
        "one range for a small header"
    );
    assert!(served.bytes() <= FIRST_GGUF_RANGE, "{}", served.bytes());
}

/// The lengths a hostile header states are refused before they are fetched.
#[test]
fn a_hostile_remote_header_is_refused_before_it_is_fetched() {
    // A SafeTensors length past the file, or past the spec.
    for claim in [50_000_000u64, MAX_SAFETENSORS_HEADER + 1, u64::MAX] {
        let mut st = safetensors_bytes("{}", 64);
        st[..8].copy_from_slice(&claim.to_le_bytes());
        let served = Served {
            files: [("u".to_string(), st)].into(),
            ..Default::default()
        };
        assert!(served.read(&["u"], FileFormat::Safetensors).is_err());
        assert_eq!(
            served.asked.lock().unwrap().len(),
            1,
            "only the first read, for {claim}"
        );
    }
    // A GGUF string or count longer than the file: one range, then the error.
    let mut w = GgufWriter::new(0, 1);
    w.u64(u64::MAX - 3);
    w.out.resize(1 << 20, 0);
    let served = Served {
        files: [("g".to_string(), w.out)].into(),
        ..Default::default()
    };
    assert!(served.read(&["g"], FileFormat::Gguf).is_err());
    assert_eq!(served.asked.lock().unwrap().len(), 1);
    let w = GgufWriter::new(u64::MAX, 0);
    assert!(ranged(&w.out, FileFormat::Gguf, 4).is_err());
}

/// A GGUF header the size of a Llama 3 vocabulary (128k tokens, 280k merges, about
/// 7 MB), at the front of a much larger file.
fn llama3_sized_gguf() -> Vec<u8> {
    let tokens = vec!["tok_ab"; 128_256];
    let merges = vec!["Ġab Ġcdefg"; 280_147];
    let mut w = GgufWriter::new(291, 3);
    w.kv_str("general.architecture", "llama")
        .kv_strings("tokenizer.ggml.tokens", &tokens)
        .kv_strings("tokenizer.ggml.merges", &merges);
    for i in 0..291 {
        w.tensor(&format!("blk.{i}.attn_q.weight"), &[1], 0, i * 32);
    }
    w.data(291 * 32 + (32 << 20));
    w.out
}

/// A stopped open asks for nothing more: not the next range of a header, nor the
/// shards no read has started on.
#[test]
fn a_stopped_read_asks_for_nothing_more() {
    let served = Served {
        files: [("g".to_string(), llama3_sized_gguf())].into(),
        ..Default::default()
    };
    let asked = served.asked.clone();
    let stop = || !asked.lock().unwrap().is_empty();
    let mut src = ServedFile {
        served: served.clone(),
        url: "g".to_string(),
    };
    let err = read_header_ranged_from(&mut src, FileFormat::Gguf, 1024, &stop).unwrap_err();
    assert!(
        matches!(err, RangeError::Failed(ref m) if m.contains("cancelled")),
        "{err:?}"
    );
    assert_eq!(served.asked.lock().unwrap().len(), 1);

    let st = safetensors_bytes(
        r#"{"x":{"dtype":"F32","shape":[1],"data_offsets":[0,4]}}"#,
        4,
    );
    let shards: Vec<String> = (0..SHARD_READS * 4).map(|i| format!("s{i:03}")).collect();
    let served = Served {
        files: shards.iter().map(|s| (s.clone(), st.clone())).collect(),
        ..Default::default()
    };
    let asked = served.asked.clone();
    let open = |url: &str| -> std::result::Result<Box<dyn RangeSource>, RangeError> {
        Ok(Box::new(ServedFile {
            served: served.clone(),
            url: url.to_string(),
        }))
    };
    let stop = || !asked.lock().unwrap().is_empty();
    let read = read_remote_model(
        &shards,
        FileFormat::Safetensors,
        &Remote {
            open: &open,
            sibling: &|_, name| name.to_string(),
            stop: &stop,
        },
    );
    assert!(
        matches!(read, Err(RangeError::Failed(ref m)) if m.to_lowercase().contains("cancelled")),
        "{:?}",
        read.err()
    );
    // At most each reader's request already under way when the stop came.
    let n = served.asked.lock().unwrap().len();
    assert!((1..=SHARD_READS).contains(&n), "{n}");
}

/// A vocabulary-sized header costs a handful of requests and not much more than
/// itself on the wire. Measured at 7.2 MB: a first range of 64 KiB took 7 requests
/// (7.9 MiB), 256 KiB 5 (7.8 MiB), 1 MiB 4 (15 MiB).
#[test]
fn a_vocabulary_sized_gguf_header_takes_a_few_ranges() {
    let gguf = llama3_sized_gguf();
    let served = Served {
        files: [("g".to_string(), gguf.clone())].into(),
        ..Default::default()
    };
    let header = served.read_gguf_from("g", FIRST_GGUF_RANGE);
    assert_eq!(header.tensors.len(), 291);
    // The header and its few bytes of tensor data, before the padding.
    let end = (gguf.len() - (32 << 20)) as u64;
    assert!(
        served.asked.lock().unwrap().len() <= 5,
        "{:?}",
        served.asked.lock().unwrap()
    );
    assert!(served.bytes() < end * 2, "{} for {end}", served.bytes());
}

/// A source that answers with more or fewer bytes than were asked for, or whose
/// length changes between requests, is an error rather than a header.
#[test]
fn a_lying_source_is_an_error() {
    struct Liar(u32);
    impl RangeSource for Liar {
        fn get(&mut self, start: u64, end: u64) -> std::result::Result<(Vec<u8>, u64), RangeError> {
            self.0 += 1;
            let st = safetensors_bytes(
                r#"{"x":{"dtype":"F32","shape":[1],"data_offsets":[0,4]}}"#,
                4,
            );
            let bytes = st[start as usize..end.min(st.len() as u64) as usize].to_vec();
            Ok(match self.0 {
                // More than asked.
                1 => ([bytes, vec![0; 4]].concat(), st.len() as u64),
                2 => (bytes, st.len() as u64),
                // A length that changed.
                _ => (bytes, 1 << 30),
            })
        }
    }
    let err =
        read_header_ranged_from(&mut Liar(0), FileFormat::Safetensors, 8, &|| false).unwrap_err();
    assert!(
        matches!(err, RangeError::Failed(ref m) if m.contains("got")),
        "{err:?}"
    );
    // From 8 bytes, so the JSON takes a second request.
    let err =
        read_header_ranged_from(&mut Liar(1), FileFormat::Safetensors, 8, &|| false).unwrap_err();
    assert!(
        matches!(err, RangeError::Failed(ref m) if m.contains("changed size")),
        "{err:?}"
    );
}

/// An index names its shards beside its own URL, each read once; a name that leaves
/// the directory is refused.
#[test]
fn a_remote_index_resolves_its_shards_beside_it() {
    let shard = |n: u64| {
        safetensors_bytes(
            &format!(r#"{{"t{n}":{{"dtype":"F32","shape":[2],"data_offsets":[0,8]}}}}"#),
            8,
        )
    };
    let index = r#"{"metadata":{"total_size":16},"weight_map":{
            "t1":"model-00001-of-00002.safetensors","t2":"model-00002-of-00002.safetensors"}}"#;
    let served = Served {
        files: [
            (
                "gs://b/m/model.safetensors.index.json".to_string(),
                index.as_bytes().to_vec(),
            ),
            (
                "gs://b/m/model-00001-of-00002.safetensors".to_string(),
                shard(1),
            ),
            (
                "gs://b/m/model-00002-of-00002.safetensors".to_string(),
                shard(2),
            ),
        ]
        .into(),
        ..Default::default()
    };
    let (lf, summary) = served
        .read(
            &[
                "gs://b/m/model.safetensors.index.json",
                "gs://b/m/model-00001-of-00002.safetensors",
            ],
            FileFormat::Safetensors,
        )
        .unwrap();
    assert_eq!((summary.files, summary.tensors), (2, 2));
    assert_eq!(summary.metadata[0].0, "total_size");
    let df = lf.collect().unwrap();
    let files: Vec<&str> = df
        .column("file")
        .unwrap()
        .str()
        .unwrap()
        .iter()
        .map(|v| v.unwrap())
        .collect();
    assert_eq!(
        files,
        [
            "model-00001-of-00002.safetensors",
            "model-00002-of-00002.safetensors"
        ]
    );

    for bad in [
        "../x.safetensors",
        "a/b.safetensors",
        "..",
        "a\\\\b.safetensors",
    ] {
        let index = format!(r#"{{"weight_map":{{"t":"{bad}"}}}}"#);
        let served = Served {
            files: [(
                "i/model.safetensors.index.json".to_string(),
                index.into_bytes(),
            )]
            .into(),
            ..Default::default()
        };
        let err = served
            .read(&["i/model.safetensors.index.json"], FileFormat::Safetensors)
            .err()
            .expect("an error");
        assert!(
            matches!(err, RangeError::Failed(ref m) if m.contains("not a file beside it")),
            "{bad}: {err:?}"
        );
    }
}

/// A checkpoint's shards are read a few at a time, one request each, and come out
/// in the index's order however their reads finish; a shard that fails is named.
#[test]
fn shards_are_read_a_few_at_a_time() {
    let n = SHARD_READS * 3;
    let names: Vec<String> = (1..=n)
        .map(|i| format!("model-{i:05}-of-{n:05}.safetensors"))
        .collect();
    let map: Vec<String> = names
        .iter()
        .enumerate()
        .map(|(i, name)| format!(r#""t{i}":"{name}""#))
        .collect();
    let index = format!(r#"{{"weight_map":{{{}}}}}"#, map.join(","));
    let mut files: std::collections::BTreeMap<String, Vec<u8>> = names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let json = format!(r#"{{"t{i}":{{"dtype":"F32","shape":[1],"data_offsets":[0,4]}}}}"#);
            (format!("h/{name}"), safetensors_bytes(&json, 4))
        })
        .collect();
    files.insert(
        "h/model.safetensors.index.json".to_string(),
        index.into_bytes(),
    );
    let served = Served {
        files,
        wait: std::time::Duration::from_millis(20),
        ..Default::default()
    };
    let (lf, summary) = served
        .read(&["h/model.safetensors.index.json"], FileFormat::Safetensors)
        .unwrap();
    assert_eq!((summary.files, summary.tensors), (n, n));
    let df = lf.collect().unwrap();
    let read: Vec<&str> = df
        .column("file")
        .unwrap()
        .str()
        .unwrap()
        .iter()
        .flatten()
        .collect();
    assert_eq!(read, names, "in the index's order");
    assert_eq!(
        served.asked.lock().unwrap().len(),
        1 + n,
        "one request a shard"
    );
    let most = served.busy.1.load(std::sync::atomic::Ordering::SeqCst);
    assert!((2..=SHARD_READS).contains(&most), "{most} at once");

    let mut broken = served.clone();
    broken.wait = std::time::Duration::ZERO;
    broken
        .files
        .insert(format!("h/{}", names[5]), b"not a header".to_vec());
    let err = broken
        .read(&["h/model.safetensors.index.json"], FileFormat::Safetensors)
        .err()
        .expect("an error");
    assert!(
        matches!(err, RangeError::Failed(ref m) if m.starts_with(&format!("\"h/{}\": ", names[5]))),
        "{err:?}"
    );
}

/// A server that sends whole files: one file named on its own is downloaded
/// instead; shards cannot be, and say why.
#[test]
fn no_ranges_is_a_download_for_one_file_only() {
    let served = Served {
        files: [
            ("h/m.safetensors".to_string(), safetensors_bytes("{}", 0)),
            (
                "h/model.safetensors.index.json".to_string(),
                br#"{"weight_map":{}}"#.to_vec(),
            ),
        ]
        .into(),
        no_ranges: true,
        ..Default::default()
    };
    assert_eq!(
        served
            .read(&["h/m.safetensors"], FileFormat::Safetensors)
            .err()
            .expect("an error"),
        RangeError::NoRanges
    );
    let err = served
        .read(&["h/model.safetensors.index.json"], FileFormat::Safetensors)
        .err()
        .expect("an error");
    assert!(
        matches!(err, RangeError::Failed(ref m) if m.contains("byte ranges")),
        "{err:?}"
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
