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
    let result = crate::chart::chart_data::prepare_chart_data(
        &lf,
        &schema,
        SECONDS,
        &["ch1".into()],
        &crate::chart::chart_data::ChartSampling::rows(Some(1_000)),
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
    crate::formats::readers::bad_input::each_names_its_file(
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
    let streaming = |lf: LazyFrame| crate::analysis::statistics::collect_lazy(lf, true).unwrap();
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
