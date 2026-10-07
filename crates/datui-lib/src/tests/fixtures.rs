//! Binary sample files built in Rust, shared by each format's tests. ELF reads the
//! generated `tests/sample-data/elf/tiny.elf` instead.

use crate::dataflash::{FMT, FMT_LEN, format_size};
use crate::ulog::{MAGIC, SYNC};

/// A ULog message: its length, its kind, then the payload.
fn ulog_message(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = (payload.len() as u16).to_le_bytes().to_vec();
    out.push(kind);
    out.extend(payload);
    out
}

/// A small log: a nested format, two instances of one topic, a parameter, a
/// logged message, damage and a sync marker, a message cut short by trailing
/// padding, and data cut off at the end.
pub(crate) fn ulog() -> Vec<u8> {
    let mut log = MAGIC.to_vec();
    log.push(1);
    log.extend(1_000u64.to_le_bytes());
    log.extend(ulog_message(b'F', b"vec3:float x;float y;float z;"));
    log.extend(ulog_message(
        b'F',
        b"sensor:uint64_t timestamp;uint8_t id;vec3 v;char[4] tag;int16_t[2] raw;uint8_t[3] _padding0;",
    ));
    log.extend(ulog_message(b'F', b"status:uint64_t timestamp;bool armed;"));
    let mut info = vec![b"char[6] sys_name".len() as u8];
    info.extend(b"char[6] sys_name");
    info.extend(b"PX4\0\0\0");
    log.extend(ulog_message(b'I', &info));
    let mut param = vec![b"float MPC_XY_VEL".len() as u8];
    param.extend(b"float MPC_XY_VEL");
    param.extend(2.5f32.to_le_bytes());
    log.extend(ulog_message(b'P', &param));
    for (multi, id, name) in [(0u8, 1u16, "sensor"), (1, 2, "sensor"), (0, 3, "status")] {
        let mut a = vec![multi];
        a.extend(id.to_le_bytes());
        a.extend(name.as_bytes());
        log.extend(ulog_message(b'A', &a));
    }
    let sensor = |id: u16, t: u64, x: f32, pad: bool| {
        let mut d = id.to_le_bytes().to_vec();
        d.extend(t.to_le_bytes());
        d.push(id as u8);
        for v in [x, x * 2.0, x * 3.0] {
            d.extend(v.to_le_bytes());
        }
        d.extend(b"ab\0\0");
        d.extend(7i16.to_le_bytes());
        d.extend((-7i16).to_le_bytes());
        if pad {
            d.extend([0u8; 3]);
        }
        ulog_message(b'D', &d)
    };
    log.extend(sensor(1, 100, 1.0, true));
    log.extend(sensor(2, 110, 5.0, false));
    let mut l = vec![b'4'];
    l.extend(120u64.to_le_bytes());
    l.extend(b"low battery");
    log.extend(ulog_message(b'L', &l));
    // Damage, then a sync marker.
    log.extend([0xEE; 7]);
    log.extend(ulog_message(b'S', &SYNC));
    log.extend(sensor(1, 200, 2.0, false));
    let mut s = 3u16.to_le_bytes().to_vec();
    s.extend(150u64.to_le_bytes());
    s.push(1);
    log.extend(ulog_message(b'D', &s));
    // A message cut off by the end of the file.
    let cut = sensor(1, 300, 3.0, false);
    log.extend(&cut[..cut.len() - 4]);
    log
}

/// A DataFlash FMT record describing message type `id`.
pub(crate) fn dataflash_fmt(id: u8, name: &str, format: &str, labels: &str) -> Vec<u8> {
    let length = if id == FMT {
        FMT_LEN
    } else {
        format_size(format).unwrap() + 3
    };
    let mut r = vec![0xA3, 0x95, FMT, id, length as u8];
    let pad = |s: &str, n: usize| {
        let mut b = s.as_bytes().to_vec();
        b.resize(n, 0);
        b
    };
    r.extend(pad(name, 4));
    r.extend(pad(format, 16));
    r.extend(pad(labels, 64));
    r
}

/// A small log: FMT, the unit tables, two message types interleaved, a scaled
/// field, units, damage, and a record cut off at the end.
pub(crate) fn dataflash() -> Vec<u8> {
    let mut log = dataflash_fmt(FMT, "FMT", "BBnNZ", "Type,Length,Name,Format,Columns");
    log.extend(dataflash_fmt(129, "UNIT", "QbZ", "TimeUS,Id,Label"));
    log.extend(dataflash_fmt(130, "MULT", "Qbd", "TimeUS,Id,Mult"));
    log.extend(dataflash_fmt(
        131,
        "FMTU",
        "QBNN",
        "TimeUS,FmtType,UnitIds,MultIds",
    ));
    log.extend(dataflash_fmt(140, "ATT", "QccH", "TimeUS,Roll,Pitch,Yaw"));
    log.extend(dataflash_fmt(141, "BARO", "QfiL", "TimeUS,Alt,Press,Lat"));
    let unit = |id: u8, label: &str| {
        let mut r = vec![0xA3, 0x95, 129];
        r.extend(0u64.to_le_bytes());
        r.push(id);
        let mut l = label.as_bytes().to_vec();
        l.resize(64, 0);
        r.extend(l);
        r
    };
    log.extend(unit(b'd', "deg"));
    log.extend(unit(b'm', "m"));
    log.extend(unit(b'P', "Pa"));
    let mut mult = vec![0xA3, 0x95, 130];
    mult.extend(0u64.to_le_bytes());
    mult.push(b'2');
    mult.extend(100.0f64.to_le_bytes());
    log.extend(mult);
    let fmtu = |ty: u8, units: &str, mults: &str| {
        let mut r = vec![0xA3, 0x95, 131];
        r.extend(0u64.to_le_bytes());
        r.push(ty);
        for s in [units, mults] {
            let mut b = s.as_bytes().to_vec();
            b.resize(16, 0);
            r.extend(b);
        }
        r
    };
    log.extend(fmtu(141, "-mPd", "-02?"));
    for i in 0..3u64 {
        let mut att = vec![0xA3, 0x95, 140];
        att.extend((1000 + i * 10).to_le_bytes());
        att.extend((150i16 + i as i16).to_le_bytes());
        att.extend((-250i16).to_le_bytes());
        att.extend(9000u16.to_le_bytes());
        log.extend(att);
        let mut baro = vec![0xA3, 0x95, 141];
        baro.extend((1005 + i * 10).to_le_bytes());
        baro.extend((12.5f32 + i as f32).to_le_bytes());
        baro.extend(101_325i32.to_le_bytes());
        baro.extend(473_977_418i32.to_le_bytes());
        log.extend(baro);
        if i == 1 {
            log.extend([0x00, 0xA3, 0x11]);
        }
    }
    log.extend([0xA3, 0x95, 140, 1, 2]);
    log
}

/// A MIDI variable-length quantity.
pub(crate) fn vlq(mut n: u32) -> Vec<u8> {
    let mut out = vec![(n & 0x7f) as u8];
    n >>= 7;
    while n > 0 {
        out.insert(0, (n & 0x7f) as u8 | 0x80);
        n >>= 7;
    }
    out
}

/// A standard MIDI file from its header fields and each track's event bytes.
pub(crate) fn smf(format: u16, division: u16, tracks: &[&[u8]]) -> Vec<u8> {
    let mut out = b"MThd\0\0\0\x06".to_vec();
    out.extend_from_slice(&format.to_be_bytes());
    out.extend_from_slice(&(tracks.len() as u16).to_be_bytes());
    out.extend_from_slice(&division.to_be_bytes());
    for t in tracks {
        out.extend_from_slice(b"MTrk");
        out.extend_from_slice(&(t.len() as u32).to_be_bytes());
        out.extend_from_slice(t);
    }
    out
}

/// A DBC file: three messages, one multiplexed, with comments, values and a float signal.
pub(crate) const DBC: &str = r#"VERSION ""

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

/// The generated 64-bit ELF executable: `.text`, `.rodata`, `.data` and `.bss`, with
/// symbols in each and one mangled Rust name.
pub(crate) fn elf() -> Vec<u8> {
    crate::tests::ensure_sample_data();
    std::fs::read(crate::tests::sample_data_dir().join("elf/tiny.elf")).unwrap()
}
