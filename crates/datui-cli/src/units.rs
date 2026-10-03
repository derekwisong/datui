//! Sizes and durations written with their unit: `512MiB`, `250ms`. One parser reads
//! them from a config file, from `-c` and from a flag, so a value means the same
//! wherever it is written.

use std::time::Duration;

/// Binary units, and the decimal ones people also write. A size has a unit unless it
/// is 0: a bare `512` could be bytes or the MiB an older key took.
const SIZE_UNITS: &[(&str, u64)] = &[
    ("B", 1),
    ("KiB", 1 << 10),
    ("MiB", 1 << 20),
    ("GiB", 1 << 30),
    ("TiB", 1 << 40),
    ("kB", 1_000),
    ("KB", 1_000),
    ("MB", 1_000_000),
    ("GB", 1_000_000_000),
    ("TB", 1_000_000_000_000),
];

const DURATION_UNITS: &[(&str, u64)] = &[("ms", 1), ("s", 1_000), ("m", 60_000), ("h", 3_600_000)];

/// The number and the unit of `text`, the unit's factor looked up in `units`.
fn split(
    text: &str,
    units: &[(&str, u64)],
    what: &str,
    example: &str,
) -> Result<(f64, u64), String> {
    let text = text.trim();
    let at = text
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '_'))
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(at);
    let number = number.replace('_', "");
    let value: f64 = number
        .parse()
        .map_err(|_| format!("\"{text}\" is not a {what}, such as {example}"))?;
    let unit = unit.trim();
    if unit.is_empty() {
        if value == 0.0 {
            return Ok((0.0, 1));
        }
        return Err(format!("\"{text}\" needs a unit, as in {example}"));
    }
    let factor = units
        .iter()
        .find(|(name, _)| *name == unit)
        .map(|(_, f)| *f)
        .ok_or_else(|| {
            let names: Vec<&str> = units.iter().map(|(n, _)| *n).collect();
            format!("\"{unit}\" is not a unit of {what}: {}", names.join(", "))
        })?;
    Ok((value, factor))
}

/// Bytes from `512MiB`, `2GiB`, `1.5GB` or `0`.
pub fn parse_size(text: &str) -> Result<u64, String> {
    let (value, factor) = split(text, SIZE_UNITS, "size", "512MiB")?;
    let bytes = value * factor as f64;
    if !bytes.is_finite() || bytes > u64::MAX as f64 {
        return Err(format!("\"{}\" is too large", text.trim()));
    }
    Ok(bytes.round() as u64)
}

/// A duration from `250ms`, `1.5s`, `2m` or `0`.
pub fn parse_duration(text: &str) -> Result<Duration, String> {
    let (value, factor) = split(text, DURATION_UNITS, "duration", "250ms")?;
    let ms = value * factor as f64;
    if !ms.is_finite() || ms > u64::MAX as f64 {
        return Err(format!("\"{}\" is too long", text.trim()));
    }
    Ok(Duration::from_millis(ms.round() as u64))
}

/// `bytes` in the largest binary unit that holds it whole: `512MiB`, `100KiB`.
pub fn format_size(bytes: u64) -> String {
    if bytes == 0 {
        return "0".to_string();
    }
    let (name, factor) = [
        ("TiB", 1u64 << 40),
        ("GiB", 1 << 30),
        ("MiB", 1 << 20),
        ("KiB", 1 << 10),
    ]
    .into_iter()
    .find(|(_, f)| bytes.is_multiple_of(*f))
    .unwrap_or(("B", 1));
    format!("{}{name}", bytes / factor)
}

/// `duration` in the largest unit that holds it whole: `250ms`, `2s`, `1m`.
pub fn format_duration(duration: Duration) -> String {
    let ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
    if ms == 0 {
        return "0".to_string();
    }
    let (name, factor) = DURATION_UNITS
        .iter()
        .rev()
        .find(|(_, f)| ms.is_multiple_of(*f))
        .copied()
        .unwrap_or(("ms", 1));
    format!("{}{name}", ms / factor)
}

/// A size in bytes, written with its unit in config: `max_buffered = "512MiB"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct ByteSize(pub u64);

impl ByteSize {
    pub const fn mib(n: u64) -> Self {
        Self(n << 20)
    }
    pub const fn kib(n: u64) -> Self {
        Self(n << 10)
    }
    pub fn bytes(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for ByteSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format_size(self.0))
    }
}

impl std::str::FromStr for ByteSize {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        parse_size(s).map(Self)
    }
}

/// A duration written with its unit in config: `follow_interval = "250ms"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Interval(pub Duration);

impl Interval {
    pub const fn ms(n: u64) -> Self {
        Self(Duration::from_millis(n))
    }
    pub fn duration(self) -> Duration {
        self.0
    }
}

impl std::fmt::Display for Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format_duration(self.0))
    }
}

impl std::str::FromStr for Interval {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        parse_duration(s).map(Self)
    }
}

macro_rules! serde_as_text {
    ($ty:ty) => {
        impl serde::Serialize for $ty {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(&self.to_string())
            }
        }

        impl<'de> serde::Deserialize<'de> for $ty {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                struct Visit;
                impl serde::de::Visitor<'_> for Visit {
                    type Value = $ty;
                    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        f.write_str("a value with its unit, in quotes")
                    }
                    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<$ty, E> {
                        v.parse().map_err(E::custom)
                    }
                    // `0` needs no unit or quotes.
                    fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<$ty, E> {
                        self.visit_str(&v.to_string())
                    }
                    fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<$ty, E> {
                        self.visit_str(&v.to_string())
                    }
                }
                d.deserialize_any(Visit)
            }
        }
    };
}

serde_as_text!(ByteSize);
serde_as_text!(Interval);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_read_and_write_with_their_unit() {
        assert_eq!(parse_size("512MiB"), Ok(512 << 20));
        assert_eq!(parse_size(" 2GiB "), Ok(2 << 30));
        assert_eq!(parse_size("1.5KiB"), Ok(1536));
        assert_eq!(parse_size("100KB"), Ok(100_000));
        assert_eq!(parse_size("0"), Ok(0));
        assert!(parse_size("512").unwrap_err().contains("needs a unit"));
        assert!(parse_size("5mb").unwrap_err().contains("not a unit"));
        assert!(parse_size("MiB").is_err());
        assert_eq!(format_size(512 << 20), "512MiB");
        assert_eq!(format_size(2048 << 20), "2GiB");
        assert_eq!(format_size(100 << 10), "100KiB");
        assert_eq!(format_size(1000), "1000B");
        assert_eq!(format_size(0), "0");
    }

    #[test]
    fn durations_read_and_write_with_their_unit() {
        assert_eq!(parse_duration("250ms"), Ok(Duration::from_millis(250)));
        assert_eq!(parse_duration("1.5s"), Ok(Duration::from_millis(1500)));
        assert_eq!(parse_duration("2m"), Ok(Duration::from_secs(120)));
        assert_eq!(parse_duration("0"), Ok(Duration::ZERO));
        assert!(parse_duration("250").unwrap_err().contains("needs a unit"));
        assert_eq!(format_duration(Duration::from_millis(1500)), "1500ms");
        assert_eq!(format_duration(Duration::from_secs(2)), "2s");
        assert_eq!(format_duration(Duration::from_secs(60)), "1m");
    }
}
