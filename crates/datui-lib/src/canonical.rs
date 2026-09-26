//! `canonicalize` without Windows' `\\?\` prefix.
//!
//! On Windows `std::fs::canonicalize` answers `\\?\C:\data\a.csv`: a verbatim path, which
//! nothing else in datui spells that way. Recorded as a recent it read `\\?\C:\…` rather
//! than `~\…`, and it matched no row a listing built from `C:\data`. The prefix is dropped
//! wherever the plain spelling names the same file, which is what the `dunce` crate does.
//! Everything that canonicalizes a path goes through here, tests included, so what is
//! stored and what it is compared with are spelled alike.

use std::path::{Path, PathBuf};

/// [`Path::canonicalize`], in the spelling a Windows user would type.
pub fn canonicalize(path: &Path) -> std::io::Result<PathBuf> {
    let canonical = path.canonicalize()?;
    if !cfg!(windows) {
        return Ok(canonical);
    }
    Ok(match canonical.to_str().and_then(simplified) {
        Some(plain) => PathBuf::from(plain),
        None => canonical,
    })
}

/// The plain spelling of a verbatim path, when one means the same thing.
///
/// `None` otherwise: a path past `MAX_PATH` is reachable only verbatim, and a verbatim
/// path can hold names the plain form would read differently — `a.` and `b ` lose their
/// ends, and `CON` or `nul.txt` is a device wherever it appears.
fn simplified(path: &str) -> Option<String> {
    let (plain, names) = if let Some(rest) = path.strip_prefix(r"\\?\UNC\") {
        (format!(r"\\{rest}"), rest)
    } else {
        let rest = path.strip_prefix(r"\\?\")?;
        let bytes = rest.as_bytes();
        let drive = bytes.len() >= 3 && bytes[0].is_ascii_alphabetic() && &bytes[1..3] == b":\\";
        if !drive {
            return None;
        }
        (rest.to_string(), &rest[3..])
    };
    const MAX_PATH: usize = 260;
    if plain.len() >= MAX_PATH {
        return None;
    }
    names
        .split('\\')
        .filter(|name| !name.is_empty())
        .all(plain_name_means_the_same)
        .then_some(plain)
}

fn plain_name_means_the_same(name: &str) -> bool {
    const DEVICES: [&str; 22] = [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];
    let stem = name.split('.').next().unwrap_or(name).trim_end();
    !name.ends_with(['.', ' '])
        && !name.contains('/')
        && !DEVICES.iter().any(|d| d.eq_ignore_ascii_case(stem))
}

#[cfg(test)]
mod tests {
    use super::simplified;

    #[test]
    fn a_drive_path_loses_its_prefix() {
        assert_eq!(
            simplified(r"\\?\C:\Users\me\data\a.csv").as_deref(),
            Some(r"C:\Users\me\data\a.csv")
        );
        assert_eq!(simplified(r"\\?\D:\").as_deref(), Some(r"D:\"));
    }

    #[test]
    fn a_share_becomes_a_unc_path() {
        assert_eq!(
            simplified(r"\\?\UNC\nas\data\sales.parquet").as_deref(),
            Some(r"\\nas\data\sales.parquet")
        );
    }

    #[test]
    fn a_path_only_the_verbatim_form_reaches_keeps_it() {
        for kept in [
            r"\\?\C:\data\trailing.",
            r"\\?\C:\data\trailing ",
            r"\\?\C:\data\CON",
            r"\\?\C:\data\nul.txt",
            r"\\?\C:\data\a/b",
            r"\\?\Volume{0b1c}\data",
            r"C:\already\plain",
        ] {
            assert_eq!(simplified(kept), None, "{kept}");
        }
        let long = format!(r"\\?\C:\{}", "a".repeat(300));
        assert_eq!(simplified(&long), None);
    }

    #[test]
    fn a_name_that_only_starts_like_a_device_is_plain() {
        assert_eq!(
            simplified(r"\\?\C:\data\console.csv").as_deref(),
            Some(r"C:\data\console.csv")
        );
    }
}
