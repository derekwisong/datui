//! A documentation link handed to the system's browser: `o` in the Documentation
//! view.
//!
//! Catalogs and format specs can come from anyone, so a link's text is hostile until
//! checked. Only `http` and `https` reach the opener: other schemes are where a
//! link-opening bug turns into running code, through whatever handler the system
//! has registered for them. The opener gets the parser's serialization of the URL,
//! never the text as written, as one argument of a program run without a shell.

use std::fmt;

/// Why a link is not opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    /// Not written `http://` or `https://`, in lowercase: another scheme, or a form
    /// a parser reads leniently (`HTTPS:`, `https:x`, `https:\\x`).
    Scheme,
    /// A control character, whitespace or a backslash, which parsers drop or turn
    /// into `/` rather than refuse.
    Character,
    /// It does not parse as a URL.
    Malformed,
    /// No host to go to.
    NoHost,
    /// A user name or password before the host.
    Credentials,
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Refused::Scheme => "not an http or https link",
            Refused::Character => "a character a link cannot hold",
            Refused::Malformed => "not a URL",
            Refused::NoHost => "no host",
            Refused::Credentials => "a user name or password",
        })
    }
}

/// The URL to hand the browser for `raw`, as the parser writes it back: a host that
/// is not ASCII in punycode, the path percent-encoded. Refused unless it is an
/// `http` or `https` URL with a host and nothing a lenient parse would mend.
pub fn checked_url(raw: &str) -> Result<String, Refused> {
    if raw
        .chars()
        .any(|c| c.is_control() || c.is_whitespace() || c == '\\' || bidi(c))
    {
        return Err(Refused::Character);
    }
    // The scheme as written, not as parsed: the parser lowercases `HTTPS:`, reads
    // `https:x` as `https://x/` and skips a third slash, and none of those is a
    // link anyone wrote on purpose.
    let Some(rest) = raw
        .strip_prefix("https://")
        .or_else(|| raw.strip_prefix("http://"))
    else {
        return Err(Refused::Scheme);
    };
    if rest.starts_with('/') {
        return Err(Refused::Malformed);
    }
    let url = url::Url::parse(raw).map_err(|_| Refused::Malformed)?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(Refused::Scheme);
    }
    if url.host_str().is_none_or(str::is_empty) {
        return Err(Refused::NoHost);
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Refused::Credentials);
    }
    let out = url.as_str().to_string();
    // A scheme leads it, so no opener can read it as an option.
    assert!(
        !out.starts_with('-'),
        "a checked URL starts with its scheme"
    );
    Ok(out)
}

/// A character that reorders the text around it, so the URL confirmed would not
/// read as the URL opened.
fn bidi(c: char) -> bool {
    matches!(c, '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
}

/// The systems whose openers differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Windows,
    MacOs,
    /// Linux and the BSDs: `xdg-open`, and a desktop only where a display is named.
    Unix,
}

impl Platform {
    pub fn current() -> Self {
        if cfg!(windows) {
            Platform::Windows
        } else if cfg!(target_os = "macos") {
            Platform::MacOs
        } else {
            Platform::Unix
        }
    }
}

/// Whether a browser opened here would open in front of the user: not over SSH
/// (checked first, since `ssh -X` sets `DISPLAY`), and on Linux and the BSDs only
/// with a display. `env` looks up a variable; blank counts as unset.
pub fn local_desktop(platform: Platform, env: impl Fn(&str) -> Option<String>) -> bool {
    let set = |name: &str| env(name).is_some_and(|v| !v.trim().is_empty());
    if set("SSH_CONNECTION") || set("SSH_TTY") {
        return false;
    }
    match platform {
        Platform::Windows | Platform::MacOs => true,
        Platform::Unix => set("DISPLAY") || set("WAYLAND_DISPLAY"),
    }
}

/// The program and arguments that open `url`, the URL one argument of its own and
/// no shell anywhere. Windows uses `rundll32 url.dll,FileProtocolHandler`, not
/// `cmd /c start`, because cmd reads `&` and `^` in a URL as its own syntax.
pub fn opener_argv(platform: Platform, url: &str) -> Vec<String> {
    let program: &[&str] = match platform {
        Platform::Windows => &["rundll32", "url.dll,FileProtocolHandler"],
        Platform::MacOs => &["open"],
        Platform::Unix => &["xdg-open"],
    };
    program
        .iter()
        .map(|s| s.to_string())
        .chain(std::iter::once(url.to_string()))
        .collect()
}

/// Start the browser on `url`, already checked, without waiting for it.
pub fn open(url: &str) -> std::io::Result<()> {
    crate::inspector::external_open::start(&opener_argv(Platform::current(), url))
}

#[cfg(test)]
mod tests;
