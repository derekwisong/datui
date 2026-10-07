use super::*;

#[test]
fn only_plain_http_and_https_links_pass() {
    let accepted = [
        ("https://example.com", "https://example.com/"),
        ("http://example.org/x", "http://example.org/x"),
        (
            "https://example.com/a?b=1&c=^2#top",
            "https://example.com/a?b=1&c=^2#top",
        ),
        ("https://Example.COM/Path", "https://example.com/Path"),
        ("https://example.com:8443/", "https://example.com:8443/"),
        ("http://127.0.0.1/", "http://127.0.0.1/"),
        ("https://[::1]/x", "https://[::1]/x"),
        // Encoded where a URL must be, so the opener sees one plain token.
        (
            "https://example.com/a\"b<c>",
            "https://example.com/a%22b%3Cc%3E",
        ),
        ("https://example.com/ü", "https://example.com/%C3%BC"),
        // A host that is not ASCII goes out in punycode, as confirmed.
        ("https://bücher.example/", "https://xn--bcher-kva.example/"),
        ("https://例え.jp/", "https://xn--r8jz45g.jp/"),
    ];
    for (raw, out) in accepted {
        assert_eq!(checked_url(raw).as_deref(), Ok(out), "{raw}");
    }
    let refused = [
        ("file:///etc/passwd", Refused::Scheme),
        ("javascript:alert(1)", Refused::Scheme),
        ("ms-msdt:/id PCWDiagnostic", Refused::Character),
        ("ms-msdt:/id", Refused::Scheme),
        ("search-ms:query=x", Refused::Scheme),
        ("datui-custom://x", Refused::Scheme),
        ("ftp://example.com/", Refused::Scheme),
        ("s3://bucket/key", Refused::Scheme),
        ("HTTPS://example.com", Refused::Scheme),
        ("Https://example.com", Refused::Scheme),
        ("hTTp://example.com", Refused::Scheme),
        ("https:example.com", Refused::Scheme),
        ("https:/example.com", Refused::Scheme),
        ("//example.com", Refused::Scheme),
        ("example.com", Refused::Scheme),
        ("-https://example.com", Refused::Scheme),
        (" https://example.com", Refused::Character),
        ("https://example.com ", Refused::Character),
        ("https://exa mple.com", Refused::Character),
        ("https://example.com/\n", Refused::Character),
        ("https://example.com/\tx", Refused::Character),
        ("https://example.com/\u{7f}", Refused::Character),
        ("https://example.com/\u{202e}gpj.exe", Refused::Character),
        ("https://example.com/\u{2066}x", Refused::Character),
        ("https:\\\\example.com", Refused::Character),
        ("https://example.com\\@evil.com", Refused::Character),
        ("https://user@example.com", Refused::Credentials),
        ("https://user:pw@example.com", Refused::Credentials),
        ("https://:pw@example.com", Refused::Credentials),
        ("https://", Refused::Malformed),
        ("https:///path", Refused::Malformed),
        ("https://exa%mple.com", Refused::Malformed),
    ];
    for (raw, why) in refused {
        assert_eq!(checked_url(raw), Err(why), "{raw:?}");
    }
}

fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let pairs: Vec<(String, String)> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    move |name| {
        pairs
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    }
}

/// A platform, the variables set, and whether that is a local desktop.
type Case = (Platform, &'static [(&'static str, &'static str)], bool);

#[test]
fn a_desktop_is_local_without_ssh_and_with_a_display() {
    use Platform::*;
    let cases: &[Case] = &[
        (Unix, &[("DISPLAY", ":0")], true),
        (Unix, &[("WAYLAND_DISPLAY", "wayland-1")], true),
        (Unix, &[], false),
        (Unix, &[("DISPLAY", " ")], false),
        // `ssh -X` sets DISPLAY; the browser would open on the far side.
        (
            Unix,
            &[("DISPLAY", "localhost:10.0"), ("SSH_CONNECTION", "a b c d")],
            false,
        ),
        (
            Unix,
            &[("WAYLAND_DISPLAY", "w"), ("SSH_TTY", "/dev/pts/1")],
            false,
        ),
        (MacOs, &[], true),
        (MacOs, &[("SSH_CONNECTION", "a b c d")], false),
        (Windows, &[], true),
        (Windows, &[("SSH_TTY", "x")], false),
    ];
    for (platform, vars, local) in cases {
        assert_eq!(
            local_desktop(*platform, env(vars)),
            *local,
            "{platform:?} {vars:?}"
        );
    }
}

/// The URL is one argument of a program; nothing is a shell.
#[test]
fn the_opener_is_a_program_with_the_url_as_one_argument() {
    let url = checked_url("https://example.com/a?b=1&c=^2").unwrap();
    assert_eq!(
        opener_argv(Platform::Windows, &url),
        ["rundll32", "url.dll,FileProtocolHandler", url.as_str()]
    );
    assert_eq!(opener_argv(Platform::MacOs, &url), ["open", url.as_str()]);
    assert_eq!(
        opener_argv(Platform::Unix, &url),
        ["xdg-open", url.as_str()]
    );
    for platform in [Platform::Windows, Platform::MacOs, Platform::Unix] {
        let argv = opener_argv(platform, &url);
        for shell in ["cmd", "sh", "bash", "powershell", "/c", "-c", "start"] {
            assert!(!argv.iter().any(|a| a == shell), "{platform:?}: {argv:?}");
        }
    }
}
