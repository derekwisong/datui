# Security Policy

## Reporting a vulnerability

Please report security issues privately, through GitHub's private vulnerability
reporting:

**[Report a vulnerability](https://github.com/derekwisong/datui/security/advisories/new)**

That opens a private draft advisory visible only to you and the maintainer. It
is the right channel even if you are not sure whether what you found is a
security issue.

Please do not open a public issue for a suspected vulnerability. A public issue
tells everyone about the problem before there is a fix, including people who
would rather use it than report it.

Helpful things to include, as far as you have them:

- What an attacker gains, in one sentence.
- The steps to reproduce, and the datui version and platform.
- A sample file, if a particular input triggers it. A minimal one is easier to
  work with than a real dataset.

### What to expect

| | |
|---|---|
| Acknowledgement | Within 7 days |
| Assessment and fix timeline | After triage, since it depends on severity |
| Credit | Named in the advisory, unless you would rather not be |

datui is maintained by one person. A fix timeline comes after the report has
been read and reproduced rather than being promised up front. You will get a
straight answer about severity and timing rather than an optimistic one.

Please keep the details private until a fix is released, or for 90 days,
whichever comes first. If a fix is taking longer than that, say so and we will
agree on what happens next.

## Supported versions

Only the most recent release receives security fixes. A fix ships as a new
release across every channel datui publishes to: crates.io, PyPI, Homebrew,
the AUR, WinGet, and the APT repository.

datui is pre-1.0 and moving quickly. This policy will tighten at 1.0.

## What datui defends against

The short version: **datui trusts the person running it, and does not trust the
data it opens.**

That distinction is what decides whether something is a vulnerability.

### In scope

Anything that lets a **file or URL** do something the person who opened it did
not ask for. datui reads Parquet, CSV, JSON, Avro, Arrow, ORC and Excel, over
local paths, S3, GCS and HTTP, and none of that content is trusted. Reports are
wanted for:

- Memory corruption, or any crash that looks exploitable, from a malformed file.
- Escaping the terminal: a file whose contents, column names or filename emit
  escape sequences that change the terminal's state, write to the clipboard, or
  spoof the interface.
- Reading or writing files outside what was opened, including through export
  paths, template files, cache and temporary files.
- Making network requests that were not asked for, or sending data somewhere
  the user did not name.
- Leaking credentials. In particular, S3 keys appearing in error messages,
  debug output, cache files, or saved templates.
- Anything in the release and packaging pipeline that would let a third party
  influence a published artifact.

### Out of scope

**A serialized query plan passed to the Python API is trusted input.**
`datui.view()` accepts a Polars `LazyFrame`, which reaches the extension as a
serialized plan. A plan is closer to a program than to data: it can name
arbitrary file paths to read. Building something that feeds attacker-supplied
plan bytes to `datui.view()` is a bug in that program, in the same way that
passing attacker input to `eval()` is.

For what it is worth, this build cannot execute arbitrary code from a plan.
Polars' Python user-defined-function support is not compiled in, so the plan
nodes that carry pickled Python callables cannot be deserialized. That is a
property of the current build configuration, not a security boundary, and it is
not something to rely on.

Also out of scope:

- **Denial of service against yourself.** A file that makes datui hang or
  exhaust memory is a bug worth reporting as a normal issue, but opening a
  hostile file in a local viewer you launched is not a privilege boundary.
- **Vulnerabilities in dependencies** with no path to reach them from datui.
  Advisories that are reachable are tracked in `deny.toml` with the reasoning.
- **Anything requiring an attacker who can already write to your home
  directory**, since at that point the config file is the least of it.
- Missing hardening flags, or scanner output, without a demonstrated impact.

## How datui is checked

Every pull request runs `cargo-deny` against the RustSec advisory database and
`zizmor` against the workflow files. OpenSSF Scorecard reports supply-chain
posture on a schedule. See
[Security Checks](https://derekwisong.github.io/datui/latest/for-developers/security-checks.html)
for what those cover and how to run them locally.

## Verifying what you install

Being straight about where this currently stands.

The APT repository is signed, and its GPG key is fetched over HTTPS during
setup. The AUR package carries checksums in its `PKGBUILD`.

Release tarballs, zips and wheels are **not** currently published with a
checksum file or a signature, which means the one-line installer and a manual
download from the releases page both trust HTTPS and GitHub alone. Publishing
signed checksums, and verifying them in the installer, is known work that has
not been done yet.

If you want the strongest assurance available today, build from source:

```bash
git clone https://github.com/derekwisong/datui.git
cd datui
cargo build --release --locked
```
