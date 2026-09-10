# Security Policy

## Reporting a vulnerability

**[Report it privately here.](https://github.com/derekwisong/datui/security/advisories/new)**

Please don't open a public issue for a suspected vulnerability. That discloses
it before there is a fix.

Include what you can:

- What an attacker gains.
- Steps to reproduce.
- Your datui version and platform.
- A sample file, if a particular input triggers it.

Fixes ship in the next release. datui is pre-1.0, so there are no backports.

## Scope

**datui trusts the person running it. It does not trust the data it opens.**

In scope is anything a file or URL can do that the person who opened it did not
ask for: memory corruption or a suspicious crash from a malformed file, escape
sequences reaching the terminal, reading or writing files outside what was
opened, unrequested network traffic, leaked S3 credentials, or anything that
lets a third party influence a published release artifact.

Out of scope:

- **A serialized query plan passed to `datui.view()` is trusted input.** A plan
  can name arbitrary paths to read, so feeding it attacker-supplied bytes is a
  bug in the calling program, like passing attacker input to `eval()`.
- A file that makes datui hang or exhaust memory. Report it as a normal issue.
- Dependency advisories with no reachable path from datui. Reachable ones are
  tracked in `deny.toml`.
- Anything needing an attacker who can already write to your home directory.

## Proactive testing

The parsers and matchers that run on untrusted input are fuzzed with
[cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz): the query language, the number
renderer, the fuzzy matcher, the config glob matcher, and config loading. Every pull
request replays the committed corpus as a regression gate, and the Nightly workflow
fuzzes for new findings. See
[Fuzzing](https://derekwisong.github.io/datui/latest/for-developers/fuzzing.html) for how to
run them, and `fuzz/` for the targets.

## Verifying a release

Releases publish `SHA256SUMS`, and the install script checks it. That proves a
download arrived intact, but not who produced it: anyone who could replace an
artifact could replace the checksum file beside it.

`SHA256SUMS.sigstore.json` is what proves origin. It is a [Sigstore][sigstore]
signature over the checksum file, made by the release workflow itself rather than
by a key any person holds, so verifying it tells you the file was produced by this
repository's release workflow at this tag. Because every artifact is listed in
`SHA256SUMS`, verifying that one signature covers the whole release.

With [cosign][cosign] installed:

```bash
cosign verify-blob \
  --bundle SHA256SUMS.sigstore.json \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github.com/derekwisong/datui/\.github/workflows/release\.yml@refs/tags/' \
  SHA256SUMS
```

Then check the artifacts against the file it just vouched for:

```bash
sha256sum -c SHA256SUMS
```

The signing certificate is recorded in Rekor, Sigstore's public transparency log,
which is what lets you verify without having to trust us to hand you the right key.

[sigstore]: https://www.sigstore.dev/
[cosign]: https://github.com/sigstore/cosign
