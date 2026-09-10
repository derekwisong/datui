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

Releases publish `SHA256SUMS`, and the install script checks it. There are no
signatures yet, so a checksum proves the file arrived intact, not who built it.
