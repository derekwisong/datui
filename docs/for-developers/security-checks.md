# Security Checks

Datui runs two automated security checks alongside the usual format and clippy
gates. Both are in the `Security` workflow, and both can be run locally.

## What runs

**cargo-deny** checks `Cargo.lock` against the [RustSec advisory
database][rustsec], plus licenses, banned and duplicate crates, and the
registries dependencies come from. It runs against the root workspace and
against `crates/datui-pyo3`, which is excluded from the workspace and would
otherwise never be audited. Configuration is in `deny.toml` at the repository
root.

**zizmor** analyses the GitHub Actions workflow files for the patterns that let
a pull request steal a secret or poison a build: unpinned actions, over-broad
token permissions, expressions interpolated straight into shell, and cache
poisoning. Findings are reported to the repository's Security tab.

A third check, [OpenSSF Scorecard][scorecard], runs on a schedule in its own
workflow. It scores the repository's supply-chain posture and writes each check
to the Security tab with a specific remediation. It never fails a build.

## Running them locally

Install the tools once:

```bash
cargo install cargo-deny --locked
uv tool install zizmor          # or: pipx install zizmor
```

Then:

```bash
./scripts/code/check_security.sh
```

## When cargo-deny fails

Most advisory failures are cleared by updating the lockfile:

```bash
cargo update
cargo deny check advisories
```

If an advisory cannot be cleared, because the fix is in a version some other
dependency will not accept, add it to the `ignore` list in `deny.toml` with two
things written down: why it is acceptable today, and the event that should clear
it. An entry without both is a silenced alarm rather than a decision.

The current entries are all of that shape. The two `quick-xml` denial-of-service
advisories are the ones worth watching: they are reachable whenever datui opens
an `.xlsx` file, which is untrusted data, and they clear when `calamine` can be
upgraded past its `quick-xml` 0.38 pin.

## Adding a new action to a workflow

Pin it to a full commit SHA, with the version tag in a trailing comment:

```yaml
- uses: actions/checkout@fbc6f3992d24b796d5a048ff273f7fcc4a7b6c09 # v5.1.0
```

A tag is a mutable pointer. Whoever controls the upstream repository can move
`v5` to point at anything, and every workflow that trusts the tag will run it on
the next build. A SHA cannot be moved. Dependabot is configured to raise pull
requests when a pinned action has a newer release, so pinning does not mean
going stale.

Resolve the SHA for a tag with:

```bash
gh api repos/actions/checkout/tags --jq '.[] | select(.name == "v5.1.0") | .commit.sha'
```

[rustsec]: https://rustsec.org/
[scorecard]: https://github.com/ossf/scorecard
