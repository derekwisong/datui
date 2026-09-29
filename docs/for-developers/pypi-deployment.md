# PyPI Deployment

Push a release tag after CI passes to build the wheels. See [packaging and releases](packaging.md#ci-and-releases) for the related artifacts.

| Stage | Workflow | Result |
|---|---|---|
| Build | `release.yml` | Linux x86_64, Windows x86_64, and macOS ARM64/x86_64 wheels attached to the GitHub release |
| Publish | `publish-packages.yml` | Downloads those wheels and uploads them with `twine` |

Wheels use **maturin** from `python/`. They contain the Rust extension from
`crates/datui-pyo3` and a bundled CLI copied into `python/datui_bin/` before the build.
Linux ARM64 currently gets a standalone binary, not a wheel.

The release workflow calls **Publish packages** after creating the release.
That workflow can also be run manually with a release tag; leaving the tag blank
uses the latest GitHub release. PyPI publishing uses the `PYPI_API_TOKEN` secret.

Use `scripts/bump_version.py` to keep the Rust crates, lockfiles and Python
package version in sync.
