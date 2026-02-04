# PyPI Deployment

GitHub actions publish the Python package as part of the release workflow.


## Trigger

- **Release workflow** (`.github/workflows/release.yml`) runs on **push of a tag `v*`** (e.g. `v0.2.31`).
- **Prerequisite**: CI must have passed for that commit. Recommended: push to `main` → wait for CI → then create and push the tag.

## What gets built

Wheels are built with **maturin** from `python/`: the Rust extension comes from `crates/datui-pyo3`, and the release binary is copied into `python/datui_bin/` before `maturin build` so the wheel ships a bundled `datui` CLI.

## PyPI

- **Publish step**: `twine upload` delivers the wheels to PyPI (Linux and Windows wheels).
- **Version**: Keep `python/pyproject.toml` version in sync with the release; `scripts/bump_version.py release` updates the root crate and the Python package.
