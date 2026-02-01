# PyPI Deployment Checklist

This document summarizes what is in place and what remains to deploy the datui Python package to PyPI.

## Already in place

- **Python package** (`python/`): `pyproject.toml`, README, tests; maturin build from `crates/datui-pyo3`.
- **Console script**: `datui = "datui:run_cli"` so `pip install datui` provides the `datui` command (uses bundled binary when present).
- **Release workflow** (`.github/workflows/release.yml`):
  - Triggered on tags `v*`.
  - **pypi** job: builds release binary, copies to `python/datui_bin/`, runs `maturin build --release --out dist` from `python/`, uploads `python/dist/*.whl` to the GitHub release, then runs `twine upload --skip-existing python/dist/*.whl` when `PYPI_API_TOKEN` is set.
- **Version alignment**: `scripts/bump_version.py release` updates root `Cargo.toml`, `crates/datui-lib`, `crates/datui-cli`, and `python/pyproject.toml`; use it before tagging (e.g. `bump_version.py release --commit --tag`).
- **CI**: Python job builds the extension with maturin and runs pytest (see note below on polars version).

## Remaining for PyPI deploy

### 1. PyPI metadata in `python/pyproject.toml`

Add standard fields so the package is valid and discoverable on PyPI:

- **License**: e.g. `license = "MIT"` (repo uses MIT).
- **Classifiers** (optional but recommended): e.g. `Programming Language :: Python :: 3`, `License :: OSI Approved :: MIT License`, `Programming Language :: Rust`, etc.

Example addition under `[project]`:

```toml
license = "MIT"
classifiers = [
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Rust",
    "Topic :: Scientific/Engineering",
]
```

Optional: `authors`, `maintainers`, `keywords`, `repository`, `homepage`.

### 2. PyPI API token (secret)

The release step only runs when a secret is present:

```yaml
- name: Publish to PyPI
  if: secrets.PYPI_API_TOKEN != ''
```

To enable publishing:

1. Create a PyPI account (or use an existing one).
2. Create an API token at [PyPI Account → API tokens](https://pypi.org/manage/account/token/).
3. Add a repository secret in GitHub: **Settings → Secrets and variables → Actions → New repository secret**; name it `PYPI_API_TOKEN` and paste the token.

Without this secret, the workflow still builds and uploads wheels to the GitHub release; it simply skips `twine upload`.

### 3. CI Python job: align polars version (recommended)

The CI Python job installs `polars>=0.20` for tests, but the package requires `polars>=1.35,<1.36`. To avoid testing against an incompatible Polars version, change the install line in `.github/workflows/ci.yml` (in the job that runs `maturin develop` and pytest) to use the same constraint, e.g.:

```yaml
pip install maturin "polars>=1.35,<1.36" "pytest>=7.0"
```

### 4. Optional: multi-platform wheels

The current **pypi** job runs only on `ubuntu-latest`, so only a Linux x86_64 wheel is built and uploaded. To publish wheels for macOS (x86_64 + arm64) and Windows:

- Add a **matrix** (e.g. `runs-on: [ubuntu-latest, macos-latest, windows-latest]`) and build the wheel per runner.
- Collect all `python/dist/*.whl` (and optionally sdist) as artifacts, then run a single **twine upload** step that uploads every built file (e.g. from a job that downloads all artifacts and runs `twine upload --skip-existing **/*.whl **/*.tar.gz`).

You can ship a Linux-only wheel first and add more platforms later.

### 5. Optional: source distribution (sdist)

PyPI allows uploading an sdist (e.g. `datui-0.2.17.tar.gz`) so users can `pip install` from source. Maturin can build an sdist; add that to the build step if desired and include the resulting `.tar.gz` in the `twine upload` step.

## Release flow (summary)

1. Bump and prepare release:  
   `python scripts/bump_version.py release --commit --tag`  
   This updates versions (including `python/pyproject.toml`) and creates the tag (e.g. `v0.2.17`).
2. Push and push tags:  
   `git push && git push --tags`
3. GitHub Actions runs on the tag: builds binary, docs, packages, and the Python wheel; creates/updates the GitHub release and uploads assets.
4. If `PYPI_API_TOKEN` is set, the workflow runs `twine upload --skip-existing python/dist/*.whl` and the package appears on PyPI.

After the first successful run with the token, `pip install datui "polars>=1.35,<1.36"` will install from PyPI.
