# Python Bindings

The **datui-pyo3** crate and the **datui** Python package let you open the datui TUI from Python with a Polars `LazyFrame` or `DataFrame` (e.g. `datui.view(lf)` or `datui.view(df)`). Data is passed via **binary** (from `LazyFrame.serialize()`). Use a **Polars Python version built with the same Rust Polars as this extension** (e.g. polars 1.35.x uses Rust 0.51). The crate lives at `crates/datui-pyo3` and is excluded from the Cargo workspace; it is built with **maturin** from the `python/` directory.

## Summary

| Task | Command |
|------|--------|
| Build and install (development) | `cd python && maturin develop` |
| Run Python tests | `pytest python/tests/ -v` |
| Use in Python | `import datui; datui.view(lf)` or `datui.view(df)` |

Use a **virtual environment** so the extension and package are installed into that env.

---

## Virtual environment

Use a venv so the datui package and its dependencies (e.g. polars) are isolated.

The [Setup Script](setup-script.md) (`scripts/setup_dev.py`) creates `.venv` and installs `scripts/requirements.txt`
which contain all the requirements here.

**Create and activate (from repo root):**

```bash
python -m venv .venv
source .venv/bin/activate
```

**Install build and test dependencies in the venv:**

```bash
pip install --upgrade pip
pip install maturin "polars>=0.20" "pytest>=7.0"
```

---

## Building locally

Build and install the full **datui** package (Python wrapper + Rust extension) into the current environment. From the **python/** directory:

```bash
# Activate venv first (see above)
cd python
maturin develop
```

- **Debug** (default): faster to compile, larger binary. Omit `--release`.
- **Release**: add `--release` for a smaller, faster binary:

  ```bash
  maturin develop --release
  ```

You need **Rust** and **Python development headers** (e.g. `python3-dev` on Debian/Ubuntu). Maturin will use the Python that runs `maturin` (or the one in your activated venv). From the repo root you can run `cd python && maturin develop`.

---

## Testing

With the package installed in your venv (after `cd python && maturin develop`), run the Python tests from the **repo root**:

```bash
pytest python/tests/ -v
```

Tests check that the module imports, that `view`, `view_from_json`, and `run_cli` exist, and that invalid inputs raise (they do not run the TUI).

---

## Running

**In Python (view a LazyFrame):**

```python
import polars as pl
import datui

lf = pl.scan_csv("data.csv")
datui.view(lf)   # Opens TUI; press q to exit
```

**CLI from the same env:**  
If you built the datui binary (`cargo build` (from repo root)) and it’s on your `PATH`, the `datui` console script (from `pip` / maturin) will use it. Otherwise install the CLI separately (e.g. from GitHub releases or your system package manager).

---

## More

- User-facing install and usage: [python/README.md](../../python/README.md) in the repo.
- PyPI package: `pip install datui`
