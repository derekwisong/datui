# Python Bindings

Build with **maturin** from `python/`. The extension crate,
`crates/datui-pyo3`, is separate from the Cargo workspace.
For using the installed package, see the [Python guide](../user-guide/python-module.md).

<a id="virtual-environment"></a>

## Set up

From the repository root, create or activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
pip install maturin "polars==1.43.*" "pytest>=7.0"
```

You also need Rust and Python development headers (`python3-dev` on Debian/Ubuntu).
The [setup script](setup-script.md) creates `.venv`, but does not install
maturin or pytest; install those separately as above.

<a id="building-locally"></a>
<a id="testing"></a>

## Build and test

```bash
cd python
maturin develop
cd ..
pytest python/tests/ -v
```

Add `--release` to `maturin develop` for an optimized build. The tests cover
imports, options, invalid inputs and serialized plans. On platforms with PTY
support, they also open the TUI and check that a captured frame survives closing it.

<a id="running"></a>

## Run

```python
import datui
import polars as pl

datui.view(pl.scan_csv("data.csv"))
```

Press `q` to close the view. See [capture](../user-guide/python-module.md)
for returning the edited view as a LazyFrame.

The Python `datui` command needs a bundled binary. For local development,
build and copy it from the repository root, then rerun maturin:

```bash
cargo build
mkdir -p python/datui_bin
cp target/debug/datui python/datui_bin/
cd python
maturin develop
```

On Windows, copy `target/debug/datui.exe` instead. The console script looks
beside the package for this binary; it does not search `PATH`. You can also
run `target/debug/datui` directly.

## Polars compatibility

Python frames cross the extension boundary as serialized LazyFrame plans.
Rust Polars **0.55** is paired with Python Polars **1.43**. The wheel declares
`polars>=1.38` without an upper bound, so installation alone does not prove
that every plan is compatible. Use the paired version when debugging a plan
that cannot be read.

The bridge checks the plan's DSL version and replaces its per-commit schema
hash with the receiver's hash. Capture uses the same process in reverse.
This handles differing build hashes; it does not translate incompatible plans.

When upgrading Rust Polars, review these together:

| File | Setting |
|---|---|
| `python/pyproject.toml` | Minimum supported Python Polars version |
| `python/datui/__init__.py` | `PAIRED_POLARS` |
| `scripts/requirements.txt` | Development/test Polars pin |

Run the Python tests after changing either side of the bridge.
