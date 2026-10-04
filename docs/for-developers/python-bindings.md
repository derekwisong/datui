# Build Python bindings

The extension, `crates/datui-pyo3`, builds with maturin from `python/`. It is
not a member of the Cargo workspace. For the installed package, see
[Use datui from Python](../user-guide/python-module.md) and the
[Python API](../reference/python-api.md).

<a id="virtual-environment"></a>

## Set up

```bash,repo
python -m venv .venv
.venv/bin/pip install maturin "polars==1.43.*" "pytest>=7.0"
```

It also needs Rust and the Python headers (`python3-dev` on Debian and Ubuntu).
The [setup script](contributing.md) installs these into `.venv` too.

<a id="building-locally"></a>
<a id="testing"></a>

## Build and test

The `datui` command the wheel installs runs a bundled binary, found beside the
package rather than on `PATH`. Build it, copy it in, then build the extension:

```bash,repo
cargo build
mkdir -p python/datui_bin
cp target/debug/datui python/datui_bin/
cp LICENSE python/LICENSE
cd python && ../.venv/bin/maturin develop && cd ..
.venv/bin/pytest python/tests/ -v
```

On Windows, copy `target/debug/datui.exe`. Add `--release` to `maturin develop`
for an optimized build. The tests cover imports, options, invalid input and
serialized plans; where there is a pseudo-terminal, they also open the TUI and
check that a captured frame outlives it, and that the
[Python API](../reference/python-api.md) page lists every keyword.

<a id="running"></a>

## Run

```python
import polars as pl
import datui

datui.view(pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}))
```

`q` closes the view. The docs' Python blocks run with
`.venv/bin/python scripts/docs/doc_examples.py --python`, with this build
installed.

## Polars compatibility

Python frames cross into the extension as serialized LazyFrame plans. Rust
Polars **0.55** is paired with Python Polars **1.43**. The wheel declares
`polars>=1.38` with no upper bound, so installing it does not prove every plan
reads. Use the paired version when debugging a plan that does not.

The bridge checks the plan's DSL version and replaces its per-commit schema
hash with the receiver's; capture does the same in reverse. That handles
differing build hashes; it does not translate incompatible plans.

When the Rust Polars moves, change these together:

| File | Setting |
|---|---|
| `python/pyproject.toml` | The lowest Python Polars supported |
| `python/datui/__init__.py` | `PAIRED_POLARS` |
| `scripts/requirements-fixtures.txt` | The development and test Polars pin |

Run the Python tests after changing either side of the bridge.
