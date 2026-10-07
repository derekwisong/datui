# Build Python bindings

The extension, `crates/datui-pyo3`, builds with maturin from `python/`. It is
not a member of the Cargo workspace. For the installed package, see
[Use datui from Python](../user-guide/python-module.md) and the
[Python API](../reference/python-api.md).

<a id="virtual-environment"></a>

## Set up

```bash,repo
./scripts/dev/test.sh setup --wheel
```

It installs maturin, pytest and the pinned Polars into `.venv`. The build also
needs Rust and the Python headers (`python3-dev` on Debian and Ubuntu).

<a id="building-locally"></a>
<a id="testing"></a>

## Build and test

```bash,repo
./scripts/dev/test.sh python
```

That is what CI's Python job runs. The `datui` command the wheel installs runs a
bundled binary, found beside the package rather than on `PATH`, so by hand:
build it, copy it in, then build the extension:

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

A DataFrame crosses into the extension over the Arrow C stream
(`view_from_arrow`), which does not change between Polars releases. A
LazyFrame crosses as a serialized plan. Rust Polars **0.55** is paired with
Python Polars **1.43**. The wheel declares `polars>=1.38` with no upper bound, so
installing it does not prove every plan reads. Use the paired version when
debugging a plan that does not.

The bridge checks the plan's DSL version and replaces its per-commit schema
hash with the receiver's. That handles differing build hashes; it does not
translate incompatible plans. Polars 2.0 keeps the DSL version but adds required
fields, so it cannot read the plans 0.55 writes.

A capture comes back as a `Captured`: `plan()` gives the plan bytes, and
`__arrow_c_stream__` collects the view and streams its rows. The wrapper reads
the plan, and takes the rows with a `UserWarning` when its Polars cannot, when
`plan()` fails (an anonymous scan or opaque function has no plan form), or
without asking for the plan when its Polars is another major release. On the
paired release an unreadable plan raises instead: it is a bug. CI runs the
Python tests against the pinned 1.43, the 1.38 floor and 2.x, where captures
come back as rows, and the docs' Python examples against 2.x too.

When the Rust Polars moves, change these together:

| File | Setting |
|---|---|
| `python/pyproject.toml` | The lowest Python Polars supported |
| `python/datui/__init__.py` | `PAIRED_POLARS` |
| `scripts/requirements-fixtures.txt` | The development and test Polars pin |

Run the Python tests after changing either side of the bridge.
