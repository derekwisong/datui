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

That is what CI's Python job runs. To do it by hand, note that the `datui`
command the wheel installs runs a bundled binary, found beside the package
rather than on `PATH`. So build the binary, copy it into the package, then build
the extension:

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
serialized plans, and check that the [Python API](../reference/python-api.md)
page lists every keyword. Where there is a pseudo-terminal, they also open the
TUI and check that a captured frame outlives it.

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
a successful install does not mean every plan can be read. When debugging a
plan that cannot be read, use the paired version.

The bridge checks the plan's DSL version and replaces its per-commit schema
hash with the receiver's. That handles differing build hashes; it does not
translate incompatible plans. Polars 2.0 keeps the DSL version but adds required
fields, so it cannot read the plans 0.55 writes.

A capture comes back as a `Captured`: `plan()` gives the plan bytes, and
`__arrow_c_stream__` collects the view and streams its rows. The Python wrapper
reads the plan when it can. It takes the rows instead, with a `UserWarning`,
when:

- the installed Polars cannot read the plan;
- `plan()` fails (an anonymous scan or opaque function has no plan form); or
- the installed Polars is another major release, in which case it does not ask
  for the plan at all.

On the paired release, an unreadable plan raises instead, since it is a bug. CI
runs the Python tests against the pinned 1.43, the 1.38 floor and 2.x (where
captures come back as rows), and the docs' Python examples against 2.x too.

When the Rust Polars moves, change these together:

| File | Setting |
|---|---|
| `python/pyproject.toml` | The lowest Python Polars supported |
| `python/datui/__init__.py` | `PAIRED_POLARS` |
| `scripts/requirements-fixtures.txt` | The development and test Polars pin |

Run the Python tests after changing either side of the bridge.
