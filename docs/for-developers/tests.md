# Tests

```bash
./scripts/dev/setup-test-data.sh   # once: creates .venv and generates the fixtures
cargo test --workspace
```

`cargo test` alone runs only the root package. `--workspace` adds `datui-lib`
and `datui-cli`, which is what CI runs. The Python bindings are tested
separately; see [Python Bindings](python-bindings.md#testing).

## Fixtures

The statistics, distribution-detection and pivot/melt tests read sample files
that are too large to commit. `scripts/dev/setup-test-data.sh` creates `.venv`,
installs `scripts/requirements.txt` (Polars, NumPy, pyarrow, fastavro,
openpyxl) and generates them, using [uv](https://github.com/astral-sh/uv) when
it is installed and `python -m venv` otherwise. It is safe to re-run;
`--force` regenerates from scratch.

The test harness looks for `.venv/bin/python` (`.venv\Scripts\python.exe` on
Windows) and falls back to the system Python, so the environment does not need
to be activated. If the fixtures are missing when the tests start, they run the
generator themselves.

To regenerate by hand:

```bash
.venv/bin/python scripts/generate_sample_data.py
```

The fixtures are not regenerated automatically once they exist.

## Layout

| Path | Tests |
|---|---|
| `tests/integration_test.rs` | Load, query, display, end to end |
| `tests/statistics_test.rs`, `tests/distribution_detection_test.rs` | Analysis |
| `tests/pivot_melt_backend_test.rs` | Reshaping |
| `tests/template_test.rs` | Templates and their scoring |
| `tests/home_test.rs`, `tests/search_test.rs`, `tests/locality_test.rs` | Home screen, recursive search, filesystem detection |
| `tests/config_test.rs`, `tests/config_integration_test.rs`, `tests/theme_application_test.rs` | Configuration and themes |
| `tests/cloud_live_test.rs` | Against a real object store. Ignored by default; run with `DATUI_LIVE_GCS=1` or `DATUI_LIVE_S3=<endpoint>` and `--ignored` |
| `tests/common/` | Shared helpers |

Unit tests live beside the code they test.
