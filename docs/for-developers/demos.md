# Record demos

```bash,repo
cargo build --release
python3 scripts/demos/capture.py --bin target/release/datui
python3 scripts/demos/capture.py --publish
```

Run from the repository root. The first command builds the binary to record,
the second records every tape with [VHS](https://github.com/charmbracelet/vhs)
into a scratch directory, and the third copies the outputs, once reviewed,
into `demos/`.

## Prerequisites

| Tool | For |
|---|---|
| `vhs`, `ttyd`, `ffmpeg` | Recording; VHS's [installation guide](https://github.com/charmbracelet/vhs#installation) |
| Chromium or Chrome | VHS's renderer |
| The font in `scripts/demos/header.tape` | It must pass `scripts/code/audit_glyphs.py` ([glyph audit](glyph-audit.md)) |
| A network connection | Every tape opens the built-in catalog's public data |

## Options

| Command | Does |
|---|---|
| `capture.py --list` | Lists the tapes: kind, expected length, where each is used |
| `capture.py teaser shots-food` | Records only the tapes named |
| `capture.py --out DIR` | Writes to `DIR`; the default is `datui-captures` in the system temp directory |
| `capture.py --dry-run` | Shows each take's fixture, `datui` command, scrubbed variables and outputs |
| `capture.py --cache-dir DIR` | Shares one datui cache across takes, warm after the first; each take starts cold otherwise |
| `capture.py --network-note TEXT` | Records the connection for captions |
| `capture.py --keep-fixtures` | Keeps each take's scratch HOME, config and cache |
| `capture.py --publish` | Copies the outputs in `--out` into `demos/`; records nothing |

## Tapes and outputs

| Path | What |
|---|---|
| `scripts/demos/header.tape` | The settings every take shares: 120 × 32 cells at 18 px, font, terminal colors |
| `scripts/demos/teaser.tape`, `noaa-cloud.tape` | The two GIFs: `NAME.gif`, `NAME.webm`, and a poster, `NAME.png`, from the last frame |
| `scripts/demos/shots-*.tape` | Screenshots, one per `Screenshot` line, in `screenshots/`; `review/` holds each take's WebM and last frame |
| `scripts/demos/theme-gallery.tape` | One screen per theme in `themes/`, and `theme-gallery.png`, two by two |
| `NAME.json` beside each output | The datui version and commit, hardware, network, cold or warm cache, time and length, for captions |

A tape holds keys, waits and `Screenshot` lines; no `Set`, `Output` or
`Source`. Setup goes inside `Hide`/`Show`; waits for the network stay on
camera, as `Wait+Screen`. A number in a rolling dataset (NOAA, earthquakes) is
never a wait's pattern.

## Isolation

Each take runs in its own fixture, removed afterward:

- `HOME`, the XDG directories, `DATUI_CONFIG_DIR` and `DATUI_CACHE_DIR` in
  scratch; the working directory is an empty `~/demo`.
- No cloud credentials: `AWS_`, `AZURE_`, `GOOGLE_`, `GCP_`, `CLOUDSDK_`
  and the other login variables are unset, and so are `NO_COLOR` and `DATUI_`
  settings.
- No shell history; the prompt is `$ `.
- `datui` on the take's `PATH` adds `-c cloud.discover=false` and the theme,
  and downloads into the take's temp directory.

Look at every output at its published size before `--publish`: no paths
outside the fixture, no account details, no stray dialogs.
