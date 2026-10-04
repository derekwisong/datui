# Record demos

```bash,repo
python demo/build.py
python scripts/demos/generate_demos.py --number 2
```

Run from the repository root. The first command downloads public demo data;
the second builds datui and records the baby-name query demo with
[VHS](https://github.com/charmbracelet/vhs).

## Prerequisites

Install VHS and its dependencies using its
[installation guide](https://github.com/charmbracelet/vhs#installation), plus
**JetBrainsMono Nerd Font**, the font named by the tapes. The data builder
uses the Python packages in `scripts/requirements.txt`.

## Data and tapes

| Path | Purpose |
|---|---|
| `demo/build.py` | Downloads and prepares public datasets as Parquet |
| `demo/DATA-LICENSES.md` | Sources, licenses and transformations |
| `demo/data/` | Inputs opened by the tapes |
| `scripts/demos/NN-name.tape` | Keystrokes and recording settings |
| `demos/NN-name.gif` | Generated animations |

Only tapes matching `{number}-{name}.tape` are recorded. Some legacy tapes
require quant-research or Bitcoin snapshots with no public download; those
inputs are separate from the public data build. Use a public-data tape when
recording from a fresh checkout. SSA may require a manual download; the data
builder accepts `--names-zip /path/to/names.zip`.

## Record the animations

| Command | Result |
|---|---|
| `python scripts/demos/generate_demos.py --number 2` | Record one tape |
| `python scripts/demos/generate_demos.py -n 4` | Record all tapes with four workers; requires every tape's inputs |
| `python scripts/demos/generate_demos.py --release --number 2` | Record using a release build |

The default is a debug build and one worker per available CPU. The
[documentation builder](documentation.md) copies the GIFs into each built
book's `demos/` directory.

## The home-screen demo records against a fixture

Tapes `12` through `16` use an isolated fixture created by
`scripts/demos/make-home-fixture.py`. The generator runs it before recording.
It supplies:

- A workspace under `/tmp/datui-demo`, copied from `demo/data`.
- A separate cache with seeded recent entries.
- A config with desktop recents disabled.
- An empty home directory and no inherited cloud credentials.

The generator also removes `NO_COLOR` and sets `COLORTERM=truecolor`.
Inspect `demo/data` before recording: the fixture copies that directory,
including any additional files you have put there.
