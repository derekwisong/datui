# Demos

The Datui demo animations are created using [vhs][vhs], which lets you script and
record keystrokes to a terminal app.

## Prerequisites

### VHS

See [vhs install instructions](https://github.com/charmbracelet/vhs?tab=readme-ov-file#installation).

### JetBrainsMono Nerd Font

- Get it from [nerdfonts.com](https://www.nerdfonts.com/font-downloads) and install it manually
- Get it from your distribution's package manager
  - [Arch Linux](https://archlinux.org/packages/extra/any/ttf-jetbrains-mono-nerd/)
  - Other distributions may have their own packages


## The demo data

Every tape opens a file under `demo/data`: real datasets, chosen because the
features show better on data with a story (the heaviest meteorites, the great
earthquakes, one name's rise and fall). `demo/build.py` downloads the public ones
and writes them as Parquet; `demo/DATA-LICENSES.md` records where each came from,
its license, and what the build changed. Two snapshots (a quant-research extract
and daily Bitcoin chain statistics) have no public source and are copied in with
`--private`.

## Define Tapes

The `vhs` application uses `.tape` files to script keystrokes. See Datui's [here][demo-tapes].
Only tape files matching the `{number}-{name}.tape` format (e.g. `01-basic-navigation.tape`)
are included when generating all demos.

## The home-screen demo records against a fixture

Every other tape opens a file it names. The home screen instead shows *what is around
you* — recents, the working directory, configured directories, and the desktop's
recently-used list — so recording it on a developer's machine would put that machine's
contents on screen. That is a privacy problem before it is anything else: the desktop's
list in particular holds whatever you last opened anywhere, which is regularly
something you would not publish.

So the home-screen tapes (`12` through `16`) record against a fixture, built by
[`make-home-fixture.py`][home-fixture], which the generator runs first:

- its own workspace under `/tmp/datui-demo`: a copy of `demo/data`, so the home
  screen lists the same datasets the other tapes open, hive-partitioned trees and
  all, because its whole point is what it can tell you about data before you open it;
- its own `DATUI_CACHE_DIR`, so `Recent` is seeded rather than inherited;
- its own `XDG_CONFIG_HOME` holding a config with `use_desktop_recents = false`, which
  is the line that matters: that list is the one input which can put a file from
  anywhere on the machine into the recording;
- its own empty `HOME`, with inherited cloud credential variables removed before VHS
  starts, so only the built-in public datasets appear under `CLOUD`.

The generator gives VHS those variables directly. The tape only changes into the
fixture workspace while its terminal is hidden. It also removes `NO_COLOR` and sets
`COLORTERM=truecolor`, so the recording uses datui's palette even when it is generated
from colorless automation.

The upshot is that regenerating this GIF gives the same result on any checkout, and
shows nothing belonging to whoever ran it.

## Generating the Animations

Run [generate_demos.py][generate-demos] to use `vhs` to generate an animated gif file for each
matching tape. The script builds the debug binary, then runs VHS in parallel (one process per
tape by default, using all available cores).

> The script runs a **debug** build and uses that binary when creating the demos.

```bash
# Generate all demos (parallel, uses all cores)
python scripts/demos/generate_demos.py
```

Or if the script is executable:

```bash
scripts/demos/generate_demos.py
```

### Useful command-line options

| Option | Short | Description |
|--------|-------|-------------|
| `--number N` | `-N` | Generate only the demo with this number (e.g. `-N 2` for `02-querying.tape`). |
| `--workers N` | `-n` | Number of parallel workers (default: all available cores). |

Examples:

```bash
# Generate only the second demo
python scripts/demos/generate_demos.py --number 2

# Generate all demos using 4 worker processes
python scripts/demos/generate_demos.py -n 4
```

The animations will be placed in the [demos][demos] directory. A percentage bar shows
progress as each tape finishes.

> During the creation of the [documentation](documentation.md), these animations are
> copied into a `demos/` subdirectory of the generated site. From there, the files may be
> referenced from within the docs.

---

[vhs]: https://github.com/charmbracelet/vhs
[generate-demos]: https://github.com/derekwisong/datui/tree/main/scripts/demos/generate_demos.py
[demo-tapes]: https://github.com/derekwisong/datui/tree/main/scripts/demos
[demos]: https://github.com/derekwisong/datui/tree/main/demos

[home-fixture]: https://github.com/derekwisong/datui/blob/main/scripts/demos/make-home-fixture.py
