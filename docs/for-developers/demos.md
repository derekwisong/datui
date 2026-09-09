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

So `12-home-screen.tape` records against a fixture, built by
[`make-home-fixture.py`][home-fixture], which the generator runs first:

- its own workspace of generated Parquet under `/tmp/datui-demo`, laid out like a real
  research tree — hive-partitioned by year, with related datasets — because the home
  screen's whole point is what it can tell you about data before you open it, and that
  needs data with a shape;
- its own `DATUI_CACHE_DIR`, so `Recent` is seeded rather than inherited;
- its own `XDG_CONFIG_HOME` holding a config with `use_desktop_recents = false`, which
  is the line that matters: that list is the one input which can put a file from
  anywhere on the machine into the recording.

The tape exports those two variables itself, with `Hide` … `Show` around them, because
VHS 0.11 has no `Set Env`.

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
