# Demos

The Datui demo animations are created using [vhs][vhs], which lets you script and
record keystrokes to a terminal app.

## Install `vhs`

See [vhs install instructions](https://github.com/charmbracelet/vhs?tab=readme-ov-file#installation).

## Define Tapes

The `vhs` application uses `.tape` files to script keystrokes. See Datui's [here][demo-tapes].
Only tape files matching the `{number}-{name}.tape` format (e.g. `01-basic-navigation.tape`)
are included when generating all demos.

## Generating the Animations

Run [generate_demos][generate-demos] to use `vhs` to generate an animated gif file for each
matching tape. The script builds the debug binary, then runs VHS in parallel (one process per
tape by default, using all available cores).

> The script runs a **debug** build and uses that binary when creating the demos.

```bash
# Generate all demos (parallel, uses all cores)
python3 scripts/demos/generate_demos.py
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
python3 scripts/demos/generate_demos.py --number 2

# Generate all demos using 4 worker processes
python3 scripts/demos/generate_demos.py -n 4
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
