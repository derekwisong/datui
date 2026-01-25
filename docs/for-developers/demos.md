# Demos

The Datui demo animations are created using [vhs][vhs], which lets you script and
record keystrokes to a terminal app.

## Install `vhs`

See [vhs install instructions](https://github.com/charmbracelet/vhs?tab=readme-ov-file#installation).

## Define Tapes

The `vhs` application uses `.tape` files to script keystrokes. See Datui's [here][demo-tapes].

## Generating the Animations

Run [generate-all][generate-all] to use `vhs` to generate an animated gif file for each tape.

> The generate-all script will first run a release build, and then use that version of the
> application when creating the demos.

```bash
python3 scripts/demos/generate-all.py
```

Or if the script is executable:

```bash
scripts/demos/generate-all.py
```

The animations will be placed in the [demos][demos] directory.

> During the creation of the [documentation](documentation.md), these animations are
> copied into a `demos/` subdirectory of the generated site. From there, the files may be
> referenced from within the docs.

---

[vhs]: https://github.com/charmbracelet/vhs
[generate-all]: https://github.com/derekwisong/datui/tree/main/scripts/demos/generate-all.py
[demo-tapes]: https://github.com/derekwisong/datui/tree/main/scripts/demos
[demos]: https://github.com/derekwisong/datui/tree/main/demos

