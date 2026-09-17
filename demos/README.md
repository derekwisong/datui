# Demo GIFs

Animated GIF demos are generated with [VHS](https://github.com/charmbracelet/vhs) from `.tape` scripts in `scripts/demos/`. Run from the repository root:

```bash
python3 scripts/demos/generate_demos.py    # all demos (use --number N for one, -n N for workers)
vhs scripts/demos/01-basic-navigation.tape  # single demo
```

Most tapes open datasets under `demo/data`, built by `demo/build.py`. The home-screen
tapes (`12` through `16`) copy that directory into a throwaway workspace under
`/tmp/datui-demo`, with their own home directory, cache and config. The generator also
removes inherited cloud credentials, so those recordings show seeded local data and
the built-in public catalog rather than anything belonging to the machine recording
them. It also pins true-color output and ignores an inherited `NO_COLOR` setting.

For prerequisites, options, and detailed instructions, see the [Demos documentation][demos-docs].

[demos-docs]: https://derekwisong.github.io/datui/latest/for-developers/demos.html
