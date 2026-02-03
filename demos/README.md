# Demo GIFs

Animated GIF demos are generated with [VHS](https://github.com/charmbracelet/vhs) from `.tape` scripts in `scripts/demos/`. Run from the repository root:

```bash
python3 scripts/demos/generate_demos.py    # all demos (use --number N for one, -n N for workers)
vhs scripts/demos/01-basic-navigation.tape  # single demo
```

For prerequisites, options, and detailed instructions, see the [Demos documentation][demos-docs].

[demos-docs]: https://derekwisong.github.io/datui/latest/for-developers/demos.html
