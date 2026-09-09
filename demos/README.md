# Demo GIFs

Animated GIF demos are generated with [VHS](https://github.com/charmbracelet/vhs) from `.tape` scripts in `scripts/demos/`. Run from the repository root:

```bash
python3 scripts/demos/generate_demos.py    # all demos (use --number N for one, -n N for workers)
vhs scripts/demos/01-basic-navigation.tape  # single demo
```

`12-home-screen.tape` builds a throwaway workspace, cache and config under
`/tmp/datui-demo` before recording, so the home screen shows generated data rather than
whatever the machine running it happens to have open.

For prerequisites, options, and detailed instructions, see the [Demos documentation][demos-docs].

[demos-docs]: https://derekwisong.github.io/datui/latest/for-developers/demos.html
