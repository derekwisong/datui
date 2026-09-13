# Demo GIFs

Animated GIF demos are generated with [VHS](https://github.com/charmbracelet/vhs) from `.tape` scripts in `scripts/demos/`. Run from the repository root:

```bash
python3 scripts/demos/generate_demos.py    # all demos (use --number N for one, -n N for workers)
vhs scripts/demos/01-basic-navigation.tape  # single demo
```

The tapes open the datasets under `demo/data`, built by `demo/build.py`.
`12-home-screen.tape` and `13-light-theme.tape` copy that directory into a throwaway
workspace under `/tmp/datui-demo`, with their own cache and config, so the home screen
shows the demo data rather than whatever the machine running it happens to have open.

For prerequisites, options, and detailed instructions, see the [Demos documentation][demos-docs].

[demos-docs]: https://derekwisong.github.io/datui/latest/for-developers/demos.html
