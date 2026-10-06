# Captures

The GIFs, screenshots and theme gallery the docs and the landing page embed.
`scripts/demos/capture.py` records them with [VHS](https://github.com/charmbracelet/vhs)
from the tapes in `scripts/demos/`, each in an isolated fixture, from the
built-in catalog's public data:

```bash,repo
python3 scripts/demos/capture.py --list
```

`--publish` copies reviewed outputs here. See
[Record demos](https://derekwisong.github.io/datui/latest/for-developers/demos.html).
