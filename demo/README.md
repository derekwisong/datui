# Demo data

`demo/data` holds the datasets the demo GIFs open: real, small, and interesting to
look at. `demo/build.py` builds the public ones from their sources; see
[DATA-LICENSES.md](DATA-LICENSES.md) for where each came from and what changed.

```bash
python demo/build.py                         # the public datasets
python demo/build.py --private /path/to/snapshots   # plus the snapshots
```

The tapes in `scripts/demos` open these files by relative path, and the
home-screen fixture copies the whole directory into its workspace.
