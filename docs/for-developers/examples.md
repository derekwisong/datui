# Check the examples

The numbers the guides quote from the **Example datasets** that come with datui are checked
by one script; that every command and query runs is
[the doc-example runner's](documentation.md#code-blocks) job.

```bash,repo
.venv/bin/python scripts/docs/check_examples.py
```

Run it before a release, before recording demos, and after a change to
anything an example touches.

The script reruns each documented query on the frozen datasets through Polars
SQL and compares the numbers with the text. It downloads about 140 MB on the
first run and keeps the files in `~/.cache/datui-doc-examples` (`--cache` moves
it; `-k food` runs one check). It needs the network, so CI does not run it.

| Check | Dataset | Numbers it holds the docs to |
|---|---|---|
| `penguins-species`, `penguins-counts` | Palmer penguins | Species means and counts, r = 0.871 over 342 pairs |
| `flights-jfk`, `flights-carriers`, `flights-days` | NYC flights (2013) | JFK delay by hour, carrier ranking and AS's route, 365 days and the worst one, `jfk sea` |
| `food` | Food nutrition | Restaurant summary, `chicken` and `chkn` counts, the 30 heavy chicken items, missing vitamins |
| `names` | US baby names | The three-name pivot, its nulls and Jennifer's peak, name totals |
| `football` | Premier League (2020-21) | The goals query's first rows, the 12 postponed dates |
| `launches` | Space launches | The count pivot |
| `taxis` | NYC yellow taxis (January 2025) | Trips by pickup hour |

## By hand

The runner opens what each command names and runs each query; what a key does
on screen, and the numbers a sampled analysis or a remote dataset shows, still
need a person. Build a release binary, run it with a throwaway cache and config
(`DATUI_CACHE_DIR`, `DATUI_CONFIG_DIR`), and open each dataset from the home
screen.

| Page | Do | Expect |
|---|---|---|
| [Quick start](../getting-started/quick-start.md) | Every step | The numbers in the page; Gentoo drills to 124 rows |
| [Query data](../user-guide/querying-data.md) | Each query, the drill-downs, the q table | Results as written; AS drills to 714 flights, taxi hour 4 to 20,033 |
| [Sort, filter and arrange columns](../user-guide/filtering-sorting.md) | The five steps | `30 of 515`, 2,430 calories first, `vit_a` and `vit_c` hidden |
| [Pivot and melt](../user-guide/reshaping.md) | Names pivot, melt, launches count | 138, 414 and 62 rows |
| [Make a chart](../user-guide/charting.md) | Every row of the examples table | The shapes and labels described |
| [Analysis](../user-guide/analysis-features.md) | Taxis with seed `1`; penguins without `rownames` | Describe and Distribution values; r = 0.871 |
| [Check data quality](../user-guide/data-quality.md) | Food; taxis with seed `1` | The findings table; 16,000 rows missing together |
| [Copy](../user-guide/copying.md) | The Markdown copy, under tmux with `set-clipboard on` and `[clipboard] backend = "osc52"` | `tmux show-buffer` prints the table in the page |
| [Export](../user-guide/exporting-data.md) | `goals.csv` | 381 lines, the three shown first |
| [Views](../user-guide/views.md) | Save on 2024, apply on 2023; `--view` on 2022 | 366, 365 and 365 rows |
| [Remote data](../user-guide/remote-data.md) | NOAA 2024, its element counts, Bitcoin 2024 | 38,465,374 rows; `PRCP` first; 12 months |
| [Python](../user-guide/python-module.md) | The capture example, with the wheel built as in [Build Python bindings](python-bindings.md) | The three-row summary |

Earthquakes change daily, and Bitcoin gains a partition a day: their pages
quote no counts. A number that no longer matches is a docs fix or a datui bug;
file the bug with the dataset and the steps.
