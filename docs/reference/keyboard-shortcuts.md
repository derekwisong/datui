# Keyboard Shortcuts

In the main view, the following keyboard shortcuts are available.

**Busy state:** When the app is working (loading data, scrolling, exporting, analysis, pivot/melt), a throbber appears in the control bar.

**Navigation (main table):**

| Key | Action |
|-----|--------|
| `↑` / `↓` or `j` / `k` | Move selection one row |
| `←` / `→` or `h` / `l` | Scroll columns |
| `Home` | Jump to first row |
| `End` or `G` | Jump to last row |
| `Page Up` / `Page Down` | Scroll one page |
| `Ctrl-F` / `Ctrl-B` | Page down / page up |
| `Ctrl-D` / `Ctrl-U` | Half page down / half page up |
| `:` | Go to line: type a line number and press Enter (e.g. `:0` Enter for first row); Esc to cancel |

**Actions:**

| Key | Action |
|-----|--------|
| `/` | Query input (See [Querying Data](../user-guide/querying-data.md)) |
| `p` | Open **Pivot & Melt** controls (See [Pivot and Melt](../user-guide/reshaping.md)) |
| `s` | Open **Sort & Filter** controls (See [Sorting and Filtering](../user-guide/filtering-sorting.md)) |
| `e` | Open export controls (See [Exporting Data](../user-guide/exporting-data.md)) |
| `a` | Open the analysis tools (See [Analysis Features](../user-guide/analysis-features.md)) |
| `c` | Open **Chart** view (See [Charting](../user-guide/charting.md)) |
| `t` | Open template manager (See [Templates](../user-guide/templates.md)) |
| `T` | Apply most relevant template |
| `i` | Open **Info** panel (modal); `Tab` / `Shift+Tab` move focus (tab bar ↔ schema table); `Left` / `Right` switch tabs (See [Dataset Info](../user-guide/dataset-info.md)) |
| `r` | Reset (clear query, filters, sort) |
| `q` | Quit |
| `?` / `F1` | Help (F1 works in text fields, e.g. query input) |

**Note for Alacritty users:** If F1 does nothing, ensure F1 is not bound in `~/.config/alacritty/alacritty.toml`. You can still use `?` for help when not in a text field.
