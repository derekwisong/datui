# Troubleshooting

What to do when datui does not do what you expect. Each row links to the page
that explains it.

## Terminal

| Problem | What to do |
|---|---|
| <kbd>F1</kbd> does nothing in Alacritty | Alacritty binds it in `~/.config/alacritty/alacritty.toml`; unbind it there. <kbd>?</kbd> opens help outside text fields |
| The mouse cannot select text | <kbd>Shift</kbd>+drag (<kbd>Option</kbd>+drag in iTerm2), or leave the mouse to the terminal with `--mouse=false`: [Mouse and text selection](../user-guide/configuration.md#mouse-and-text-selection) |
| Boxes, arrows and rails show as `?` or garbage | The terminal does not take UTF-8, or its font lacks the glyphs: [Glyphs or ASCII](../user-guide/configuration.md#glyphs-or-ascii) |
| Header and row stripes near-black on a light background | Set `theme.mode = "light"`: [Light and dark](../user-guide/configuration.md#light-and-dark) |
| Everything monochrome, or colors off | `NO_COLOR` is set, or the terminal does not take 24-bit color: [Configure datui](../user-guide/configuration.md#troubleshooting) |
| Lines left behind or out of place after scrolling | Switch to another window and back, or resize, to repaint; `-c display.scroll_region=false` redraws every line that moved: [Configure datui](../user-guide/configuration.md#troubleshooting) |
| Header or chips garbled in VS Code's terminal | [Configure datui](../user-guide/configuration.md#troubleshooting) |

## Opening data

| Problem | What to do |
|---|---|
| datui asks before reading a file into memory | JSON, Avro, ORC, Excel and some others are read whole, and datui asks first when that would pass `read.memory_warning` (1 GiB). Parquet, CSV and Arrow IPC are read lazily. [How each format is read](../formats/index.md#how-each-format-is-read) covers every format |
| datui asks before downloading | A web file, or a bucket object in a format that cannot be read in place, is copied to the temp directory first: [How each format is read](../formats/index.md#how-each-format-is-read) |
| A sort, query or analysis is slow on a large file | It reads every row the operation needs, not just the screen: [Large datasets](../user-guide/large-datasets.md) |
| A CSV's first row is data but is read as the header, or the other way around | `--no-header`, or <kbd>H</kbd> on the Info panel's Schema tab: [Delimited text](../formats/delimited-text.md) |
| A file has no extension, or the wrong one | `--format NAME`; `datui --help` lists the names: [Formats](../formats/index.md) |
| A file datui cannot read opens as bytes | No reader or format spec recognizes it: [Hex view](../user-guide/hex-view.md), and [Format specs](../formats/format-specs.md) to describe it |

## Config and cache

| Problem | What to do |
|---|---|
| datui stops on a config error | Fix the line it names, or move the file aside to start from the defaults; `datui config init` then writes a fresh one: [Configure datui](../user-guide/configuration.md#troubleshooting) |
| Odd home-screen state: recents, folded sections, a hidden source | `datui cache clear` resets the cache, never your data or config: [Configure datui](../user-guide/configuration.md#troubleshooting) |

## Cloud logins

A cloud source's row on the home screen says why it lists nothing; its details
pane gives the whole message. [Cloud sources](../user-guide/home-screen.md#cloud-sources)
lists every row status.

| Row or error | What to do |
|---|---|
| `403` | The login cannot list buckets. Open an object by its URL: `datui s3://<BUCKET>/<KEY>` |
| `not logged in` | No usable credentials reached the store: [Connect to cloud storage](../user-guide/remote-data.md) |
| `no project` | Set `GOOGLE_CLOUD_PROJECT` or `DATUI_GCP_PROJECT`: [Google Cloud Storage](../user-guide/remote-data.md#google-cloud-storage) |
| `needs gcloud`, `needs the AWS CLI` | The login goes through that tool, which is not installed |
| `not signed in` | Run `az login` or `Connect-AzAccount`: [Azure Blob Storage](../user-guide/remote-data.md#azure-blob-storage) |
| `not configured` | A variable a `[[cloud.connections]]` entry names is not set: [Cloud connections](cloud-sources.md) |
| An expired AWS SSO login | `aws sso login --profile <PROFILE>`: [AWS profiles](../user-guide/remote-data.md#aws-profiles) |

## Windows

| Problem | What to do |
|---|---|
| Another program cannot replace a file datui has open | datui reads files through memory maps, which Windows will not let another program truncate, rename or delete. Close the dataset first: [Windows](../getting-started/installation.md#windows) |
| 16 colors only | "Use legacy console" is checked in the console's properties: [Windows](../getting-started/installation.md#windows) |
| Temporary files left behind | A file still mapped cannot be removed; datui tries again as it quits: [Temporary files](../user-guide/open-files.md#temporary-files) |

## Report a problem

Errors, warnings and backtraces go to `datui.log` in the cache directory:
[The log](../user-guide/configuration.md#the-log). File an issue at
<https://github.com/derekwisong/datui/issues> with the log, the command and,
for public data, the dataset.
