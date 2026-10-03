# Pipes and growing files

## Standard input

`datui -` reads the data piped to it, and so does `datui` with no path when
something is piped in. Keys still come from the terminal.

```bash
xsv select id,amount sales.csv | datui
curl -s https://example.com/export.csv.gz | datui
datui --no-header - < raw.txt
```

The data is written to a temporary file as it arrives, in `--temp-dir` or the
`temp_dir` setting when given, then read like any file. The loading screen counts the bytes read;
<kbd>Ctrl</kbd>+<kbd>O</kbd> stops the read and removes the file. The file is
removed when datui exits.

The format comes from the first bytes, unless `--format` or `--compression`
names it: see [Formats](../formats/index.md#detected-by-content).

The CSV options below apply. The dataset is named `stdin`. It is not added to
recent datasets, and [views](views.md) match it by its columns only.

## Following a growing file

`--follow` (`-f`) shows rows as they are appended to a local CSV, TSV, PSV,
NDJSON or text file, or record batches to an Arrow IPC stream, as `tail -f` does:
a logger's output, a test rig's results, an app's event log. With `-`, it shows
standard input as it arrives rather than waiting for it to end. Text is a row per
line, blank lines included.

```bash
datui -f readings.csv
datui -f /var/log/app.log
cat /dev/ttyUSB0 | datui -f -
while sleep 0.1; do echo "$(date +%s.%N),$RANDOM"; done | datui -f --no-header -
```

| Key | Action |
|---|---|
| <kbd>t</kbd> | Pause and resume. On a file opened without `-f`, follow it: it is read again, as <kbd>H</kbd> reads it, so the query, filters and sort are cleared |
| <kbd>Esc</kbd> | Stop following; the rows read stay |

The control bar says `following` and how long ago rows last arrived, or
`paused` and how many have arrived since.

- A follow starts on the last row. On the last row, the cursor stays on the
  last row as rows arrive; anywhere else it stays where you put it, and the
  bar counts the rows that came in below.
- Only complete lines are read. A partial last line waits for its newline.
- The query, filters, sort, hidden columns and column cursor apply to the new
  rows. [Value counts](value-counts.md), [analysis](analysis-features.md) and
  [charts](charting.md) keep the rows they were opened on; the bar says how many
  have arrived since, and <kbd>t</kbd> there reads them.
- The column types come from the first rows, as for any file. A later row
  that does not fit (a field too many, a word in a number column) is counted on
  the bar in the warning color and its values are read as null; it never stops
  the follow.
- A refresh reads only the rows on screen, from a mark near them, so it costs
  the same on a 10 MB file as on a 10 GB one. Under a filter, only the new rows
  are counted.
- A file that shrinks or is replaced (truncation, rotation) is read again from
  its start, and the bar says so. A deleted file stops the follow; its rows
  stay readable.
- On Linux, datui hears of an append as it lands and reads new rows at most
  every 250ms; elsewhere, and on a network file system, it checks the file's
  size every 250ms. A burst of appends is one refresh. Set `follow_interval`
  under [`[read]`](../reference/settings.md#read) to change it, as in
  `-c read.follow_interval=1s`.
- Standard input keeps being written to its temporary file after the first
  rows show, until it ends or <kbd>Esc</kbd>, <kbd>Ctrl</kbd>+<kbd>O</kbd> or
  quitting stops it. The bar says when it ends.

An Arrow IPC stream shows a record batch once its message is whole; one with
dictionary-encoded columns cannot be followed. Parquet, Arrow IPC files, Excel
and other formats written with a footer cannot be read before they are
finished, nor can a compressed file be read from the middle: `--follow`
refuses them, and remote data, with a message.

### Recording standard input

`--tee FILE` records standard input to FILE while you view it: the bytes
exactly as they arrive, whatever the format. The table reads FILE itself; there is
no second copy.

```bash
some_logger | datui -f --tee run1.csv -
arecord -f S16_LE -r 48000 -c 2 -t wav - | datui -f --tee take1.wav -
```

| When | What happens |
|---|---|
| The bar | `rec` with the bytes written and the rate; `saved` with the size, length and file once the stream ends; `stopped` and why, in the warning color, if writing failed (a full disk), which the error dialog says too |
| An existing FILE | Refused; `--force` overwrites it |
| Quit, <kbd>Ctrl</kbd>+<kbd>O</kbd> while the producer still sends | Asks: Stop recording, or Keep recording until the stream ends (after a quit, datui waits for it with the terminal handed back). <kbd>Esc</kbd> stays |
| <kbd>Esc</kbd> at the table | Stops following; the recording goes on |
| SIGTERM, SIGHUP | The file is finished and closed, and datui exits |
| WAV | A producer writing to a pipe cannot fill in the RIFF and `data` sizes; they are filled in when the stream ends, as RF64 over 4 GB when the producer reserved a `JUNK` chunk for it. `--tee-raw` leaves FILE exactly as it came |

`--tee -` passes the stream on to standard output instead, as `tee` does, and
draws on the terminal (`/dev/tty`, or the console on Windows): datui sits in
the middle of a pipeline and shows what goes through it. The table reads a
temporary copy in `--temp-dir`. Standard output has to go to a pipe or a file;
the bar says `sent` once the stream ends, and a reader downstream that stops
reading stops the copy, saying so.

```bash
some_logger | datui -f --tee - | gzip > run1.csv.gz
```

Without `-f`, the whole stream is recorded before the table opens. With
`-f`, a format that cannot be followed (a WAV file) shows what had arrived
when it opened, and the recording goes on. The copy holds one megabyte however
long the stream runs; a producer faster than the disk waits on the pipe.
FILE is never removed, whatever happens.
