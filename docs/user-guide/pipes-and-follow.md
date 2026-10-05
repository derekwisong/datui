# Pipes and growing files

datui reads data piped to it, shows a file or a pipe as it grows, and records
a stream while you view it.

| Command | Does |
|---|---|
| `COMMAND \| datui` | Shows what is piped in as it arrives; `datui -` does the same |
| `datui -f FILE` | Follows a file as it grows, as `tail -f` does |
| `COMMAND \| datui -f -` | Shows the rows of a pipe as they arrive, staying on the last row |
| `COMMAND \| datui --tee FILE -` | Records the stream to FILE while you view it |
| `COMMAND \| datui --tee - - \| COMMAND` | Passes the stream on to standard output, viewing it on the way |

## Standard input

`datui -` reads the data piped to it, and so does `datui` with no path when
something is piped in. Keys still come from the terminal.

```bash
printf 'id,amount\n1,9.50\n2,3.25\n' | datui
journalctl -o json -n 500 | datui
printf 'id,amount\n1,9.50\n' > sales.csv && datui - < sales.csv
(echo 1,2; echo 3,4) | datui --no-header -
```

- The first rows show once a thousand lines have arrived, or fewer when the
  producer is slower than that, and datui reads on to the end of the stream.
  The view stays where you put it; the footer says `reading stdin` and the
  bytes so far, and the row count reads `1,234+` until the stream ends.
  [Value counts](value-counts.md), [analysis](analysis-features.md) and
  [charts](charting.md) opened meanwhile keep the rows they opened on, and
  <kbd>t</kbd> there reads the new ones, as for `--follow`.
- What cannot be read before it is finished (Parquet, an Arrow IPC file,
  Excel, a compressed stream) is read to the end first; the loading screen
  counts the bytes.
- NDJSON and [journal JSON](../formats/signals-and-logs.md#systemd-journal)
  show the entries whose lines have ended; an object still being written
  waits for its newline. The columns are the fields of the lines that had
  arrived when the table opened; a field first seen later joins as a column at
  the right once the stream ends, the query, filters and sort kept, so the
  final columns cover every line.
- The data is written to a temporary file in `spool` under the cache
  directory as it arrives, not the system temp directory (memory on many
  Linux systems); `--temp-dir` puts it elsewhere. The file is removed when the
  dataset goes; one left by a crash is removed after a week.
  <kbd>Ctrl</kbd>+<kbd>O</kbd> stops the read and removes the file.
- The format comes from the first bytes, unless `--format` or `--compression`
  names it: see [Detected by content](../formats/index.md#detected-by-content).
- The [delimited text](../formats/delimited-text.md) flags apply.
- The dataset is named `stdin`. It is not added to recents, and
  [views](views.md) match it by its columns only.

## Following a growing file

`--follow` (`-f`) shows rows as they are appended: to a local CSV, TSV, PSV,
NDJSON or text file, or record batches to an Arrow IPC stream. With `-`, it
shows standard input as it arrives instead of waiting for it to end.

```bash
printf 'time,value\n1,10\n2,20\n' > readings.csv && datui -f readings.csv
```

```bash,interactive
(echo time,value; while sleep 0.2; do echo "$(date +%s),$RANDOM"; done) | datui -f -
```

| Key | Action |
|---|---|
| <kbd>t</kbd> | Pause and resume. On a file opened without `-f`, follow it: the file is read again, as <kbd>H</kbd> on the Info panel's Schema tab reads it, so the query, filters and sort are cleared |
| <kbd>Esc</kbd> | Stop following; the rows read stay |

The footer says `following` and how long ago rows last arrived, or `paused`
and how many have arrived since, and offers <kbd>t</kbd> and <kbd>Esc</kbd>.

| What | How it behaves |
|---|---|
| The cursor | A follow starts on the last row and stays there as rows arrive. Moved elsewhere, it stays put, and the bar counts the rows that came in below |
| A partial last line | Waits for its newline. Once standard input ends, a last line with no newline is a row |
| The query, filters, sort, hidden columns | Apply to new rows. [Value counts](value-counts.md), [analysis](analysis-features.md) and [charts](charting.md) keep the rows they opened on; the bar counts the new ones, and <kbd>t</kbd> there reads them |
| A row that does not fit the types of the first rows | Counted on the bar in the warning color; its values read as null. The follow goes on |
| An NDJSON field the first rows did not have | In a file, the row is counted as not fitting. From standard input, the field joins as a column when the stream ends |
| A refresh | Reads only the rows on screen, from a mark near them: a 10 GB file costs what a 10 MB one does. Under a filter, only the new rows are counted |
| Truncation, rotation | The file is read again from its start, and the bar says so. A deleted file stops the follow; its rows stay |
| How often | On Linux, as an append lands, at most every 250ms; elsewhere and on network file systems, the size is checked every 250ms. `read.follow_interval` changes it: `-c read.follow_interval=1s` |
| Standard input | Written to its temporary file until it ends, or <kbd>Esc</kbd>, <kbd>Ctrl</kbd>+<kbd>O</kbd> or quitting stops it. The bar says when it ends. Without `-f` it is read the same way, but the cursor stays where it is |

An Arrow IPC stream shows a record batch once the batch is whole; one with
dictionary-encoded columns cannot be followed. `--follow` refuses Parquet,
Arrow IPC files, Excel and the other formats written with a footer, compressed
files, and remote data, with a message.

## Recording standard input

`--tee FILE` records standard input to FILE while you view it: the bytes as they
came, except a WAV stream's sizes, filled in when it ends (below).
The table reads FILE itself; there is no second copy.

```bash
(echo n; seq 1 1000) | datui --tee numbers.csv -
```

```bash,interactive
(echo time,value; while sleep 0.2; do echo "$(date +%s),$RANDOM"; done) | datui -f --tee run1.csv -
```

| When | What happens |
|---|---|
| While it records | The bar shows `rec`, the bytes written and the rate; then `saved` with the size, length and file once the stream ends |
| Writing fails (a full disk) | The bar shows `stopped` and why, in the warning color, and the error dialog says so |
| FILE exists | Refused; `--force` replaces it |
| Quit or <kbd>Ctrl</kbd>+<kbd>O</kbd> while the producer still sends | Asks: stop recording, or keep recording until the stream ends (after a quit, datui waits with the terminal handed back). <kbd>Esc</kbd> stays |
| <kbd>Esc</kbd> at the table | Stops following; the recording goes on |
| SIGTERM, SIGHUP | FILE is finished and closed, and datui exits |
| A WAV stream | Its RIFF and `data` sizes are filled in when the stream ends, as RF64 past 4 GB when the producer left a `JUNK` chunk for it. `--tee-raw` leaves FILE exactly as it came |

Without `-f`, the whole stream is recorded before the table opens. With `-f`,
a format that cannot be followed (WAV) shows what had arrived when it opened,
and the recording goes on. FILE is never removed.

`--tee -` passes the stream on to standard output instead, as `tee` does, and
draws on the terminal (`/dev/tty`, or the console on Windows). Standard output
must go to a pipe or a file. The table reads a temporary copy in
`--temp-dir`; the bar says `sent` once the stream ends, and a reader
downstream that stops reading stops the copy, saying so.

```bash
(echo n; seq 1 1000) | datui --tee - - | gzip > numbers.csv.gz
```

To record a serial device or a sound card, replace `<DEVICE>` with yours:

```bash,template
cat <DEVICE> | datui -f --tee capture.log -
arecord -D <DEVICE> -f S16_LE -r 48000 -c 2 -t wav - | datui -f --tee take1.wav -
```
