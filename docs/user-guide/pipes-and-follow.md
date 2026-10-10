# Pipes and growing files

datui reads data piped to it, shows a file or a pipe as it grows, and records
a stream while you view it.

| Command | Does |
|---|---|
| `COMMAND \| datui` | Shows what is piped in as it arrives; `datui -` does the same |
| `datui -f FILE` | Follows a file as it grows, as `tail -f` does |
| `COMMAND \| datui -f -` | Shows the rows of a pipe as they arrive, staying on the last row |
| `COMMAND \| datui --tee FILE -` | Records the stream to FILE while you view it |
| `COMMAND \| datui --tee - - \| COMMAND` | Passes the stream on to standard output while you view it |

## Standard input

`datui -` reads the data piped to it, and so does `datui` with no path when
something is piped in. Keys still come from the terminal.

```bash
printf 'id,amount\n1,9.50\n2,3.25\n' | datui
journalctl -o json -n 500 | datui
printf 'id,amount\n1,9.50\n' > sales.csv && datui - < sales.csv
(echo 1,2; echo 3,4) | datui --no-header -
```

- The first rows show once a thousand lines have arrived, or sooner with fewer
  lines when the producer is slow, and datui keeps reading to the end of the
  stream.
  The view stays where you put it; the footer says `reading stdin` and the
  bytes so far, and the row count reads `1,234+` until the stream ends.
  [Value counts](value-counts.md), [analysis](analysis-features.md) and
  [charts](charting.md) opened meanwhile keep the rows they opened on;
  <kbd>t</kbd> in them reads the new ones, as with `--follow`.
- What cannot be read before it is finished (Parquet, an Arrow IPC file,
  Excel, a compressed stream) is read to the end first; the loading screen
  counts the bytes.
- NDJSON and [journal JSON](../formats/signals-and-logs.md#systemd-journal)
  show only complete lines; an object still being written waits for its
  newline. The journal's columns are the fields of every line that had arrived
  when the table opened; NDJSON's columns come from its first 100 lines. A
  field first seen later is added as a column at the right once the stream
  ends, so the final columns cover every line. Filters and the sort stay.
  Under a query, a reshape, a group or a drill-down, the new column appears
  once that is cleared.
- As it arrives, the data is written to a temporary file in `spool` under the
  cache directory, not in the system temp directory (which is memory on many
  Linux systems); `--temp-dir` puts it elsewhere. The file is removed when the
  dataset is closed; one left by a crash is removed after a week.
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
| <kbd>t</kbd> | Pause and resume. On a file opened without `-f`, start following it: the file is read again, as <kbd>H</kbd> on the Info panel's Schema tab does, so the query, filters and sort are cleared |
| <kbd>Esc</kbd> | Stop following; the rows read stay |

The footer says `following` and how long ago rows last arrived, or `paused`
and how many have arrived since, and offers <kbd>t</kbd> and <kbd>Esc</kbd>.

| What | How it behaves |
|---|---|
| The cursor | A follow starts on the last row and stays there as rows arrive. If you move it elsewhere, it stays there, and the footer counts the rows that arrive below |
| A partial last line | Waits for its newline. Once standard input ends, a last line with no newline is a row |
| The query, filters, sort, hidden columns | Apply to new rows. [Value counts](value-counts.md), [analysis](analysis-features.md) and [charts](charting.md) keep the rows they opened on; the footer counts the new ones, and <kbd>t</kbd> in them reads them |
| A row that does not fit the types of the first rows | Counted in the footer in the warning color; its values read as null. The follow goes on |
| An NDJSON field the first rows did not have | In a file, the row is counted as not fitting. From standard input, the field joins as a column when the stream ends |
| A refresh | Reads only the rows on screen, from a mark near them: a 10 GiB file costs what a 10 MiB one does. Under a filter, only the new rows are counted |
| Truncation, rotation | The file is read again from its start, and the footer says so. A deleted file stops the follow; its rows stay |
| How often | On Linux, when an append lands, at most every 250ms; elsewhere and on network file systems, the size is checked every 250ms. `read.follow_interval` changes it: `-c read.follow_interval=1s` |
| Standard input | Written to its temporary file until it ends, or until <kbd>Esc</kbd>, <kbd>Ctrl</kbd>+<kbd>O</kbd> or quitting stops it. The footer says when it ends. Without `-f` it is read the same way, but the cursor stays where it is |

An Arrow IPC stream shows a record batch once the batch is whole; one with
dictionary-encoded columns cannot be followed. `--follow` refuses Parquet,
Arrow IPC files, Excel and the other formats written with a footer, compressed
files, and remote data, with a message.

## Recording standard input

`--tee FILE` records standard input to FILE while you view it. FILE gets the
bytes exactly as they arrived, except that a WAV stream's sizes are filled in
when it ends (see below). The table reads FILE itself, so there is no second
copy.

```bash
(echo n; seq 1 1000) | datui --tee numbers.csv -
```

```bash,interactive
(echo time,value; while sleep 0.2; do echo "$(date +%s),$RANDOM"; done) | datui -f --tee run1.csv -
```

| When | What happens |
|---|---|
| While it records | The footer shows `rec`, the bytes written and the rate; then `saved` with the size, length and file once the stream ends |
| Writing fails (a full disk) | The footer shows `stopped` and why, in the warning color, and the error dialog says so |
| FILE exists | Refused; `--force` replaces it |
| Quit or <kbd>Ctrl</kbd>+<kbd>O</kbd> while the producer still sends | Asks: stop recording, or keep recording until the stream ends (after a quit, datui hands the terminal back and waits). <kbd>Esc</kbd> stays in datui |
| <kbd>Esc</kbd> at the table | Stops following; the recording goes on |
| SIGTERM, SIGHUP | FILE is finished and closed, and datui exits |
| A WAV stream | Its RIFF and `data` sizes are filled in when the stream ends, as RF64 past 4 GiB when the producer left a `JUNK` chunk for it. `--tee-raw` leaves FILE exactly as it came |

Without `-f`, the whole stream is recorded before the table opens. With `-f`,
a format that cannot be followed (WAV) shows what had arrived when it opened,
and the recording goes on. FILE is never removed.

`--tee -` passes the stream on to standard output instead, as `tee` does, and
draws on the terminal (`/dev/tty`, or the console on Windows). Standard output
must go to a pipe or a file. The table reads a temporary copy in
`--temp-dir`. The footer says `sent` once the stream ends. If the reader
downstream stops reading, the copy stops too, and datui says so.

```bash
(echo n; seq 1 1000) | datui --tee - - | gzip > numbers.csv.gz
```

To record a serial device or a sound card, replace `<DEVICE>` with yours:

```bash,template
cat <DEVICE> | datui -f --tee capture.log -
arecord -D <DEVICE> -f S16_LE -r 48000 -c 2 -t wav - | datui -f --tee take1.wav -
```
