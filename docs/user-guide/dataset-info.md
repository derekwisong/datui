# Dataset Info

Press <kbd>i</kbd> for the schema and the facts about the file. <kbd>i</kbd>
or <kbd>Esc</kbd> closes it.

![Info Panel Demo](../demos/03-info.gif)

| Tab | Shows |
|---|---|
| **Schema** | Total rows and columns, columns by type, whether the schema is stored (Parquet) or inferred (CSV, JSON), and every column with its type and, for Parquet, its codec and compression ratio |
| **Resources** | File size, the memory used by the buffered rows, the format, for Parquet the overall compression ratio, row groups, version and writer, and what opening the dataset cost |
| **Partitions** | For a hive-partitioned dataset, its partition columns |
| **Notes** | What datui noticed about the data while reading it. Only there when something is worth saying |

## Keys

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> | Switch tab |
| <kbd>Tab</kbd> | On the Schema tab, move focus between the tab bar and the column table |
| <kbd>↑</kbd> <kbd>↓</kbd> | Scroll the column table, or move through the notes |
| <kbd>?</kbd> | Help |
| <kbd>Esc</kbd> <kbd>i</kbd> | Close |

The row count is for the whole dataset, not the rows on screen. The type of
each column is also shown in the table's second header row, which <kbd>D</kbd>
toggles.

## Notes

A directory of Parquet files rarely holds files that agree, and rarely holds
only files with something in them. Where datui notices either, it says so:

| Note | From |
|---|---|
| A column is not in every file, and which partitions have it | The footers and the file names |
| Files disagree on a column's type, beyond what widening settles | The footers |
| A column is stored as more than one type the scan can read into one | The footers |
| A column is read as text, so it compares as text | The footers |
| A file holds no rows at all | The footers |
| Row groups are large enough that a page of rows costs much more than a page | The footers |
| There are very many files and the middle one holds very little | The listing and the footers |
| The directories do not all partition by the same keys | The file names |
| A file's footer could not be read, so it was left out | The footers |
| Files in the directory are not Parquet, so they are not in the table | The listing |
| The directory held more than one format and was read as the commonest; what was passed over | The listing |
| These are a Delta, Iceberg or Hudi table's plain files, not the table | The directory's markers |
| A filter or sort leaves rows out, because their files hold its column in another type | The footers |

The last two are notes about the read rather than about the data, and they are
the only ones settled before a footer is read. A lake table's files carry a chip
beside the row count as well: that count includes rows a delete tombstoned and
versions an update replaced, so it is a true count of the files and a wrong
count of the table.

Where the files that have a column fall into a shape worth naming, the note says so
as well as counting them: `fee is in 4,001 of 6,541 files, none before
date=2010-07-18` is a field a feed started sending, and `oops is in 1 of 6,541
files, only date=2024-03-02` is one directory's mistake. Only those two shapes —
a column scattered across a dataset is just a count. `none before` also needs the
directories to sort the way you would read them: where partition values are not
zero-padded, `part=10` comes before `part=2` in the listing and there is no
honest way to say where a column starts, so nothing is said. A dataset whose footers were
sampled gets no such phrase: a file whose footer was not read looks like a file
missing nothing, and a range drawn over those would be a guess.

Every note says what it is based on — `in all 6,541 footers`, or `in 20,000 of
200,000 footers (sample)` — so a count never stands for files datui has not
looked at. A cloud directory that is still reading its footers behind the data
says `in 2 of 6,541 footers (sample)` until they land, and the notes are
rewritten from all of them when they do. None of them costs a read of its own:
they come from the footers the schema and the row count already needed.

A note is one sentence and the line beneath it saying what it is based on;
<kbd>↑</kbd> and <kbd>↓</kbd> move between them. When the list is taller than
the panel, the corner says how many notes are out of view.

A note about a column the files store in more than one type offers to **read it
as text**: press <kbd>Enter</kbd> on it and the column is read from the files
that disagree too, at the type each of them wrote, so the values the conflict hid
appear. While the cursor is on such a note the panel's last row says so. A filter
or sort on the column then compares text, and a note stays to say it. A column
any file holds as a list, a duration or binary cannot be shown as text at all,
and no offer is made for it.

Every note but one kind describes the dataset as opened. The exception is the
note about a filter or sort leaving rows out, which describes the view: one
arrives for each column you sort or filter by that some file holds in another
type, and goes when you clear it. See [datasets whose files differ](loading-data.md#files-that-disagree).

A query, a pivot or a drill-down builds rows of its own, so the notes step
aside; resetting or drilling back up brings them back.

When there is something to note, the <kbd>i</kbd> key in the control bar takes
a quiet accent until you open the panel. `notes_accent = false` in the
`[display]` section of the [config](configuration.md) turns that off; the Notes
tab is still there either way.

## Measurements

At the foot of the **Resources** tab, what this dataset has cost: finding it,
reading its footers, and fetching the page on screen.

Listing and Footers appear for the datasets datui finds and reads itself: a
directory of Parquet opened with `--hive`, a remote prefix, a remote glob, and
a single remote object. Anything else — a single local file, a CSV, a local
directory opened without `--hive`, a directory or prefix opened with
`single_spine_schema = false` — is handed straight to Polars, which does not
report what it did, so neither row appears and there is no Total. A glob is
listed and matched by datui rather than by the object store, so its Listing
row reports the files the pattern matched, out of everything under the literal
part of the key.

Last page appears whichever route opened the dataset, because datui asks for
the rows on screen and times the answer either way; a dataset already known to
be empty has no page to fetch, so nothing is reported.

| Metric | Formula |
|---|---|
| Listing | time to find the dataset's files, and how many were found |
| Footers | time the footer passes cost, and how many footers were read |
| Last page | time from asking for the rows on screen to having them, and how many files were read for them |
| Total | the listing and the footers added up |

"Footers read" is not the number of files. A footer is read more than once on
most datasets — one large enough to open before its footers are read has them
read again behind the open, and one whose open could not settle its row count
reads them again to count — so a three-file directory commonly reports six. The
figure is what the reads cost, which is the point of it; the size of the
dataset is on the Listing row.
