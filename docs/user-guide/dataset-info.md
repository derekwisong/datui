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

A folder of Parquet files rarely holds files that agree, and rarely holds only
files with something in them. Where datui notices either, it says so:

| Note | From |
|---|---|
| A column is not in every file, and which partitions have it | The footers and the file names |
| Files disagree on a column's type, beyond what widening settles | The footers |
| A column is stored as more than one type the scan can read into one | The footers |
| A column is read as text, so it compares as text | The footers |
| A file holds no rows at all | The footers |
| Row groups are large enough that a page of rows costs much more than a page | The footers |
| There are very many files and the middle one holds very little | The listing and the footers |
| The folders do not all partition by the same keys | The file names |
| A file's footer could not be read, so it was left out | The footers |
| Files in the folder are not Parquet, so they are not in the table | The listing |
| A filter or sort leaves rows out, because their files hold its column in another type | The footers |

Where the files that have a column fall into a shape worth naming, the note says so
as well as counting them: `fee is in 4,001 of 6,541 files, none before
date=2010-07-18` is a field a feed started sending, and `oops is in 1 of 6,541
files, only date=2024-03-02` is one folder's mistake. Only those two shapes —
a column scattered across a dataset is just a count. `none before` also needs the
folders to sort the way you would read them: where partition values are not
zero-padded, `part=10` comes before `part=2` in the listing and there is no
honest way to say where a column starts, so nothing is said. A dataset whose footers were
sampled gets no such phrase: a file whose footer was not read looks like a file
missing nothing, and a range drawn over those would be a guess.

A folder of Parquet files often holds other things, and what matters is where
they are rather than what they are called. A file in a folder that holds data is
one somebody may have meant to be in the table — a `.csv` beside the parts — and
that is counted and said. A file in a folder with no data anywhere beneath it is
somebody's plumbing: a table format's log, a manifest directory, a folder of
images. Delta and Hudi name theirs with a leading `_` or `.`, Iceberg does not,
and the next format will do something else again; none of them is a mistake and
datui says nothing about any of them on its own.

An object with nothing in it and a name that says Parquet is the exception worth
leading with: a write that stopped. In a bucket nothing else can see it — the
object is dropped before any footer is read — while on disk the same file turns
up as a footer that could not be read, which is a different note saying the same
thing. One with nothing in it and no such
name is a folder marker — a console leaves one per partition — and is plumbing
like the rest.

What it counts is what the listing saw. A folder it could not read, or one
deeper than datui walks, is not in the total.

Every note says what it is based on — `in all 6,541 footers`, or
`in 20,000 of 200,000 footers (sample)` — so a count never stands for files
datui has not looked at. A cloud folder that is still reading its footers behind
the data says `in 2 of 6,541 footers (sample)` until they land, and the notes are
rewritten from all of them when they do. None of them costs a read of its own:
they come from the footers the schema and the row count already needed.

A note is one sentence and the line beneath it saying what it is based on;
<kbd>↑</kbd> and <kbd>↓</kbd> move between them. When the list is taller than the
panel, the corner says how many notes are out of view — unless the note the
cursor is on fills the panel, where it keeps that row. Whole notes only: half a
note is worse than none, since a claim with no basis under it, or a basis with no
claim above it, is the misreading the basis is there to prevent. Where the note
the cursor is on will not fit at all, the panel says so rather than showing part
of it; a shorter note may still fit.

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
tab is still there either way. A note is an observation about the
data, not a fault in it, so there is no pop-up and no error styling.

## Measurements

At the foot of the **Resources** tab, what opening this dataset cost.

It is there for the datasets datui finds and reads itself — a folder of Parquet
opened with `--hive`, a remote prefix, and a single remote object. Anything else
is handed straight to Polars, which does not report what it did, and shows no
Measurements section at all: a single local file, a CSV, a local folder opened
without `--hive`, and a folder or a prefix opened with
`single_spine_schema = false`.

| Metric | Formula |
|---|---|
| Listing | time to find the dataset's files, and how many were found |
| Footers | time the footer passes cost, and how many footers were read |
| Total | the two times added up |

A row appears only where there was something to measure. A single remote object
is named, not searched for, so it has no Listing row — and with one stretch
there is nothing to total, so it has no Total row either.

For a remote dataset the Footers row also carries the requests datui made and
the bytes they returned, and the Total carries them too. Those reads datui
issues itself, sixty-four at a time, so it can count them exactly.

**No Listing row reports requests, and a local dataset reports none anywhere.**
A local folder is walked rather than requested, so there are none to report; and
every remote listing hands its paging to the object store, which does not say
how many round trips it took. Every figure here is one datui produced itself,
and where it cannot count something it shows nothing rather than a zero, because
a zero reads as "none" rather than "not measured".

**"Footers read" is not the number of files.** A footer is read more than once
on most datasets — one large enough to open before its footers are read has
them read again behind the open, and one whose open could not settle its row
count reads them again to count — so a three-file folder commonly reports six.
The figure is what the reads cost, which is the point of it; the size of the
dataset is on the Listing row.

A count pass has to find the files before it can read them, so its walk of the
folder is in the Footers time too. Only the open's own search is on the Listing
row.

Counting is measured once. It runs again whenever the row count is invalidated
— clearing a filter does it — and those later passes are re-work on a dataset
that is already open, not part of what opening it cost.

The figures keep moving after you can first see them. A large remote dataset
opens once two footers have been read and reads the rest behind the open, so
the Footers row and the Total climb while you are already looking at rows — see
[datasets whose files differ](loading-data.md#files-that-disagree).
