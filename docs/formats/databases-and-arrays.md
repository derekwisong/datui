# Databases and arrays

## SQLite databases

Read: [lazy, in place](index.md#how-each-format-is-read); a database in a bucket or over
HTTP(S) is downloaded first.

```bash
datui shop.db                        # its one table, or the list of its tables
datui shop.db --table orders         # a table or view by name
datui shop.db/orders                 # the same
cat shop.db | datui --table orders   # from standard input
```

| The database | What happens |
|---|---|
| One table or view of its own | Opens it |
| Several | Opens the home screen inside the database: a row per table and view, like a directory of tables. <kbd>Enter</kbd> opens one; <kbd>q</kbd> comes back to the list |
| Several, downloaded or piped in | Refused with the names of its tables; `--table` picks one |

SQLite's own tables (`sqlite_master`, `sqlite_sequence`, the `sqlite_stat`
tables, a full-text index's shadow tables) are hidden until
<kbd>Ctrl</kbd>+<kbd>A</kbd>; `--table` opens them by name. A file is known by
its first bytes whatever it is called. The home screen labels a database with
its tables (`3 tables`).

The Info panel's [SQLite tab](../user-guide/dataset-info.md#file-format-tabs) gives the page size,
the schema and user versions, and each table with its columns and, where `ANALYZE`
stored them, its rows. It counts no table: that would read the whole database.

A table is read in place; nothing is copied.

| | How |
|---|---|
| The rows on screen | Read from SQLite a page at a time, by the table's rowid (or primary key), so the first rows show at once and <kbd>End</kbd> costs what the top does |
| Row count | SQLite's `count(*)`, in the background |
| Sort and filters from the sidebar | Run in SQLite as `ORDER BY` and `WHERE`, so an index on the column serves them. Ties keep the table's order and nulls sort last, as for any other file |
| A query, analysis, Data Quality, a chart, an export | Read the columns they use from SQLite a batch at a time; what they hold is in memory, as for a JSON or Excel file. A query's simple comparisons run in SQLite |
| Leaving the table (<kbd>Ctrl</kbd>+<kbd>O</kbd>, quit) | Stops whatever SQLite is running for it |

A view is paged by position and cannot be reversed with <kbd>r</kbd> in SQLite
(Polars does it). A sort on a column without an index has SQLite sort the
rows for each page; SQLite may use temporary files in the temp directory to do
so.

Columns are typed by what they declare, as SQLite reads a declared type:

| Declared | Column |
|---|---|
| `INTEGER`, `INT`, `BIGINT`, anything with `INT` | `i64` |
| `REAL`, `FLOAT`, `DOUBLE` | `f64` |
| `TEXT`, `VARCHAR(n)`, `CLOB` | `str` |
| `BLOB` | `binary` |
| nothing, `NUMERIC`, `DECIMAL`, `BOOLEAN`, `DATE` | by the values in the first 1,000 rows: whole numbers `i64`, numbers `f64`, blobs `binary`, text `str` |

SQLite lets a column hold values of any type. A column whose first 1,000 rows
hold values of several types is read as text, numbers as SQLite writes them and
blobs as `X'0A1B'`. After the open, one pass over the table checks the rest: a
value further on that is not a number, in a number column, reads as null, and
the Info panel's Notes tab says how many. Dates stay text, as SQLite stores
them.

The database is only read:

| | |
|---|---|
| Opened | Read only, with `query_only` and defensive mode. Extensions cannot be loaded, and reading a table runs no trigger |
| A WAL database with a `-wal` file | Read through it, so what another program has committed is seen. SQLite creates the `-shm` index beside it if it is missing |
| A WAL database without a `-wal` file | Read as the file stands (SQLite's `immutable`), writing nothing and taking no lock. A program that starts writing it during the read can make the read fail or come out wrong |
| A `-wal` without its `-shm`, in a directory datui cannot write to | An error: read without the WAL, it would lack what was committed there |
| A `-journal` left by a program that stopped mid-write | An error: datui does not roll it back. Opening the database once with the `sqlite3` tool does |
| A program writing the database meanwhile | datui waits up to 2 seconds for its lock. Without WAL, the program cannot commit while datui reads, which is a page at a time except for a whole-table read |
| Not a SQLite database, or damaged | An error |

A compressed database (`shop.db.gz`) is not read; decompress it first.

## NumPy arrays

Read: [lazy](index.md#how-each-format-is-read), from a map of the file; an array
compressed in an `.npz` archive is decompressed once to a temporary file first.

```bash
datui prices.npy
datui run.npz                    # its one array, or the list of its arrays
datui run.npz --table weights    # an array by name
datui run.npz/weights            # the same
```

| The array | Columns |
|---|---|
| 1-D | One, named for the file (`prices.npy` is `prices`) or the archive's array |
| 2-D | One per index, `0` to `n-1`; more than 1,024 make one Array column, `values` |
| Structured (`[('ts', '<u8'), ('px', '<f8')]`) | One per field; a nested field is `outer.inner`, a subarray field an Array column |
| 0-D | One row |
| 3-D or more | An error that gives the shape |

| `dtype` | Column |
|---|---|
| `b1` | `bool` |
| `i1` to `i8`, `u1` to `u8` | The integer of that width |
| `f2`, `f4` | `f32` |
| `f8` | `f64` |
| `c8`, `c16` | An Array of two floats: real, imaginary |
| `S` | `str`, NUL padding trimmed |
| `U` | `str`, from UTF-32 |
| `V` | `binary` |
| `M8[ns]`, `M8[us]`, `M8[ms]` | `datetime` in that unit |
| `M8[s]`, `M8[m]`, `M8[h]` | `datetime[ms]` |
| `M8[D]` | `date` |
| `m8[...]` | `duration`, by the same units |
| `M8` and `m8` in months, years or finer than `ns` | `i64`, the count as stored |
| `O` | An error: Python objects are pickled, and datui does not unpickle |

- `NaT` is null.
- Big-endian (`>i4`) and little-endian fields mix in one array.
- Fortran (column-major) order reads the same as C order.
- Padding fields (`align=True`) are left out, and fields at offsets
  (`offsets`, `itemsize`) are read where they are.
- A file shorter than its shape says shows the rows it holds; the Notes tab
  says so.
- A file named without `.npy` is known by its first bytes, `\x93NUMPY`.

An `.npz` archive of several arrays opens the home screen inside it, a row per
array in the order they were saved, like a directory. <kbd>Enter</kbd> opens
one; <kbd>q</kbd> comes back to the list. An archive downloaded or piped in is
refused with the names of its arrays; `--table` picks one. An array saved with
`np.savez` is read in place in the archive; one saved with
`np.savez_compressed` is decompressed to the temp directory and removed when
the dataset closes.

Press <kbd>i</kbd> for the NumPy tab: shape, type, order and format version,
and each field's type and offset.
