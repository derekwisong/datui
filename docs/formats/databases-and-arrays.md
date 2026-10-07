# Databases and arrays

SQLite databases and NumPy arrays are read where they are, a page of rows at a
time, and a file of several tables lists them like a directory.

## SQLite

**`make_shop_db.py`**

```python,file=make_shop_db.py
import sqlite3

db = sqlite3.connect("shop.db")
db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT, amount REAL)")
db.execute("CREATE TABLE customers (name TEXT, city TEXT)")
db.executemany("INSERT INTO orders (customer, amount) VALUES (?, ?)", [("ana", 9.5), ("bo", 3.25)])
db.executemany("INSERT INTO customers VALUES (?, ?)", [("ana", "Lima"), ("bo", "Oslo")])
db.commit()
db.close()
```

```bash
python3 make_shop_db.py
datui shop.db --table orders
datui shop.db/orders
cat shop.db | datui --table orders
```

Continuing from above, a database of several tables opens the home screen
inside it:

```bash,continue,expect=screen
datui shop.db
```

| | |
|---|---|
| Extensions | `.db`, `.sqlite`, `.sqlite3`, `.db3`; any other name by its first bytes, `SQLite format 3` |
| Read | [lazy](index.md#how-each-format-is-read), in place; downloaded first from a bucket or over HTTP(S) |
| `--table` | A table or view by name, or `shop.db/orders` |
| Info tab | SQLite: page size, schema and user versions, text encoding, and each table's columns and the rows `ANALYZE` stored. No table is counted |
| Not read | A compressed database (`shop.db.gz`): decompress it first |

| The database | What happens |
|---|---|
| One table or view of its own | Opens it |
| Several | Opens the home screen inside the database: a row per table and view, like a directory of tables. <kbd>Enter</kbd> opens one; <kbd>q</kbd> comes back to the list |
| Several, downloaded or piped in | Refused with the names of its tables; `--table` picks one |

SQLite's own tables (`sqlite_master`, `sqlite_sequence`, the `sqlite_stat`
tables, a full-text index's shadow tables) are hidden until
<kbd>Ctrl</kbd>+<kbd>A</kbd>; `--table` opens them by name. The home screen
labels a database with its tables (`3 tables`).

<kbd>T</kbd> at the table lists the database's tables and views, each with its
kind, columns and the rows `ANALYZE` stored, and opens another; so does
<kbd>Enter</kbd> on a table of the SQLite tab. Nothing is counted to list them.

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

Columns are typed by what they declare:

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

## NumPy

**`make_arrays.py`** writes `prices.npy` and `run.npz`:

```python,file=make_arrays.py
import ctypes
import zipfile


class Preamble(ctypes.LittleEndianStructure):
    _layout_ = "ms"
    _pack_ = 1  # no padding between fields
    _fields_ = [
        ("magic", ctypes.c_char * 6),
        ("major", ctypes.c_uint8),
        ("minor", ctypes.c_uint8),
        ("header_len", ctypes.c_uint16),
    ]


def npy(descr, shape, values):
    """An .npy file: the preamble, a header padded to 64 bytes, then the values."""
    header = repr({"descr": descr, "fortran_order": False, "shape": shape}).encode()
    header += b" " * (63 - (10 + len(header)) % 64) + b"\n"
    return bytes(Preamble(b"\x93NUMPY", 1, 0, len(header))) + header + bytes(values)


def f8(*values):  # little-endian float64, as "<f8" says
    return (ctypes.c_double.__ctype_le__ * len(values))(*values)


def f4(*values):  # little-endian float32, "<f4"
    return (ctypes.c_float.__ctype_le__ * len(values))(*values)


with open("prices.npy", "wb") as f:
    f.write(npy("<f8", (3,), f8(1.5, 2.5, 4.0)))
with zipfile.ZipFile("run.npz", "w") as z:
    z.writestr("weights.npy", npy("<f4", (2, 2), f4(1, 2, 3, 4)))
    z.writestr("bias.npy", npy("<f4", (2,), f4(0.5, -0.5)))
```

```bash
python3 make_arrays.py
datui prices.npy
datui run.npz --table weights
datui run.npz/weights
```

| | |
|---|---|
| Extensions | `.npy`, `.npz`; any other name by its first bytes, `\x93NUMPY` |
| Read | [lazy](index.md#how-each-format-is-read), from a map of the file. An array saved with `np.savez_compressed` is decompressed to the temp directory first, and removed when the dataset closes |
| `--table` | An array of an `.npz` archive by name, or `run.npz/weights` |
| Several arrays | `datui run.npz` opens the home screen inside the archive, a row per array in the order saved. Downloaded or piped in, it is refused with the arrays' names |
| Info tab | NumPy: shape, type, order and format version, and each field's type and offset |

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
- A header over 4 MiB (`npy_header_bytes` in `[limits]`) is refused.
