# Columnar and JSON

## Arrow IPC streams

Read: [converted once](index.md#how-each-format-is-read).

Arrow IPC streams, the format of a Hugging Face `datasets` cache, are told
from IPC files by their first bytes: a `.arrow`, `.arrows`, `.ipc` or `.feather`
file, one with no extension, or one read with `--format arrow`. A stream has no
index of its rows, so it is converted once to an IPC file in the temp directory,
then scanned lazily like any other:

```bash
datui ~/.cache/huggingface/datasets/imdb/plain_text/0.0.0/abc123/imdb-train.arrow
datui ~/.cache/huggingface/datasets/imdb/plain_text/0.0.0/abc123/   # the train split
datui --table test ~/.cache/huggingface/datasets/imdb/plain_text/0.0.0/abc123/
datui my_dataset/                 # save_to_disk shards: data-00000-of-00004.arrow ...
datui --table test my_dataset_dict/   # save_to_disk of a DatasetDict: one split
```

| What | How it opens |
|---|---|
| One stream | Converted, then scanned |
| A directory of stream shards | Converted together into one file, in name order; stream shards with different columns fail |
| IPC files among the streams | Scanned in place, not copied, and stacked with the streams in name order |
| A `datasets` cache directory (`name-train.arrow`, `name-test-00000-of-00002.arrow`, ...) | One split: the one `--table` names, else `train`, `validation`, `test`, then the first by name. The Schema tab lists the others |
| A DatasetDict saved with `save_to_disk` (`dataset_dict.json` and a directory per split) | One split's directory, chosen the same way |
| A cache directory on the home screen | Its splits listed inside it (`abc123/test`), above its files; <kbd>Enter</kbd> on one opens that split |
| `cache-*.arrow` files `map()` wrote in a cache directory | Left out; a note on the Notes tab counts them |
| `dataset_info.json`, `state.json` beside `.arrow` files | Left aside as the dataset's metadata; either one marks a cache directory |
| LZ4 or ZSTD buffers | Read; written out uncompressed, so the copy can be larger than the stream |

The loading screen shows how far the conversion has got;
<kbd>Ctrl</kbd>+<kbd>O</kbd> stops it and removes the partial file. `--temp-dir`
chooses where the copy goes, and it is removed with the dataset. A stream larger
than the temp directory's free space is refused before it is written.

## Excel

Read: [in memory](index.md#how-each-format-is-read).

Excel opens the first worksheet unless `--table` names another, by name
(`--table Sales`) or, when no worksheet is so named, by index (`--table 0`).

On the [home screen](../user-guide/home-screen.md), Enter on an `.xlsx` or `.xlsm` workbook opens
its first worksheet and <kbd>→</kbd> lists its worksheets as tables (`book.xlsx/Sales`), read from the
workbook's directory without its cells; a hidden worksheet shows with <kbd>Ctrl</kbd>+<kbd>A</kbd>.
An `.xls` or `.xlsb` workbook opens its first worksheet. The Info panel's
[Excel tab](../user-guide/dataset-info.md#file-format-tabs) gives each worksheet's range and size.
