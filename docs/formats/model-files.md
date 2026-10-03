# Model files

Read: [in memory](index.md#how-each-format-is-read), from the header.

```bash
datui model.safetensors
datui Llama-3-8B-Q4_K_M.gguf
datui model.safetensors.index.json     # a sharded checkpoint, as one table
datui path/to/checkpoint/              # the same, from its directory
datui https://huggingface.co/ORG/MODEL/resolve/main/model.safetensors.index.json
datui s3://bucket/checkpoints/llama/   # a prefix of shards, read where it is
```

A SafeTensors or GGUF file opens as a table with one row per tensor. Only the
header is read, so a 70 GB model opens as fast as a small one.

| Format | Columns |
|---|---|
| SafeTensors | `name`, `dtype`, `shape`, `params`, `bytes`, `offset_start`, `offset_end` |
| GGUF | `name`, `type`, `shape`, `params`, `bytes`, `offset` |

- `shape` is a list; `params` is its product.
- GGUF `type` is the quantization type (`Q4_K`, `Q8_0`, `F16`). `bytes` is
  null for a type datui does not know the size of.
- GGUF `shape` lists dimensions as the file does, fastest-varying first.
- Offsets are as the file records them, from the start of the tensor data.
  SafeTensors rows are in data order; GGUF rows are in file order.
- A sharded checkpoint opens from its `model.safetensors.index.json` or its
  directory, with a `file` column first. Several model files named together
  open the same way.
- Files are recognized by their first bytes too, so a `.bin` or a file with
  no extension opens when it is SafeTensors or GGUF. GGUF versions 2 and 3
  are read, in either byte order.

Press <kbd>i</kbd> for the [Model tab](../user-guide/dataset-info.md#model): parameter
count, size, the dtype or quantization mix, and the header's metadata
(`__metadata__`, or GGUF's key/value pairs).

A header that is corrupt, or a tensor that reaches past the end of the file
(a download cut short), is refused with an error.

Remote model files are not downloaded. Only their headers are fetched, with
ranged requests:

| Source | Read |
|---|---|
| HTTP(S), S3, GCS, Azure file | SafeTensors: the first 64 KiB, which holds most headers whole, then the rest of a longer header. GGUF: growing ranges until the tensor list ends |
| `model.safetensors.index.json` | The index, then each shard's header, beside the index's URL, eight shards at a time |
| S3, GCS or Azure prefix | Every SafeTensors or GGUF file directly under it, as a directory on disk. The listing stops at 10,000 objects; the Notes tab says when it did |

A server that does not send byte ranges gets the download question instead,
which says why, and the file is downloaded whole. Shards named by an index
cannot be downloaded this way, and the open says so.
