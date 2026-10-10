# Model files

A SafeTensors or GGUF file opens as a table of its tensors, one row each. The
table comes from the file's header; the weights are never read.

**`make_tiny_safetensors.py`**

```python,file=make_tiny_safetensors.py
import ctypes
import json

# One tensor, `w`: 2 x 2 float32, at bytes 0 to 16 of the data.
header = json.dumps({"w": {"dtype": "F32", "shape": [2, 2], "data_offsets": [0, 16]}}).encode()
weights = (ctypes.c_float.__ctype_le__ * 4)(1, 2, 3, 4)

with open("tiny.safetensors", "wb") as f:
    f.write(len(header).to_bytes(8, "little"))  # the header's length, u64
    f.write(header)
    f.write(bytes(weights))
```

```bash
python3 make_tiny_safetensors.py
datui tiny.safetensors
```

```bash,network
datui https://huggingface.co/TheBloke/TinyLlama-1.1B-Chat-v1.0-GGUF/resolve/main/tinyllama-1.1b-chat-v1.0.Q2_K.gguf
datui https://huggingface.co/Qwen/Qwen2.5-7B/resolve/main/model.safetensors.index.json
```

| | SafeTensors | GGUF |
|---|---|---|
| Extensions | `.safetensors`, `model.safetensors.index.json` | `.gguf` |
| Columns | `name`, `dtype`, `shape`, `params`, `bytes`, `offset_start`, `offset_end` | `name`, `type`, `shape`, `params`, `bytes`, `offset` |
| Rows | In data order | In file order |
| Read | [in memory](index.md#how-each-format-is-read), from the header, without the memory warning | the same |
| Info tab | Model | Model |

- `shape` is a list; `params` is its product.
- GGUF `type` is the quantization type (`Q4_K`, `Q8_0`, `F16`). `bytes` is
  null for a type datui does not know the size of. `shape` lists dimensions as
  the file does, fastest-varying first.
- Offsets are as the file records them, from the start of the tensor data.
- A sharded checkpoint opens from its `model.safetensors.index.json` or its
  directory, with a `file` column first. Several model files named together
  open the same way.
- A `.bin` or a file with no extension opens when its first bytes say
  SafeTensors or GGUF. GGUF versions 2 and 3 are read, in either byte order.
- A header that is corrupt, or a tensor that reaches past the end of the file
  (a download cut short), is refused with an error.

The Model tab (<kbd>i</kbd>) gives the parameter count, the size, the dtype or
quantization mix, and the header's metadata (`__metadata__`, or GGUF's key and
value pairs); see [Info panel](../user-guide/dataset-info.md#model).

## Remote model files

Only the headers are fetched, with ranged requests; the weights are never
downloaded.

| Source | Read |
|---|---|
| An HTTP(S) URL, or an S3, GCS or Azure object | SafeTensors: the first 64 KiB, which holds most headers whole, then the rest of a longer one. GGUF: larger and larger ranges until the whole tensor list is read |
| `model.safetensors.index.json` | The index, then each shard's header beside it, eight shards at a time |
| An S3, GCS or Azure prefix | Every SafeTensors or GGUF file directly under it, like a directory on disk. The listing stops at 10,000 objects; the Notes tab says when it did |

If a server does not support byte ranges, datui asks whether to download the
whole file instead, and says why. Shards named by an index cannot be downloaded
this way, and the open says so.
