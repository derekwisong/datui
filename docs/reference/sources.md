# Dataset collections

A collection is a named list of datasets, local or remote, that the home screen
shows as one section. Logins are in [Cloud connections](cloud-sources.md);
directories to browse are `[home] directories` in [Settings](settings.md#home).

`[[sources]]` in `config.toml` names a collection; the home screen lists its
datasets under its label, one row each, wherever the data lives:

```toml
[[sources]]
name = "my-datasets"
label = "My datasets"

[[sources.datasets]]
name = "Sales"
path = "~/datasets/sales.parquet"
description = "Monthly sales"

[[sources.datasets]]
name = "Weather"
url = "s3://noaa-ghcn-pds/parquet/"
auth = "anonymous"
description = "Daily weather observations"

[[sources.datasets]]
name = "Penguins"
url = "https://vincentarelbundock.github.io/Rdatasets/csv/palmerpenguins/penguins.csv"
```

A dataset in a private store names the [connection](cloud-sources.md#connections)
that reads it; replace `<BUCKET>` and `<CONNECTION>` with yours:

```toml,template
[[sources]]
name = "team"

[[sources.datasets]]
name = "Orders"
url = "s3://<BUCKET>/orders/"
connection = "<CONNECTION>"
```

| Collection field | Meaning |
|---|---|
| `name` | Required. Lowercase letters, digits and `-`, at most 40 characters. What `[home] hide` and a later file name it by |
| `label` | The section title. Default: the name |

| Dataset field | Meaning |
|---|---|
| `name` | Required. The row's name, unique in the collection |
| `path` | A local file or directory. `~` and `$VAR` expand; a relative path is relative to the config file that names it |
| `url` | An `s3://`, `gs://` or Azure file or directory, or an `http://` or `https://` data file |
| `auth` | Object-store `url` only: `auto` (the default) or `anonymous` |
| `connection` | Object-store `url` only: the [`[[cloud.connections]]`](cloud-sources.md#connections) entry whose login reads it |
| `description`, `publisher`, `license`, `homepage` | Shown in the details pane |
| `size` | HTTP(S) `url` only: about how many bytes the file is, shown on its row before anything is downloaded |

A dataset has exactly one of `path` and `url`. No two datasets in a collection
share a name or a location.

| Location | <kbd>Enter</kbd> | Read with |
|---|---|---|
| Local file | Opens it | |
| Local directory | Steps inside | |
| Object-store file | Opens it | `auth` or `connection` |
| Object-store directory | Steps inside. <kbd>Backspace</kbd> at its top comes back to the list | `auth` or `connection` |
| HTTP(S) file | Downloads it, after asking, and opens it; a built-in catalog file under 50 MB is downloaded without asking | No login |

| Reading | Means |
|---|---|
| `auth = "auto"` | As the URL typed at <kbd>~</kbd> would be: the login found for that cloud, unsigned when there is none or it is refused |
| `auth = "anonymous"` | No credentials and no signature, whatever login the machine has |
| `connection = "<name>"` | That connection's login and nothing else. Its `kind` must match the URL, and an Azure connection's `account` the URL's account |

HTTP(S) is always read with no login, and its URL must name a file datui reads:
a web server has no listing to browse. Name a connection with `connection`, not
in the URL: `s3://onprem@bucket/` is refused.

A collection holds references, not data. Nothing is read until a dataset is
opened or entered, so a remote directory's row says `dataset` until then. A local
path with nothing there stays listed and says `missing`.

The built-in collection, `public`, lists [public datasets](../user-guide/home-screen.md#public-datasets)
after everything else:

| To | Do |
|---|---|
| Replace it | Define a collection named `public`. It replaces the whole catalog; nothing built in is merged in |
| Drop it | `[home] builtin_catalog = false`. A configured `public` still shows |
| Hide a collection, `public` or yours | `[home] hide = ["public", "my-datasets"]` |

`datui config init` writes the catalog as an active `public` collection to
edit. It is a snapshot: later datui releases do not change it. Run
`datui config init --force` for a new one, after saving any edits you want
to keep.

Across [imported files](../user-guide/configuration.md#importing-other-config-files), collections are listed in
the order defined, imports first. A later collection with the same name replaces
the earlier one whole; datasets are never merged. `[home] hide` adds up across
files, and the last file that sets `builtin_catalog` decides it. Two
collections with one name in one file are an error.

Collections are apart from `[home] directories`, remembered directories and
`RECENT`. A directory there is a place to look through, and whatever you open goes
into `RECENT` whether or not a collection names it.
