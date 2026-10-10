# Catalogs

A catalog is a TOML file that lists named datasets, local or remote. The home
screen shows each catalog as a section under its label, with one row per
dataset. Logins are set up in [Cloud connections](cloud-sources.md).

```bash,expect=exit
datui catalog show examples
```

| Catalog | File | Written by |
|---|---|---|
| Yours, `My datasets` | `catalog.toml` in the config directory | You, and <kbd>Ctrl</kbd>+<kbd>D</kbd> on the home screen |
| A team's or a project's | Any `*.toml` in `catalogs/` in the config directory, or a file elsewhere listed by `catalogs` in the config | You; datui only reads it |
| `Example datasets` | Comes with datui; `datui catalog show examples` prints it | datui |

| Command | Does |
|---|---|
| `datui catalog show` | List the catalogs: id, label, datasets, where each comes from, and its file |
| `datui catalog show NAME` | Print a catalog's file: `mine`, `examples`, or another catalog's file name |
| `datui catalog check FILE` | Check a file and list its datasets. A mistake is reported with its line and the fix |
| `datui config init` | Write the config file, an empty `catalog.toml` and the `catalogs/` directory; an existing `catalog.toml` is kept |

## A catalog file

The top level holds `label` and `description`. Every other table is a
dataset. Its key is a short id (lowercase letters, digits and `-`), and `name`
is the name its row shows:

```toml,catalog
label = "My datasets"

[sales]
name = "Sales"
path = "~/datasets/sales.parquet"
description = "Monthly sales"
columns.region = { description = "Sales region", values = { NE = "Northeast", W = "West" } }
columns.amount = { description = "Net of returns", unit = "USD" }

[weather]
name = "Weather"
url = "s3://noaa-ghcn-pds/parquet/"
auth = "anonymous"
bookmarks."Daily highs, 2024" = "by_year/YEAR=2024/ELEMENT=TMAX/"

[penguins]
name = "Penguins"
url = "https://vincentarelbundock.github.io/Rdatasets/csv/palmerpenguins/penguins.csv"
size = 16480

[nas]
name = "NAS data"
path = "/mnt/nas/data"
```

A dataset in a private store names the
[connection](cloud-sources.md#connections) to read it with. Replace `<BUCKET>`
and `<CONNECTION>` with yours:

```toml,template
[orders]
name = "Orders"
url = "s3://<BUCKET>/orders/"
connection = "<CONNECTION>"
```

| Top-level key | Meaning |
|---|---|
| `label` | The section's title. Default: `My datasets` for `catalog.toml`, else the id |
| `description` | What the catalog is for |

| Dataset key | Meaning |
|---|---|
| `name` | Required. The row's name, unique in the catalog |
| `path` | A local file or directory. `~` and `$VAR` expand; a relative path is relative to the catalog file |
| `url` | An `s3://`, `gs://` or Azure file or directory, or an `http://` or `https://` data file |
| `auth` | Object-store `url` only: `auto` (the default) or `anonymous` |
| `connection` | Object-store `url` only: the [`[[cloud.connections]]`](cloud-sources.md#connections) entry whose login reads it |
| `description`, `publisher`, `license` | Shown in the details pane and the Documentation view |
| `homepage`, `documentation` | Links: the dataset's page, and an `https://` link to the publisher's documentation of its columns |
| `size` | HTTP(S) `url` only: the file's approximate size in bytes, shown as `~33 MiB` until datui measures it |
| `columns.NAME` | What a column means; see [Columns](#columns) |
| `bookmarks."Name"` | A place inside a directory to start from, relative to the dataset's `path` or object-store `url` |

A dataset has exactly one of `path` and `url`. No two datasets in a catalog
share a name or a location.

| Location | <kbd>Enter</kbd> | Read with |
|---|---|---|
| Local file | Opens it | |
| Local directory | Steps inside | |
| Object-store file | Opens it | `auth` or `connection` |
| Object-store directory | Steps inside. <kbd>Backspace</kbd> at its top comes back to the list | `auth` or `connection` |
| HTTP(S) file | Asks, then downloads and opens it. An Example datasets file under 50 MiB downloads without asking, and asks if the download passes 50 MiB | No login |

| Reading | Means |
|---|---|
| `auth = "auto"` | As the same URL typed at <kbd>~</kbd> would be: with the login found for that cloud, or unsigned when there is none or it is refused |
| `auth = "anonymous"` | No credentials and no signature, whatever login the machine has |
| `connection = "<name>"` | That connection's login and nothing else. Its `kind` must match the URL, and an Azure connection's `account` the URL's account |

HTTP(S) is always read with no login, and its URL must name a file datui can
read, since a web server has no listing to browse. Name a connection with the
`connection` key, not in the URL: `s3://onprem@bucket/` is refused.

A catalog holds references, not data. Nothing is read until you open or enter a
dataset, so a remote directory's row says `dataset` until then. A web file's
row sends one `HEAD` request when selected, to show the file's real size. A local path
with nothing there stays listed and says `missing`.

<a id="codebooks"></a>
<a id="documentation"></a>

## Columns

A dataset can say what its columns mean. The home screen's details pane lists
them, <kbd>Ctrl</kbd>+<kbd>E</kbd> shows them in the
[Documentation view](../user-guide/home-screen.md#documentation-view), and once
the data is open the [Info panel](../user-guide/dataset-info.md) and the
[inspector](../user-guide/inspecting-rows.md) explain each one. Write each column
on one line, and give a long list of values a table of its own:

```toml,catalog
[weather]
name = "GHCN daily"
url = "s3://noaa-ghcn-pds/parquet/"
auth = "anonymous"
documentation = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/readme.txt"

columns.DATA_VALUE = { description = "Data value for ELEMENT", unit = "per ELEMENT" }
columns.Q_FLAG.description = "Quality flag; blank is normal"

[weather.columns.Q_FLAG.values]
"" = "did not fail any quality assurance check"
S = "failed spatial consistency check"
```

| Column key | Meaning |
|---|---|
| `description` | What the column holds. Shown in Info's `About` column and under the inspector's value |
| `unit` | Its unit or format, shown after the description: `tenths of mm`, `YYYYMMDD` |
| `values` | A map from code to meaning. The inspector shows the meaning of the value under the cursor; `""` is what a blank or null value means. Codes match exactly, case included |

A column with its own `[id.columns.NAME.values]` table must use dotted keys,
`columns.NAME.description = "..."`. TOML closes an inline table,
`columns.NAME = { ... }`, so a `values` table after it is an error.
`datui catalog check` names the line and the fix.

A bookmark is listed under its dataset. <kbd>Enter</kbd> on a bookmark opens
the whole place as one table, and <kbd>→</kbd> steps inside.

## Your catalog

<kbd>Ctrl</kbd>+<kbd>D</kbd> on a home row adds it to `catalog.toml`. The row can
be a file, a directory, an object-store place, or a heading's directory. It is
saved under the row's name, with an id made from the name. A row from another
catalog is copied with its location, login and description.
<kbd>Ctrl</kbd>+<kbd>D</kbd> or <kbd>Delete</kbd> on a row from `catalog.toml`
forgets it: its table and the tables under it are removed, and the rest of the
file, comments included, stays as written.
datui never writes any other catalog file.

Directories that <kbd>Ctrl</kbd>+<kbd>D</kbd> kept before 0.4.0 move into
`catalog.toml` the first time the home screen opens.

## Team catalogs

<a id="catalogs-directory"></a>

Drop a catalog file into `catalogs/` in the config directory
(`~/.config/datui/catalogs/` on Linux, beside `catalog.toml`): every `*.toml`
there is a catalog, read in file-name order, and other files are ignored.

List a catalog file stored elsewhere, such as a team's on a shared drive, in
the config. A relative path is relative to the config file that lists it, so a
team's shared config can list a catalog stored next to it. Replace `<CATALOG_FILE>` with the
file's path:

```toml,template
catalogs = ["<CATALOG_FILE>"]
```

To give a file you cannot rename or edit an id or label of its own, list it as a
table with `path` and an `id` or `label`. Replace `<CATALOG_FILE>` with the file's path:

```toml,template
catalogs = [{ path = "<CATALOG_FILE>", id = "acme", label = "ACME" }]
```

| Key | Meaning |
|---|---|
| `path` | Required. The file |
| `id` | The catalog's id, in place of the file's name. `[home] hide` names catalogs by id, and the id `examples` replaces the Example datasets |
| `label` | The section's title, in place of the file's own `label` |

A catalog file's name, without `.toml`, is its id. A listed file that does not
exist is skipped with a warning, as a missing import is.

A catalog file with a mistake, `catalog.toml` included, is left out instead of
stopping datui. Its `file:line: message` goes to standard error and the log, and
its section on the home screen shows `▲ acme.toml:3 ...` instead of its rows.
Two files with the same id, wherever they are, are such a mistake, and so is a
file named `mine.toml`; the message names both files.
<kbd>Ctrl</kbd>+<kbd>D</kbd> never writes to a `catalog.toml` it cannot read,
and `datui catalog check` names the mistake and exits non-zero.

`catalogs` lists combine across
[imported files](../user-guide/configuration.md#importing-other-config-files),
imports first. The sections come after `My datasets`: `catalogs/` in file-name
order, then the listed files, then `Example datasets`.

| To | Do |
|---|---|
| Replace the example datasets | A catalog file named `examples.toml`, in `catalogs/` or listed: it replaces the whole catalog; nothing bundled is merged in |
| Rename a catalog you cannot edit | List it as `{ path = "...", id = "acme", label = "ACME" }`: a shared `catalog.toml` or `examples.toml` then no longer acts as yours or as the examples |
| Hide a catalog, or one entry | `[home] hide = ["acme", "examples/nyc-taxis"]`: a catalog by its id, an entry as `catalog/id`; hides combine across files, and a name that hides nothing is warned about (`public`, the id before 0.4.0, says it is now `examples`). `datui catalog show` marks a hidden catalog `(hidden by home.hide)` |
| Edit the example datasets | `datui catalog show examples > examples.toml`, move `examples.toml` into the config directory's `catalogs/` (`~/.config/datui/catalogs/` on Linux), then edit it. Don't redirect straight into `catalogs/`: the shell empties the file before datui reads it |

Catalogs are separate from `RECENT`. Everything you open goes into `RECENT`,
whether or not a catalog lists it.

## The example datasets

This is the catalog that comes with datui, as `datui catalog show examples` prints it: a worked
example of every key.

<!-- generated: public-catalog -->
> The bundled `examples` catalog: data its publishers host and maintain, read with no
> login. These are remote links, not bundled data: object-store roots browse, HTTP(S)
> files open directly, and nothing is fetched until an entry is opened.
>
> Each entry is labeled with its actual scope, and must be readable without
> credentials or requester-pays access. An HTTP(S) file gives its `size` in bytes, as
> measured when it was added: its row shows it, and one under 50 MiB is downloaded
> without asking. A rolling file's size is a typical one.
>
> An entry may carry `documentation`: the publisher's documentation of its columns, and
> `columns` notes taken from it, never from memory. `bookmarks` names places inside a
> directory dataset to start from; each must list with no login.
>
> A raw.githubusercontent.com link names a commit, not a branch, so a push upstream
> can't move, rename or change the file under its `size`.
>
> The weekly `Example datasets` workflow checks every entry and every bookmark.

```toml,output
label = "Example datasets"
description = "Data its publishers host and maintain, read with no login"

[nyc-flights]
name = "NYC flights (2013)"
url = "https://vincentarelbundock.github.io/Rdatasets/csv/nycflights13/flights.csv"
size = 33206996
description = "Departures from JFK, LaGuardia and Newark; delays in minutes"
publisher = "BTS / nycflights13; CSV hosted by Rdatasets"
license = "CC0 (nycflights13)"
homepage = "https://nycflights13.tidyverse.org/reference/flights.html"
documentation = "https://nycflights13.tidyverse.org/reference/flights.html"

columns.dep_time = { description = "Actual departure time, local", unit = "HHMM or HMM" }
columns.arr_time = { description = "Actual arrival time, local", unit = "HHMM or HMM" }
columns.sched_dep_time = { description = "Scheduled departure time, local", unit = "HHMM or HMM" }
columns.sched_arr_time = { description = "Scheduled arrival time, local", unit = "HHMM or HMM" }
columns.dep_delay = { description = "Departure delay; negative is an early departure", unit = "minutes" }
columns.arr_delay = { description = "Arrival delay; negative is an early arrival", unit = "minutes" }
columns.carrier = { description = "Two letter carrier abbreviation" }
columns.air_time = { description = "Time spent in the air", unit = "minutes" }
columns.distance = { description = "Distance between airports", unit = "miles" }
columns.hour = { description = "Hour of the scheduled departure" }
columns.minute = { description = "Minute of the scheduled departure" }

[fast-food]
name = "Food nutrition (fast food)"
url = "https://vincentarelbundock.github.io/Rdatasets/csv/openintro/fastfood.csv"
size = 44271
description = "515 menu items; nutrients per item, not per 100 g"
publisher = "OpenIntro; CSV hosted by Rdatasets"
license = "GPL-3 (OpenIntro package)"
homepage = "https://www.openintro.org/data/index.php?data=fastfood"

[baby-names]
name = "US baby names (1880-2017)"
url = "https://raw.githubusercontent.com/rfordatascience/tidytuesday/8bfa9d9a7279192cb41cab041f426f2aacefde91/data/2022/2022-03-22/babynames.csv"
size = 48788378
description = "Published name counts by year and sex; counts below five are suppressed"
publisher = "SSA / babynames; CSV hosted by TidyTuesday"
license = "CC0 / public domain"
homepage = "https://github.com/rfordatascience/tidytuesday/tree/main/data/2022/2022-03-22"

[noaa]
name = "NOAA daily weather (GHCN-D)"
url = "s3://noaa-ghcn-pds/parquet/"
auth = "anonymous"
description = "Worldwide weather station observations, by year and by station"
publisher = "NOAA"
license = "CC0"
homepage = "https://registry.opendata.aws/noaa-ghcn/"
documentation = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/readme.txt"

columns.ID = { description = "Station identification code; ghcnd-stations.txt in the bucket lists each station" }
columns.STATION = { description = "Station identification code, from the by_station partition" }
columns.YEAR = { description = "Year, from the by_year partition" }
columns.DATE = { description = "Date of the observation", unit = "YYYYMMDD" }
columns.ELEMENT.description = "Element type: what DATA_VALUE measures, and in what unit. The readme lists every code"
columns.DATA_VALUE = { description = "Data value for ELEMENT, in the unit its ELEMENT code gives; often tenths", unit = "per ELEMENT" }
columns.M_FLAG.description = "Measurement flag; blank (null) is normal: no measurement information applicable"
columns.Q_FLAG.description = "Quality flag; blank (null) is normal: the value did not fail any quality assurance check"
columns.S_FLAG.description = "Source flag: where the value came from"
columns.OBS_TIME = { description = "Time of observation, from NOAA/NCEI's station history where available (primarily U.S. Cooperative Observers); blank (null) otherwise", unit = "HHMM, 0700 = 7:00 am" }

bookmarks."Daily highs, 2024" = "by_year/YEAR=2024/ELEMENT=TMAX/"
bookmarks."Central Park, NY" = "by_station/STATION=USW00094728/"

[noaa.columns.ELEMENT.values]
PRCP = "Precipitation (tenths of mm)"
SNOW = "Snowfall (mm)"
SNWD = "Snow depth (mm)"
TMAX = "Maximum temperature (tenths of degrees C)"
TMIN = "Minimum temperature (tenths of degrees C)"
TAVG = "Average daily temperature (tenths of degrees C)"
TOBS = "Temperature at the time of observation (tenths of degrees C)"
ADPT = "Average Dew Point Temperature for the day (tenths of degrees C)"
ASLP = "Average Sea Level Pressure for the day (hPa * 10)"
AWDR = "Average daily wind direction (degrees)"
AWND = "Average daily wind speed (tenths of meters per second)"
EVAP = "Evaporation of water from evaporation pan (tenths of mm)"
MDPR = "Multiday precipitation total (tenths of mm; use with DAPR and DWPR, if available)"
DAPR = "Number of days included in the multiday precipitation total (MDPR)"
PGTM = "Peak gust time (hours and minutes, i.e., HHMM)"
PSUN = "Daily percent of possible sunshine (percent)"
RHAV = "Average relative humidity for the day (percent)"
TSUN = "Daily total sunshine (minutes)"
WDF2 = "Direction of fastest 2-minute wind (degrees)"
WDF5 = "Direction of fastest 5-second wind (degrees)"
WESD = "Water equivalent of snow on the ground (tenths of mm)"
WESF = "Water equivalent of snowfall (tenths of mm)"
WSF2 = "Fastest 2-minute wind speed (tenths of meters per second)"
WSF5 = "Fastest 5-second wind speed (tenths of meters per second)"
WSFG = "Peak gust wind speed (tenths of meters per second)"
WT01 = "Weather type: fog, ice fog, or freezing fog (may include heavy fog)"
WT03 = "Weather type: thunder"
WT08 = "Weather type: smoke or haze"
WT16 = "Weather type: rain (may include freezing rain, drizzle, and freezing drizzle)"
WT18 = "Weather type: snow, snow pellets, snow grains, or ice crystals"

[noaa.columns.M_FLAG.values]
"" = "no measurement information applicable"
B = "precipitation total formed from two 12-hour totals"
D = "precipitation total formed from four six-hour totals"
H = "represents highest or lowest hourly temperature (TMAX or TMIN) or the average of hourly values (TAVG)"
K = "converted from knots"
L = "temperature appears to be lagged with respect to reported hour of observation"
O = "converted from oktas"
P = "identified as \"missing presumed zero\" in DSI 3200 and 3206"
T = "trace of precipitation, snowfall, or snow depth"
W = "converted from 16-point WBAN code (for wind direction)"

[noaa.columns.Q_FLAG.values]
"" = "did not fail any quality assurance check"
D = "failed duplicate check"
G = "failed gap check"
I = "failed internal consistency check"
K = "failed streak/frequent-value check"
L = "failed check on length of multiday period"
M = "failed megaconsistency check"
N = "failed naught check"
O = "failed climatological outlier check"
R = "failed lagged range check"
S = "failed spatial consistency check"
T = "failed temporal consistency check"
W = "temperature too warm for snow"
X = "failed bounds check"
Z = "flagged as a result of an official Datzilla investigation"

[noaa.columns.S_FLAG.values]
"" = "No source (i.e., data value missing)"
0 = "U.S. Cooperative Summary of the Day (NCDC DSI-3200)"
1 = "CF6 (form F6) daily climate summaries from the U.S. National Weather Service"
2 = "Synoptic Summary of the Day (SSOD) \"version 2\", the successor to GSOD (source 'S')"
6 = "CDMP Cooperative Summary of the Day (NCDC DSI-3206)"
7 = "U.S. Cooperative Summary of the Day -- Transmitted via WxCoder3 (NCDC DSI-3207)"
A = "U.S. Automated Surface Observing System (ASOS) real-time data (since January 1, 2006)"
a = "Australian data from the Australian Bureau of Meteorology"
B = "U.S. ASOS data for October 2000-December 2005 (NCDC DSI-3211)"
b = "Belarus update"
C = "Environment Canada"
D = "Short time delay US National Weather Service CF6 daily summaries provided by the High Plains Regional Climate Center"
d = "Short time delay US National Weather Service Daily Summary Message (DSMs) provided by the High Plains Regional Climate Center"
E = "European Climate Assessment and Dataset (Klein Tank et al., 2002)"
F = "U.S. Fort data"
G = "Official Global Climate Observing System (GCOS) or other government-supplied data"
H = "High Plains Regional Climate Center real-time data"
I = "International collection (non U.S. data received through personal contacts)"
K = "U.S. Cooperative Summary of the Day data digitized from paper observer forms (from 2011 to present)"
M = "Monthly METAR Extract (additional ASOS data)"
f = "Data provided courtesy of the Fiji Met Service"
m = "Data from the Mexican National Water Commission (Comision National del Agua -- CONAGUA)"
N = "Community Collaborative Rain, Hail,and Snow (CoCoRaHS)"
Q = "Data from several African countries that had been \"quarantined\", that is, withheld from public release until permission was granted from the respective meteorological services"
R = "NCEI Reference Network Database (Climate Reference Network and Regional Climate Reference Network)"
r = "All-Russian Research Institute of Hydrometeorological Information-World Data Center"
S = "Global Summary of the Day (NCDC DSI-9618); use with caution, particularly for precipitation"
s = "China Meteorological Administration/National Meteorological Information Center/Climatic Data Center"
T = "SNOwpack TELemtry (SNOTEL) data obtained from the U.S. Department of Agriculture's Natural Resources Conservation Service"
U = "Remote Automatic Weather Station (RAWS) data obtained from the Western Regional Climate Center"
u = "Ukraine update"
W = "WBAN/ASOS Summary of the Day from NCDC's Integrated Surface Data (ISD)"
X = "U.S. First-Order Summary of the Day (NCDC DSI-3210)"
Z = "Datzilla official additions or replacements"
z = "Uzbekistan update"

[premier-league]
name = "Premier League (2020-21)"
url = "https://raw.githubusercontent.com/footballcsv/england/de3945297668d7114006a8ca1c4c3740010b111c/2020s/2020-21/eng.1.csv"
size = 17834
description = "Match rounds, dates, teams and full-time scores"
publisher = "OpenFootball / football.csv"
license = "CC0"
homepage = "https://github.com/footballcsv/england"

[nyc-taxis]
name = "NYC yellow taxis (January 2025)"
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
size = 59158238
description = "One original monthly trip file; fares, distances and congestion fees"
publisher = "NYC Taxi and Limousine Commission"
license = "NYC Open Data terms"
homepage = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
documentation = "https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf"

columns.VendorID.description = "The TPEP provider that provided the record"
columns.tpep_pickup_datetime = { description = "When the meter was engaged" }
columns.tpep_dropoff_datetime = { description = "When the meter was disengaged" }
columns.trip_distance = { description = "Elapsed trip distance reported by the taximeter", unit = "miles" }
columns.RatecodeID.description = "The final rate code in effect at the end of the trip"
columns.store_and_fwd_flag.description = "Whether the trip record was held in vehicle memory before sending to the vendor, because the vehicle had no connection to the server"
columns.PULocationID = { description = "TLC Taxi Zone in which the taximeter was engaged" }
columns.DOLocationID = { description = "TLC Taxi Zone in which the taximeter was disengaged" }
columns.payment_type.description = "How the passenger paid for the trip"
columns.fare_amount = { description = "The time-and-distance fare calculated by the meter" }
columns.extra = { description = "Miscellaneous extras and surcharges" }
columns.mta_tax = { description = "Tax that is automatically triggered based on the metered rate in use" }
columns.tip_amount = { description = "Tip amount, populated automatically for credit card tips; cash tips are not included" }
columns.tolls_amount = { description = "Total amount of all tolls paid in trip" }
columns.improvement_surcharge = { description = "Improvement surcharge assessed trips at the flag drop, levied since 2015" }
columns.total_amount = { description = "The total amount charged to passengers; does not include cash tips" }
columns.congestion_surcharge = { description = "Total amount collected in trip for NYS congestion surcharge" }
columns.Airport_fee = { description = "For pick up only at LaGuardia and John F. Kennedy Airports" }
columns.cbd_congestion_fee = { description = "Per-trip charge for MTA's Congestion Relief Zone, starting Jan. 5, 2025" }

[nyc-taxis.columns.VendorID.values]
1 = "Creative Mobile Technologies, LLC"
2 = "Curb Mobility, LLC"
6 = "Myle Technologies Inc"
7 = "Helix"

[nyc-taxis.columns.RatecodeID.values]
1 = "Standard rate"
2 = "JFK"
3 = "Newark"
4 = "Nassau or Westchester"
5 = "Negotiated fare"
6 = "Group ride"
99 = "Null/unknown"

[nyc-taxis.columns.store_and_fwd_flag.values]
Y = "store and forward trip"
N = "not a store and forward trip"

[nyc-taxis.columns.payment_type.values]
0 = "Flex Fare trip"
1 = "Credit card"
2 = "Cash"
3 = "No charge"
4 = "Dispute"
5 = "Unknown"
6 = "Voided trip"

[earthquakes]
name = "Earthquakes (past month)"
url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv"
size = 2153157
description = "Rolling month of worldwide earthquakes; magnitudes, depth and location"
publisher = "USGS"
license = "Public domain"
homepage = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/csv.php"
documentation = "https://www.usgs.gov/programs/earthquake-hazards/magnitude-types"

columns.magType.description = "How mag was measured: the magnitude type"

[earthquakes.columns.magType.values]
mww = "Moment W-phase: from a centroid moment tensor inversion of the W-phase; about 5.0 and larger"
mwc = "Centroid: from a centroid moment tensor inversion of the long-period surface waves; about 5.5 and larger"
mwb = "Body wave: from moment tensor inversion of long-period body waves (P- and SH); about 5.5 to 7.0"
mwr = "Regional: from moment tensor inversion of the whole seismogram at regional distances; about 4.0 to 6.5"
mb = "Short-period body wave: from the amplitude of 1st arriving P-waves at periods of about 1 s; about 4.0 to 6.5"
mfa = "Felt-area magnitude: an estimate of mb from the size of the area over which the earthquake was felt"
ml = "Local: the original magnitude relationship defined by Richter and Gutenberg in 1935 for local earthquakes; about 2.0 to 6.5"
mb_lg = "Short-period surface wave: from the amplitude of the Lg surface waves, for regional earthquakes; about 3.5 to 7.0"
md = "Duration: from the duration of shaking as measured by the time decay of the amplitude; about 4 or smaller"
me = "Energy: from the seismic energy radiated by the earthquake; about 3.5 and larger"
mi = "Integrated p-wave: from the integral of the displacement of the P wave on broadband instruments; about 5.0 to 8.0"
mwp = "Integrated p-wave: from the integral of the displacement of the P wave on broadband instruments; about 5.0 to 8.0"
mh = "Non-standard magnitude method, generally used when standard methods will not work"
mint = "Intensity magnitude: estimated from the maximum reported intensity"

[space-launches]
name = "Space launches (1957-2018)"
url = "https://raw.githubusercontent.com/rfordatascience/tidytuesday/4557eb755d1a6a6f21bbf6d9009e21e363167c4c/data/2019/2019-01-15/launches.csv"
size = 430817
description = "Historical launch records and agencies; includes failed attempts"
publisher = "Jonathan McDowell / The Economist; CSV hosted by TidyTuesday"
license = "MIT (The Economist extract); credit Jonathan McDowell"
homepage = "https://github.com/TheEconomist/graphic-detail-data/tree/master/data/2018-10-20_space-launches"

[penguins]
name = "Palmer penguins"
url = "https://vincentarelbundock.github.io/Rdatasets/csv/palmerpenguins/penguins.csv"
size = 16480
description = "344 penguins: species, island, bill, flipper length and body mass"
publisher = "Palmer Station LTER / palmerpenguins; CSV hosted by Rdatasets"
license = "CC0; credit Horst, Hill and Gorman (2020)"
homepage = "https://allisonhorst.github.io/palmerpenguins/"

[solubility]
name = "Aqueous solubility (SDF)"
url = "https://raw.githubusercontent.com/rdkit/rdkit/bfc98b529561d11e4a20a64f272c5f6900393cb2/Docs/Book/data/solubility.train.sdf"
size = 1376487
description = "1,025 molecules: measured solubility (log mol/L), a low, medium or high class, and SMILES"
publisher = "Huuskonen (2000); SDF from the RDKit book's data"
license = "BSD-3-Clause (RDKit)"
homepage = "https://github.com/rdkit/rdkit/tree/master/Docs/Book/data"

[blockchain]
name = "Bitcoin and Ethereum"
url = "s3://aws-public-blockchain/v1.0/"
auth = "anonymous"
description = "Blocks and transactions, partitioned by date"
publisher = "AWS Public Blockchain Data"
license = "AWS sample-code license"
homepage = "https://registry.opendata.aws/aws-public-blockchain/"

[overture]
name = "Overture Maps"
url = "abfss://release@overturemapswestus2.dfs.core.windows.net/"
auth = "anonymous"
description = "Places, buildings, addresses, roads and boundaries, by release"
publisher = "Overture Maps Foundation"
license = "ODbL; places CDLA Permissive 2.0 and Apache 2.0"
homepage = "https://docs.overturemaps.org"
```
<!-- end generated: public-catalog -->
