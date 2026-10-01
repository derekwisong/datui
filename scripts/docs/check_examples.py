#!/usr/bin/env python3
"""Recheck the numbers the docs quote from the built-in public datasets.

Each check runs a documented query through Polars SQL, the engine datui's SQL
mode uses, on a frozen dataset from the catalog as its publisher serves it, and
compares the result with the text. Rolling datasets (earthquakes, Bitcoin) and
the S3 ones are left to the manual checklist in
docs/for-developers/examples.md, as is everything a key does on screen.

Needs the network on the first run; files are kept in --cache afterward.
Not part of CI.

Usage:
    .venv/bin/python scripts/docs/check_examples.py [--cache DIR] [-k NAME]
"""

from __future__ import annotations

import argparse
import sys
import urllib.request
from pathlib import Path

try:
    import polars as pl
except ImportError:
    print("Error: polars is required; use the project's .venv", file=sys.stderr)
    sys.exit(2)

DATASETS = {
    "penguins": "https://vincentarelbundock.github.io/Rdatasets/csv/palmerpenguins/penguins.csv",
    "flights": "https://vincentarelbundock.github.io/Rdatasets/csv/nycflights13/flights.csv",
    "food": "https://vincentarelbundock.github.io/Rdatasets/csv/openintro/fastfood.csv",
    "names": "https://raw.githubusercontent.com/rfordatascience/tidytuesday/main/data/2022/2022-03-22/babynames.csv",
    "football": "https://raw.githubusercontent.com/footballcsv/england/master/2020s/2020-21/eng.1.csv",
    "launches": "https://raw.githubusercontent.com/rfordatascience/tidytuesday/main/data/2019/2019-01-15/launches.csv",
    "taxis": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet",
}


def fetch(name: str, cache: Path) -> Path:
    url = DATASETS[name]
    path = cache / url.rsplit("/", 1)[1]
    if not path.exists():
        print(f"  downloading {url}")
        tmp = path.with_suffix(path.suffix + ".part")
        urllib.request.urlretrieve(url, tmp)
        tmp.rename(path)
    return path


def frame(name: str, cache: Path) -> pl.LazyFrame:
    path = fetch(name, cache)
    if path.suffix == ".parquet":
        return pl.scan_parquet(path)
    # datui reads ISO timestamps as datetimes, as try_parse_dates does.
    return pl.scan_csv(path, try_parse_dates=True, infer_schema_length=10000)


def sql(lf: pl.LazyFrame, query: str) -> pl.DataFrame:
    return pl.SQLContext(df=lf).execute(query).collect()


def close(a: float, b: float, places: int) -> bool:
    return round(a, places) == round(b, places)


def penguins_species(c: Path) -> list[str]:
    df = sql(
        frame("penguins", c),
        "SELECT species, AVG(body_mass_g) AS mean_mass_g, COUNT(*) AS penguins "
        "FROM df GROUP BY species ORDER BY mean_mass_g DESC",
    )
    want = [("Gentoo", 5076.01626, 124), ("Chinstrap", 3733.088235, 68), ("Adelie", 3700.662252, 152)]
    got = [(r[0], r[1], r[2]) for r in df.iter_rows()]
    ok = len(got) == len(want) and all(
        g[0] == w[0] and close(g[1], w[1], 5) and g[2] == w[2] for g, w in zip(got, want)
    )
    return [] if ok else [str(got)]


def penguins_counts(c: Path) -> list[str]:
    lf = frame("penguins", c)
    counts = dict(lf.group_by("species").len().collect().iter_rows())
    corr = lf.select(pl.corr("flipper_length_mm", "body_mass_g")).collect().item()
    pairs = lf.drop_nulls(["flipper_length_mm", "body_mass_g"]).select(pl.len()).collect().item()
    errs = []
    if counts != {"Adelie": 152, "Gentoo": 124, "Chinstrap": 68}:
        errs.append(f"counts {counts}")
    if not close(corr, 0.871, 3) or pairs != 342:
        errs.append(f"r {corr} over {pairs}")
    return errs


def flights_jfk(c: Path) -> list[str]:
    df = sql(
        frame("flights", c),
        "SELECT hour, AVG(dep_delay) AS mean_delay, COUNT(dep_delay) AS flights "
        "FROM df WHERE origin = 'JFK' GROUP BY hour ORDER BY hour",
    )
    first, last = df.row(0), df.filter(pl.col("hour") == 21).row(0)
    ok = df.height == 19 and first[0] == 5 and close(first[1], 0.5, 1) and close(last[1], 26.1, 1)
    return [] if ok else [str(df)]


def flights_carriers(c: Path) -> list[str]:
    lf = frame("flights", c)
    df = sql(
        lf,
        "SELECT carrier, AVG(arr_delay) AS delay, COUNT(*) AS flights "
        "FROM df GROUP BY carrier ORDER BY delay DESC",
    )
    top, bottom = df.row(0), df.row(-1)
    routes = lf.filter(pl.col("carrier") == "AS").group_by("origin", "dest").len().collect()
    ok = (
        df.height == 16
        and top[0] == "F9" and close(top[1], 21.92, 2)
        and bottom[0] == "AS" and close(bottom[1], -9.93, 2) and bottom[2] == 714
        and routes.rows() == [("EWR", "SEA", 714)]
    )
    jfk_sea = lf.filter(pl.col("origin") == "JFK", pl.col("dest") == "SEA").select(pl.len()).collect().item()
    errs = [] if ok else [str(df), str(routes)]
    if jfk_sea != 2092:
        errs.append(f"jfk sea {jfk_sea}")
    return errs


def flights_days(c: Path) -> list[str]:
    df = sql(
        frame("flights", c),
        "SELECT DATE(CONCAT_WS('-', year, month, day)) AS flight_date, "
        "COUNT(*) AS flights, AVG(dep_delay) AS delay "
        "FROM df GROUP BY flight_date ORDER BY flight_date",
    )
    worst = df.sort("delay", descending=True).row(0)
    ok = df.height == 365 and str(worst[0]) == "2013-03-08" and close(worst[2], 83.54, 2)
    return [] if ok else [str(df.height), str(worst)]


def food(c: Path) -> list[str]:
    lf = frame("food", c)
    df = sql(
        lf,
        "SELECT restaurant, ROUND(AVG(calories), 0) AS avg_calories, "
        "ROUND(AVG(protein), 1) AS avg_protein, COUNT(*) AS items "
        "FROM df GROUP BY restaurant ORDER BY avg_calories DESC",
    )
    want = [
        ("Mcdonalds", 640.0, 40.3, 57), ("Sonic", 632.0, 29.2, 53), ("Burger King", 609.0, 30.0, 70),
        ("Arbys", 533.0, 29.3, 55), ("Dairy Queen", 520.0, 24.8, 42), ("Subway", 503.0, 30.3, 96),
        ("Taco Bell", 444.0, 17.4, 115), ("Chick Fil-A", 384.0, 31.7, 27),
    ]
    errs = [] if df.rows() == want else [str(df)]

    def fuzzy(word: str) -> pl.Expr:
        pattern = ".*".join(word)
        return pl.any_horizontal(pl.col(pl.String).str.to_lowercase().str.contains(pattern))

    chicken = lf.filter(fuzzy("chicken"))
    n_chicken = chicken.select(pl.len()).collect().item()
    n_chkn = lf.filter(fuzzy("chkn")).select(pl.len()).collect().item()
    heavy = chicken.filter(pl.col("protein") >= 40).sort("calories", descending=True).collect()
    if (n_chicken, n_chkn, heavy.height) != (178, 186, 30):
        errs.append(f"search {n_chicken} {n_chkn} {heavy.height}")
    if heavy.row(0, named=True)["item"] != "20 piece Buttermilk Crispy Chicken Tenders" or heavy["calories"][0] != 2430:
        errs.append(str(heavy.head(1)))
    nulls = lf.select(
        vit_a=pl.col("vit_a").null_count(),
        both=(pl.col("vit_c").is_null() & pl.col("calcium").is_null()).sum(),
    ).collect().row(0)
    if nulls != (214, 210):
        errs.append(f"nulls {nulls}")
    return errs


def names(c: Path) -> list[str]:
    lf = frame("names", c)
    three = sql(lf, "SELECT year, name, n FROM df WHERE sex = 'F' AND name IN ('Emma', 'Jennifer', 'Olivia')")
    wide = three.pivot(index="year", on="name", values="n", aggregate_function="last")
    jennifer_nulls = wide["Jennifer"].null_count()
    peak = wide.sort("Jennifer", descending=True, nulls_last=True).row(0, named=True)
    totals = dict(
        sql(lf, "SELECT name, SUM(n) AS total FROM df WHERE name IN ('Emma', 'Jennifer', 'Olivia') GROUP BY name").rows()
    )
    errs = []
    if (three.height, wide.height, jennifer_nulls) != (376, 138, 38):
        errs.append(f"{three.height} rows, {wide.height} years, {jennifer_nulls} nulls")
    if (peak["year"], peak["Jennifer"]) != (1972, 63604):
        errs.append(f"peak {peak}")
    if totals != {"Emma": 655629, "Jennifer": 1471118, "Olivia": 435016}:
        errs.append(f"totals {totals}")
    return errs


def football(c: Path) -> list[str]:
    lf = frame("football", c)
    df = sql(
        lf,
        "SELECT Round, CAST(STRPTIME(SUBSTR(Date, 1, 15), '%a %b %d %Y') AS DATE) AS match_date, "
        "\"Team 1\" AS home, \"Team 2\" AS away, "
        "CAST(SPLIT_PART(FT, '–', 1) AS INT) + CAST(SPLIT_PART(FT, '–', 2) AS INT) AS goals "
        "FROM df ORDER BY goals DESC, match_date, home",
    )
    head = [(str(r[1]), r[2], r[3], r[4]) for r in df.head(3).iter_rows()]
    want = [
        ("2020-10-04", "Aston Villa", "Liverpool", 9),
        ("2021-02-02", "Manchester Utd", "Southampton", 9),
        ("2020-12-20", "Manchester Utd", "Leeds United", 8),
    ]
    postponed = lf.filter(pl.col("Date").str.ends_with("(P)")).select(pl.len()).collect().item()
    errs = [] if df.height == 380 and head == want else [str(df.head(3))]
    if postponed != 12:
        errs.append(f"postponed {postponed}")
    return errs


def launches(c: Path) -> list[str]:
    df = frame("launches", c).select("launch_year", "category", "tag").collect()
    wide = df.pivot(index="launch_year", on="category", values="tag", aggregate_function="len")
    y1967 = wide.filter(pl.col("launch_year") == 1967).row(0, named=True)
    ok = wide.height == 62 and (y1967["O"], y1967["F"]) == (127, 12) and wide["O"].max() == 129
    return [] if ok else [str(wide.height), str(y1967)]


def taxis(c: Path) -> list[str]:
    lf = frame("taxis", c)
    df = sql(
        lf,
        "SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) AS pickup_hour, COUNT(*) AS trips, "
        "AVG(tip_amount) AS avg_tip, AVG(fare_amount) AS avg_fare "
        "FROM df GROUP BY pickup_hour ORDER BY pickup_hour",
    )
    busiest = df.sort("trips", descending=True).row(0)
    quietest = df.sort("trips").row(0)
    rows = lf.select(pl.len()).collect().item()
    ok = df.height == 24 and busiest[:2] == (18, 267951) and quietest[:2] == (4, 20033) and rows == 3475226
    return [] if ok else [str(busiest), str(quietest), str(rows)]


CHECKS = {
    "penguins-species": penguins_species,
    "penguins-counts": penguins_counts,
    "flights-jfk": flights_jfk,
    "flights-carriers": flights_carriers,
    "flights-days": flights_days,
    "food": food,
    "names": names,
    "football": football,
    "launches": launches,
    "taxis": taxis,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--cache", type=Path, default=Path.home() / ".cache" / "datui-doc-examples")
    parser.add_argument("-k", dest="only", help="run the checks whose name contains this")
    args = parser.parse_args()
    args.cache.mkdir(parents=True, exist_ok=True)

    failed = 0
    for name, check in CHECKS.items():
        if args.only and args.only not in name:
            continue
        errs = check(args.cache)
        print(f"{'ok  ' if not errs else 'FAIL'} {name}")
        for err in errs:
            print(f"     {err}")
        failed += bool(errs)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
