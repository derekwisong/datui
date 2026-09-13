#!/usr/bin/env python3
"""
Build the demo datasets under demo/data from their sources.

Every public dataset is downloaded from the URL recorded in DATA-LICENSES.md,
cleaned the same way every time, and written as zstd Parquet with a fixed row
order, so a rebuild changes nothing unless the source did. The two private
snapshots (the quant-research extract and the bitcoin chain statistics) are
committed as-is and only copied here when a source directory is given.

    python demo/build.py                  # rebuild the public datasets
    python demo/build.py --only quakes    # one of them
    python demo/build.py --cache ~/tmp    # keep the downloads somewhere else

Needs polars. The SSA baby-names archive refuses scripted downloads; fetch
names.zip in a browser from https://www.ssa.gov/oact/babynames/limits.html and
pass it with --names-zip, or the build falls back to the CC0 `babynames` R
package's copy (1880-2017) when pyreadr is installed.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import shutil
import sys
import urllib.request
import zipfile
from pathlib import Path

import polars as pl

HERE = Path(__file__).resolve().parent
DATA = HERE / "data"

SOURCES = {
    "penguins": "https://raw.githubusercontent.com/allisonhorst/palmerpenguins/main/inst/extdata/penguins.csv",
    "gapminder": "https://raw.githubusercontent.com/jennybc/gapminder/main/inst/extdata/gapminder.tsv",
    "quakes": (
        "https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv"
        "&starttime=1900-01-01&endtime=2026-09-13&minmagnitude=6&orderby=time-asc"
    ),
    "weather": "https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_station/USW00094728.csv.gz",
    "meteorites": "https://data.nasa.gov/docs/legacy/meteorite_landings/Meteorite_Landings.csv",
    "launches": "https://planet4589.org/space/gcat/tsv/launch/launch.tsv",
    "babynames_rda": "https://raw.githubusercontent.com/hadley/babynames/master/data/babynames.rda",
}


def fetch(name: str, cache: Path) -> Path:
    url = SOURCES[name]
    suffix = Path(url.split("?")[0]).suffix or ".csv"
    target = cache / (name + suffix)
    if not target.exists():
        print(f"  fetching {url}")
        req = urllib.request.Request(url, headers={"User-Agent": "datui-demo-build"})
        with urllib.request.urlopen(req, timeout=120) as resp, open(target, "wb") as out:
            shutil.copyfileobj(resp, out)
    return target


def write(df: pl.DataFrame, rel: str) -> None:
    out = DATA / rel
    out.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out, compression="zstd", statistics=True)
    size = out.stat().st_size
    print(f"  {rel}: {df.height:,} rows x {df.width}, {size / 1024:.0f} KB")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()[:16]


# ---------------------------------------------------------------------------


def build_penguins(cache: Path) -> None:
    src = fetch("penguins", cache)
    df = pl.read_csv(src, null_values=["NA"])
    write(df, "penguins.parquet")


def build_gapminder(cache: Path) -> None:
    src = fetch("gapminder", cache)
    df = pl.read_csv(src, separator="\t")
    df = df.rename({"lifeExp": "life_exp", "pop": "population", "gdpPercap": "gdp_per_capita"})
    write(df, "gapminder.parquet")


def build_quakes(cache: Path) -> None:
    src = fetch("quakes", cache)
    df = pl.read_csv(
        src,
        infer_schema_length=20000,
        schema_overrides={"depth": pl.Float64, "mag": pl.Float64, "nst": pl.Float64, "magNst": pl.Float64},
    )
    df = (
        df.select(
            pl.col("time").str.to_datetime("%Y-%m-%dT%H:%M:%S%.fZ", time_zone="UTC"),
            pl.col("latitude"),
            pl.col("longitude"),
            pl.col("depth").alias("depth_km"),
            pl.col("mag").alias("magnitude"),
            pl.col("magType").alias("mag_type"),
            pl.col("place"),
            pl.col("type"),
            pl.col("nst").cast(pl.Int32).alias("stations"),
            pl.col("gap").alias("azimuthal_gap"),
            pl.col("rms"),
            pl.col("net").alias("network"),
            pl.col("status"),
            pl.col("id"),
        )
        .sort("time")
    )
    write(df, "earthquakes_m6.parquet")


def build_weather(cache: Path) -> None:
    src = fetch("weather", cache)
    with gzip.open(src, "rb") as fh:
        raw = fh.read()
    long = pl.read_csv(
        io.BytesIO(raw),
        has_header=False,
        new_columns=["station", "date", "element", "value", "mflag", "qflag", "sflag", "obs_time"],
        schema_overrides={"date": pl.String, "value": pl.Int64},
    )
    long = long.filter(pl.col("element").is_in(["TMAX", "TMIN", "PRCP", "SNOW", "SNWD"]))
    long = long.with_columns(pl.col("date").str.to_date("%Y%m%d"))
    wide = long.pivot(on="element", index="date", values="value", aggregate_function="first").sort("date")
    # GHCN stores tenths of a degree and tenths of a millimetre; SNOW and SNWD are millimetres.
    wide = wide.select(
        "date",
        (pl.col("TMAX") / 10).alias("tmax_c"),
        (pl.col("TMIN") / 10).alias("tmin_c"),
        (pl.col("PRCP") / 10).alias("precip_mm"),
        pl.col("SNOW").alias("snow_mm"),
        pl.col("SNWD").alias("snow_depth_mm"),
    )
    write(wide, "central_park_weather.parquet")


def build_meteorites(cache: Path) -> None:
    src = fetch("meteorites", cache)
    df = pl.read_csv(src, schema_overrides={"year": pl.Float64})
    df = (
        df.select(
            pl.col("name"),
            pl.col("id").cast(pl.Int32),
            pl.col("nametype").alias("name_type"),
            pl.col("recclass").alias("class"),
            pl.col("mass (g)").alias("mass_g"),
            pl.col("fall"),
            pl.col("year").cast(pl.Int32),
            pl.col("reclat").alias("latitude"),
            pl.col("reclong").alias("longitude"),
        )
        .sort(["year", "name"], nulls_last=True)
    )
    write(df, "meteorites.parquet")


def build_launches(cache: Path) -> None:
    src = fetch("launches", cache)
    text = src.read_text(encoding="utf-8", errors="replace").splitlines()
    header = text[0].lstrip("#").split("\t")
    rows = [line for line in text[1:] if not line.startswith("#")]
    df = pl.read_csv(
        io.StringIO("\t".join(header) + "\n" + "\n".join(rows)),
        separator="\t",
        has_header=True,
        infer_schema_length=0,
        quote_char=None,
    )
    df = df.select([pl.col(c).str.strip_chars() for c in df.columns])
    df = df.filter(pl.col("LaunchCode").str.starts_with("O"))
    year = pl.col("Launch_Date").str.slice(0, 4).cast(pl.Int32, strict=False)
    num = lambda c: pl.col(c).replace("-", None).cast(pl.Float64, strict=False)  # noqa: E731
    df = (
        df.select(
            pl.col("Launch_Tag").alias("launch_tag"),
            pl.col("Launch_Date").alias("launch_date"),
            year.alias("year"),
            pl.col("LV_Type").alias("vehicle"),
            pl.col("Variant").replace("-", None).alias("variant"),
            pl.col("Agency").alias("agency"),
            pl.col("Launch_Site").alias("site"),
            pl.col("Launch_Pad").replace("-", None).alias("pad"),
            pl.col("Mission").replace("-", None).alias("mission"),
            pl.col("LaunchCode").alias("launch_code"),
            pl.when(pl.col("LaunchCode").str.slice(1, 1) == "S").then(True).when(pl.col("LaunchCode").str.slice(1, 1) == "F").then(False).otherwise(None).alias("success"),
            num("Apogee").alias("apogee_km"),
            num("OrbMass").alias("orbital_mass_kg"),
            pl.col("Category").replace("-", None).alias("category"),
        )
        .sort("launch_tag")
    )
    write(df, "space_launches.parquet")


def build_babynames(cache: Path, names_zip: Path | None) -> None:
    if names_zip and names_zip.exists():
        frames = []
        with zipfile.ZipFile(names_zip) as zf:
            for member in sorted(zf.namelist()):
                if not member.startswith("yob"):
                    continue
                y = int(member[3:7])
                part = pl.read_csv(zf.read(member), has_header=False, new_columns=["name", "sex", "count"])
                frames.append(part.with_columns(pl.lit(y).cast(pl.Int32).alias("year")))
        df = pl.concat(frames)
        source = "SSA names.zip"
    else:
        try:
            import pyreadr  # type: ignore
        except ImportError:
            print("  babynames: no names.zip and pyreadr not installed; skipped")
            return
        src = fetch("babynames_rda", cache)
        pdf = pyreadr.read_r(str(src))["babynames"]
        df = pl.from_pandas(pdf).select(
            pl.col("year").cast(pl.Int32), "sex", "name", pl.col("n").cast(pl.Int64).alias("count")
        )
        source = "babynames R package (CC0), 1880-2017"
    # The top 1,000 names per sex and year keeps every name anyone will search
    # for and the files under two megabytes. Rank ties break by name.
    df = (
        df.sort(["year", "sex", "count", "name"], descending=[False, False, True, False])
        .with_columns(pl.int_range(pl.len()).over(["year", "sex"]).alias("rank") + 1)
        .filter(pl.col("rank") <= 1000)
        .with_columns(pl.col("rank").cast(pl.Int32))
    )
    print(f"  babynames from {source}")
    for (y,), part in df.group_by("year", maintain_order=True):
        write(part.drop("year").sort(["sex", "rank"]), f"babynames/year={y}/part-0.parquet")


def copy_private(src_dir: Path) -> None:
    """Copy the snapshots that cannot be rebuilt from a public source."""
    for name in ("quant-research", "bitcoin_daily.parquet", "fred"):
        src = src_dir / name
        dst = DATA / name
        if not src.exists():
            print(f"  private: {src} not found, skipped")
            continue
        if dst.exists():
            shutil.rmtree(dst) if dst.is_dir() else dst.unlink()
        if src.is_dir():
            shutil.copytree(src, dst)
        else:
            shutil.copy2(src, dst)
        print(f"  private: {name} copied")


BUILDERS = {
    "penguins": build_penguins,
    "gapminder": build_gapminder,
    "quakes": build_quakes,
    "weather": build_weather,
    "meteorites": build_meteorites,
    "launches": build_launches,
}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--only", action="append", help="build one dataset (repeatable)")
    ap.add_argument("--cache", type=Path, default=HERE / ".cache", help="where downloads are kept")
    ap.add_argument("--names-zip", type=Path, help="the SSA names.zip, downloaded by hand")
    ap.add_argument("--private", type=Path, help="directory holding the private snapshots to copy in")
    args = ap.parse_args()
    args.cache.mkdir(parents=True, exist_ok=True)
    DATA.mkdir(parents=True, exist_ok=True)

    wanted = set(args.only or list(BUILDERS) + ["babynames"])
    for name, fn in BUILDERS.items():
        if name in wanted:
            print(f"{name}:")
            fn(args.cache)
    if "babynames" in wanted:
        print("babynames:")
        build_babynames(args.cache, args.names_zip)
    if args.private:
        print("private snapshots:")
        copy_private(args.private)
    print("done")


if __name__ == "__main__":
    sys.exit(main())
