"""Bounded official NYC TLC HVFHV source, taxi zones and observed weather.

A real high-volume for-hire trip dataset is NOT an Uber-internal dataset.
Run from a network-enabled laptop, not a restricted Databricks Free notebook.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from datetime import date
from pathlib import Path
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

CLOUDFRONT = "d37ci6vzurychx.cloudfront.net"
ZONE_URL = f"https://{CLOUDFRONT}/misc/taxi_zone_lookup.csv"
ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
ALLOWED_HOSTS = {CLOUDFRONT, "archive-api.open-meteo.com"}

def trip_url(year: int, month: int) -> str:
    if not 2019 <= year <= 2026 or not 1 <= month <= 12:
        raise ValueError("HVFHV year 2019..2026 and month 1..12")
    if (year, month) > (2026, 9):
        # Official TLC publication lags trips; update after confirming availability.
        raise ValueError("Select a published monthly TLC parquet; cap currently 2026-09")
    return f"https://{CLOUDFRONT}/trip-data/fhvhv_tripdata_{year}-{month:02d}.parquet"

def weather_url(year: int, month: int) -> str:
    date(year, month, 1)
    import calendar
    end = calendar.monthrange(year, month)[1]
    return ARCHIVE + "?" + urlencode({
        "latitude": "40.7128", "longitude": "-74.0060",
        "start_date": f"{year}-{month:02d}-01",
        "end_date": f"{year}-{month:02d}-{end:02d}",
        "daily": "precipitation_sum,temperature_2m_max,temperature_2m_min",
        "timezone": "America/New_York"
    })

def download(url, target, *, max_bytes=100 * 1024 * 1024, opener=urlopen):
    """Stream with size guard, hash verification and atomic file replacement."""
    if urlparse(url).scheme != "https" or urlparse(url).hostname not in ALLOWED_HOSTS:
        raise ValueError("Unapproved source domain")
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    part = target.with_name(target.name + ".partial")
    request = Request(url, headers={"User-Agent": "PortfolioDE/1.0 (public research)"})
    sha = hashlib.sha256()
    size = 0
    try:
        with opener(request, timeout=60) as response:
            if urlparse(response.geturl()).hostname not in ALLOWED_HOSTS:
                raise ValueError("Download redirected away from approved host")
            length = response.headers.get("Content-Length")
            if length and int(length) > max_bytes:
                raise ValueError("File exceeds configured transfer cap; confirm size and budget")
            with part.open("wb") as out:
                while chunk := response.read(1024 * 1024):
                    size += len(chunk)
                    if size > max_bytes:
                        raise ValueError("Source exceeds configured size cap")
                    out.write(chunk)
                    sha.update(chunk)
        os.replace(part, target)
    except Exception:
        part.unlink(missing_ok=True)
        raise
    return {"url": url, "filename": target.name, "bytes": size,
            "sha256": sha.hexdigest()}

def fetch_bundle(output, year=2025, month=1, *, max_megabytes=100,
                 confirm_large=False, downloader=download):
    if max_megabytes > 100 and not confirm_large:
        raise ValueError("Large downloads require --confirm-large")
    root = Path(output)
    root.mkdir(parents=True, exist_ok=True)
    previous_file = root / "manifest.json"
    if previous_file.is_file():
        previous = json.loads(previous_file.read_text(encoding="utf-8"))
        if (previous.get("year"), previous.get("month")) != (year, month):
            raise ValueError("This landing directory belongs to another month; use a distinct output path")
        validate_manifest(root)
        return previous
    datasets = {
        "trips": (trip_url(year, month), root / f"fhvhv_tripdata_{year}-{month:02d}.parquet"),
        "zones": (ZONE_URL, root / "taxi_zone_lookup.csv"),
        "weather": (weather_url(year, month), root / f"nyc_weather_{year}-{month:02d}.json")
    }
    sources = {}
    for name, (url, dest) in datasets.items():
        sources[name] = downloader(url, dest, max_bytes=max_megabytes * 1024 * 1024)
    manifest = {"year": year, "month": month, "source_provider": "NYC TLC + Open-Meteo",
                "data_classification": "public-aggregated-only-in-outputs",
                "sources": sources}
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest

def validate_manifest(root):
    root = Path(root)
    manifest = json.loads((root / "manifest.json").read_text())
    for name, source in manifest["sources"].items():
        file = root / source["filename"]
        if not file.is_file():
            raise FileNotFoundError(file)
        if file.stat().st_size != source["bytes"]:
            raise ValueError(f"Size mismatch for {name}")
        sha = hashlib.sha256()
        with file.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                sha.update(chunk)
        if sha.hexdigest() != source["sha256"]:
            raise ValueError(f"Checksum mismatch for {name}")
    return True

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="real-data/tlc")
    p.add_argument("--year", type=int, default=2025)
    p.add_argument("--month", type=int, default=1)
    p.add_argument("--max-mb", type=int, default=100)
    p.add_argument("--confirm-large", action="store_true")
    a = p.parse_args()
    print(json.dumps(fetch_bundle(a.out, a.year, a.month,
                                  max_megabytes=a.max_mb,
                                  confirm_large=a.confirm_large), indent=2))
    assert validate_manifest(a.out)
