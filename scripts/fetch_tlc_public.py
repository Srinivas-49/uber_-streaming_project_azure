"""Download official NYC TLC monthly Parquet and taxi zone reference files.

Default mode only prints a plan. Actual downloads require --download and a
per-file size cap. Files are stored locally, never committed to GitHub.
Official source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen

BASE = "https://d37ci6vzurychx.cloudfront.net"
KINDS = frozenset({"fhvhv", "yellow", "green"})
ZONE_URL = BASE + "/misc/taxi_zone_lookup.csv"

def plan(months, kinds=("fhvhv",), include_zones=True):
    if not months or len(months) > 12:
        raise ValueError("Specify 1 to 12 months per bounded run")
    if not kinds or any(kind not in KINDS for kind in kinds):
        raise ValueError("Allowed types: fhvhv, yellow, green")
    items = []
    for month in months:
        try:
            year, number = map(int, month.split("-"))
        except (ValueError, AttributeError):
            raise ValueError("Expected YYYY-MM") from None
        if not (2019 <= year <= 2026 and 1 <= number <= 12 and f"{year:04d}-{number:02d}" == month):
            raise ValueError("Unsupported month; use YYYY-MM in 2019..2026")
        for kind in kinds:
            filename = f"{kind}_tripdata_{month}.parquet"
            items.append({"source": "NYC TLC", "url": f"{BASE}/trip-data/{filename}",
                          "relative_path": f"trips/{filename}", "month": month,
                          "type": kind})
    if include_zones:
        items.append({"source": "NYC TLC taxi zones", "url": ZONE_URL,
                      "relative_path": "reference/taxi_zone_lookup.csv"})
    return items

def save_url(url, destination, *, max_bytes=250 * 1024 * 1024, opener=urlopen):
    """Reject non-official host, detect oversized files and abort partial transfers."""
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname != "d37ci6vzurychx.cloudfront.net":
        raise ValueError("Unapproved TLC download endpoint")
    if not 1024 <= max_bytes <= 8 * 1024**3:
        raise ValueError("Per-file cap must be between 1KiB and 8GiB")
    dest = Path(destination)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".partial")
    digest = hashlib.sha256()
    count = 0
    try:
        with opener(Request(url, headers={"User-Agent": "portfolio-tlc-data/1.0"}), timeout=90) as stream:
            length = stream.headers.get("Content-Length")
            if length and int(length) > max_bytes:
                raise ValueError("Remote source exceeds configured --max-file-mb limit")
            with tmp.open("wb") as output:
                while True:
                    chunk = stream.read(1024 * 1024)
                    if not chunk:
                        break
                    count += len(chunk)
                    if count > max_bytes:
                        raise ValueError("Remote source exceeded download cap; partial file removed")
                    digest.update(chunk)
                    output.write(chunk)
        os.replace(tmp, dest)
    finally:
        tmp.unlink(missing_ok=True)
    return {"relative_path": str(dest), "bytes": count, "sha256": digest.hexdigest(),
            "source_url": url}

def run(months, kinds, out_dir, *, download=False, max_file_mb=250):
    items = plan(months, kinds)
    if not download:
        return {"status": "preview_only", "planned_sources": items,
                "note": "Explicitly use --download to fetch large public files"}
    root = Path(out_dir)
    received = []
    for item in items:
        result = save_url(item["url"], root / item["relative_path"],
                          max_bytes=int(max_file_mb * 1024**2))
        result["relative_path"] = item["relative_path"]
        received.append(result)
    manifest = {"status": "downloaded", "source": "NYC Taxi and Limousine Commission",
                "data_kind": "public trip records; not private Uber business records",
                "files": received, "attribution_url":
                    "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"}
    root.mkdir(parents=True, exist_ok=True)
    (root / "manifest_tlc.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--months", nargs="+", default=["2025-01"])
    p.add_argument("--types", nargs="+", default=["fhvhv"])
    p.add_argument("--out", default="raw-tlc")
    p.add_argument("--download", action="store_true")
    p.add_argument("--max-file-mb", type=int, default=250)
    a = p.parse_args()
    print(json.dumps(run(a.months, a.types, a.out, download=a.download,
                         max_file_mb=a.max_file_mb), indent=2))
