"""No HTTP or large file downloads from GitHub Actions."""
import hashlib
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import pytest
from real_data.tlc_sources import trip_url, weather_url, fetch_bundle, validate_manifest, download

def test_real_source_url_contracts():
    assert trip_url(2025, 1).endswith("fhvhv_tripdata_2025-01.parquet")
    q = parse_qs(urlparse(weather_url(2025, 2)).query)
    assert q["end_date"] == ["2025-02-28"]
    assert q["daily"] == ["precipitation_sum,temperature_2m_max,temperature_2m_min"]
    with pytest.raises(ValueError):
        trip_url(2026, 12)

def test_three_source_manifest_and_checksum_integrity(tmp_path):
    calls = []
    def fake_download(url, target, *, max_bytes):
        calls.append(url)
        payload = b"trusted-fixture" + str(len(calls)).encode()
        Path(target).write_bytes(payload)
        return {"url": url, "filename": Path(target).name, "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest()}
    manifest = fetch_bundle(tmp_path, 2025, 1, downloader=fake_download)
    assert set(manifest["sources"]) == {"trips", "zones", "weather"}
    assert validate_manifest(tmp_path)
    assert len(calls) == 3
    (tmp_path / "taxi_zone_lookup.csv").write_bytes(b"changed")
    with pytest.raises(ValueError, match="mismatch"):
        validate_manifest(tmp_path)

def test_safety_controls(tmp_path):
    with pytest.raises(ValueError, match="confirm-large"):
        fetch_bundle(tmp_path, max_megabytes=999, downloader=lambda *args, **kwargs: {})
    with pytest.raises(ValueError, match="Unapproved"):
        download("https://example.com/file", tmp_path / "a")
