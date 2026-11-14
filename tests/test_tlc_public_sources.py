import io
from pathlib import Path
import pytest
from scripts.fetch_tlc_public import plan, save_url, run


def test_official_urls_and_bounded_data_plan(tmp_path):
    sources = plan(["2025-01", "2025-02"], ("fhvhv", "yellow"))
    assert len(sources) == 5
    assert sources[0]["url"].endswith("/fhvhv_tripdata_2025-01.parquet")
    assert sources[-1]["relative_path"] == "reference/taxi_zone_lookup.csv"
    assert run(["2025-01"], ("fhvhv",), tmp_path)["status"] == "preview_only"
    assert not list(tmp_path.glob("**/*"))


@pytest.mark.parametrize("month", ["2025-13", "2025-1", "../2025-01", "2015-01"])
def test_rejects_invalid_month(month):
    with pytest.raises(ValueError):
        plan([month])


def test_fails_closed_on_untrusted_url(tmp_path):
    with pytest.raises(ValueError):
        save_url("https://evil.example.com/thing", tmp_path / "data")


def test_download_checksum_and_size_guard(tmp_path):
    class FakeStream(io.BytesIO):
        headers = {"Content-Length": "5"}
        def __enter__(self): return self
        def __exit__(self, *args): self.close()
    def fake_open(request, timeout): return FakeStream(b"hello")
    dest = tmp_path / "data.parquet"
    manifest = save_url("https://d37ci6vzurychx.cloudfront.net/trip-data/a.parquet",
                        dest, max_bytes=1024, opener=fake_open)
    assert manifest["bytes"] == 5 and dest.read_bytes() == b"hello"
    assert len(manifest["sha256"]) == 64
    class Large(FakeStream):
        headers = {"Content-Length": "5000"}
    with pytest.raises(ValueError, match="exceeds"):
        save_url("https://d37ci6vzurychx.cloudfront.net/trip-data/b.parquet",
                 tmp_path / "large.parquet", max_bytes=1024,
                 opener=lambda request, timeout: Large(b"hello"))
    assert not (tmp_path / "large.parquet").exists()
