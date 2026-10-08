"""Writes a DayBatch to the local landing zone, byte-for-byte reproducibly.

Layout (mirrored 1:1 into the Unity Catalog volume)::

    landing/<entity>/dt=YYYY-MM-DD/part-00000-v<GENERATOR_VERSION>.json.gz
    landing/_manifests/dt=YYYY-MM-DD/manifest-v<GENERATOR_VERSION>.json
    landing/_checksums/dt=YYYY-MM-DD.json      (upload commit marker / idempotency index)

The partition folder is ``dt=`` rather than ``business_date=`` so that partition discovery can
never collide with the ``business_date`` field carried inside every record.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
from pathlib import Path

from fooddelivery import GENERATOR_VERSION
from fooddelivery.generator.simulator import DayBatch

MANIFEST_DIR = "_manifests"
CHECKSUM_DIR = "_checksums"


def _ndjson_gz(rows: list[dict]) -> bytes:
    raw = "".join(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n" for r in rows).encode()
    buf = io.BytesIO()
    # mtime=0 and an empty filename keep the gzip header (and so the bytes) deterministic
    with gzip.GzipFile(filename="", mode="wb", fileobj=buf, mtime=0, compresslevel=6) as gz:
        gz.write(raw)
    return buf.getvalue()


def partition_dir(entity: str, business_date: str) -> str:
    return f"{entity}/dt={business_date}"


def data_file_name() -> str:
    return f"part-00000-v{GENERATOR_VERSION}.json.gz"


def write_batch(batch: DayBatch, landing: Path) -> dict:
    """Write all files for one business date; returns the checksum index ({relative_path: meta})."""
    bd = batch.business_date.isoformat()
    index: dict[str, dict] = {}
    for entity, rows in batch.records.items():
        rel = f"{partition_dir(entity, bd)}/{data_file_name()}"
        index[rel] = _write(landing / rel, _ndjson_gz(rows), len(rows))
    manifest_rel = f"{partition_dir(MANIFEST_DIR, bd)}/manifest-v{GENERATOR_VERSION}.json"
    manifest = "".join(json.dumps(m, sort_keys=True) + "\n" for m in batch.manifests).encode()
    index[manifest_rel] = _write(landing / manifest_rel, manifest, len(batch.manifests))
    checksum_file = landing / checksum_name(bd)
    checksum_file.parent.mkdir(parents=True, exist_ok=True)
    checksum_file.write_text(json.dumps(index, indent=2, sort_keys=True) + "\n")
    return index


def _write(path: Path, payload: bytes, rows: int) -> dict:
    path.parent.mkdir(parents=True, exist_ok=True)
    for stale in path.parent.glob("*"):
        if stale.name != path.name:  # files from older generator versions
            stale.unlink()
    path.write_bytes(payload)
    return {"rows": rows, "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}


def checksum_name(business_date: str) -> str:
    return f"{CHECKSUM_DIR}/dt={business_date}.json"


def read_checksums(landing: Path, business_date: str) -> dict:
    return json.loads((landing / checksum_name(business_date)).read_text())


def read_manifest(landing: Path, business_date: str) -> list[dict]:
    path = landing / partition_dir(MANIFEST_DIR, business_date) / f"manifest-v{GENERATOR_VERSION}.json"
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
