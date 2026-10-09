"""Builds the restaurant seed: the committed copy (default), a Kaggle download, or a synthetic fallback.

``committed`` unpacks ``seed/restaurants.jsonl.gz``, which is checked into the repository (the Kaggle
dataset is CC0) and verified against its recorded SHA-256, so scheduled runs need no Kaggle account.
The Kaggle client reads credentials from ``KAGGLE_API_TOKEN`` or ``~/.kaggle/access_token``.
In containers, point ``FD_KAGGLE_TOKEN_FILE`` at a mounted secret instead; it is loaded
into the environment of this process only and never logged.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import shutil
import tempfile
from pathlib import Path

from fooddelivery.config import GeneratorConfig, Paths
from fooddelivery.generator.seed import (
    KAGGLE_DATASET,
    KAGGLE_FILE,
    KAGGLE_LICENSE,
    load_swiggy_csv,
    read_seed,
    synthetic_seed,
    write_seed,
)

log = logging.getLogger(__name__)

# Repository copy of the seed; FD_COMMITTED_SEED points elsewhere (e.g. inside a container image).
DEFAULT_COMMITTED_SEED = Path(__file__).resolve().parents[2] / "seed" / "restaurants.jsonl.gz"


def _load_token_file() -> None:
    token_file = os.environ.get("FD_KAGGLE_TOKEN_FILE", "").strip()
    if token_file and not os.environ.get("KAGGLE_API_TOKEN"):
        os.environ["KAGGLE_API_TOKEN"] = Path(token_file).expanduser().read_text().strip()


def download_kaggle_csv(dest_dir: Path, dataset: str = KAGGLE_DATASET) -> Path:
    """Download into a fresh, empty directory (downloads are untrusted data) and return the CSV."""
    _load_token_file()
    from kaggle.api.kaggle_api_extended import KaggleApi  # optional dependency (extra: seed)

    api = KaggleApi()
    api.authenticate()
    dest_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix="kaggle-", dir=dest_dir.parent))
    try:
        api.dataset_download_files(dataset, path=str(staging), unzip=True, quiet=True)
        csv_path = staging / KAGGLE_FILE
        if not csv_path.is_file():
            raise FileNotFoundError(f"{KAGGLE_FILE} not found in Kaggle dataset {dataset}")
        if dest_dir.exists():
            shutil.rmtree(dest_dir)
        staging.rename(dest_dir)
        return dest_dir / KAGGLE_FILE
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)


def install_committed_seed(paths: Paths, archive: Path | None = None) -> dict:
    """Unpack the committed seed into ``paths.seed_file`` after checking it against its recorded SHA-256."""
    archive = archive or Path(os.environ.get("FD_COMMITTED_SEED", "") or DEFAULT_COMMITTED_SEED)
    meta_path = archive.parent / "restaurants.meta.json"
    if not archive.is_file() or not meta_path.is_file():
        raise FileNotFoundError(
            f"committed seed not found at {archive}; set FD_COMMITTED_SEED or FD_SEED_SOURCE=kaggle"
        )
    meta = json.loads(meta_path.read_text())
    payload = gzip.decompress(archive.read_bytes())
    digest = hashlib.sha256(payload).hexdigest()
    if digest != meta["sha256"]:
        raise ValueError(f"committed seed checksum mismatch: {digest} != {meta['sha256']}")
    paths.seed_file.parent.mkdir(parents=True, exist_ok=True)
    paths.seed_file.write_bytes(payload)
    paths.seed_file.with_suffix(".meta.json").write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")
    return meta


def ensure_seed(paths: Paths, cfg: GeneratorConfig, source: str | None = None, *, force: bool = False) -> dict:
    """Create the seed file once; afterwards it is reused so simulations stay reproducible."""
    source = (source or os.environ.get("FD_SEED_SOURCE", "committed")).strip().lower()
    if paths.seed_file.exists() and not force:
        seeds = read_seed(paths.seed_file)
        log.info("seed already present", extra={"restaurants": len(seeds), "path": str(paths.seed_file)})
        return {"restaurants": len(seeds), "reused": True}
    if source == "committed":
        meta = install_committed_seed(paths)
    elif source == "kaggle":
        csv_path = download_kaggle_csv(paths.downloads / "swiggy-restaurant-dataset")
        seeds = load_swiggy_csv(csv_path)
        meta = write_seed(seeds, paths.seed_file, source=f"kaggle:{KAGGLE_DATASET}", license_name=KAGGLE_LICENSE)
    elif source == "synthetic":
        seeds = synthetic_seed(cfg.cities, max(cfg.restaurants_per_city, 50))
        meta = write_seed(seeds, paths.seed_file, source="synthetic", license_name="MIT")
    else:
        raise ValueError(f"FD_SEED_SOURCE must be 'committed', 'kaggle' or 'synthetic', got {source!r}")
    missing = [c for c in cfg.cities if c not in meta["cities"]]
    if missing:
        raise ValueError(f"seed has no restaurants for cities {missing}")
    log.info("seed written", extra={"restaurants": meta["restaurants"], "source": meta["source"]})
    return {**meta, "reused": False}
