"""Idempotent sync of one business date from the local landing zone to a Unity Catalog volume.

Free Edition serverless compute has restricted outbound internet, so data is produced on the
orchestrator side and pushed in through the Files API. The sync:

1. skips files whose sha256 matches the remote checksum index (a rerun moves no bytes),
2. uploads new or changed files,
3. deletes stale files in the touched partitions, e.g. from an older GENERATOR_VERSION,
4. writes the checksum index last, as the commit marker for the partition.
"""

from __future__ import annotations

import io
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from fooddelivery.config import LakehouseTarget
from fooddelivery.generator.writer import CHECKSUM_DIR, checksum_name, read_checksums

log = logging.getLogger(__name__)


class FilesApi(Protocol):  # the subset of databricks.sdk FilesAPI that we use
    def upload(self, file_path: str, contents, *, overwrite: bool | None = None) -> None: ...
    def download(self, file_path: str): ...
    def list_directory_contents(self, directory_path: str): ...
    def delete(self, file_path: str) -> None: ...
    def create_directory(self, directory_path: str) -> None: ...


@dataclass
class SyncResult:
    business_date: str
    uploaded: int = 0
    skipped: int = 0
    deleted: int = 0
    bytes_uploaded: int = 0

    def as_dict(self) -> dict:
        return dict(vars(self))


def _is_not_found(exc: Exception) -> bool:
    return type(exc).__name__ in ("NotFound", "ResourceDoesNotExist", "FileNotFoundError")


class VolumeSync:
    def __init__(self, files: FilesApi, target: LakehouseTarget):
        self.files = files
        self.root = target.volume_path

    def _remote_index(self, business_date: str) -> dict:
        try:
            resp = self.files.download(f"{self.root}/{checksum_name(business_date)}")
            return json.loads(resp.contents.read())
        except Exception as exc:  # absent on first sync
            if _is_not_found(exc):
                return {}
            raise

    def sync(self, landing: Path, business_date: str) -> SyncResult:
        result = SyncResult(business_date)
        local = read_checksums(landing, business_date)
        remote = self._remote_index(business_date)
        for rel, meta in sorted(local.items()):
            if remote.get(rel, {}).get("sha256") == meta["sha256"]:
                result.skipped += 1
                continue
            payload = (landing / rel).read_bytes()
            directory = f"{self.root}/{rel.rsplit('/', 1)[0]}"
            self.files.create_directory(directory)
            self.files.upload(f"{self.root}/{rel}", io.BytesIO(payload), overwrite=True)
            result.uploaded += 1
            result.bytes_uploaded += len(payload)
        wanted = {f"{self.root}/{rel}" for rel in local}
        for directory in sorted({f"{self.root}/{rel.rsplit('/', 1)[0]}" for rel in local}):
            for entry in self.files.list_directory_contents(directory):
                if not entry.is_directory and entry.path not in wanted:
                    self.files.delete(entry.path)
                    result.deleted += 1
        index = json.dumps(local, indent=2, sort_keys=True).encode()
        self.files.create_directory(f"{self.root}/{CHECKSUM_DIR}")
        self.files.upload(f"{self.root}/{checksum_name(business_date)}", io.BytesIO(index), overwrite=True)
        log.info("landing partition synced", extra=result.as_dict())
        return result


def workspace_client(host: str | None = None, token: str | None = None, profile: str | None = None):
    """databricks-sdk client via unified auth (env vars, ~/.databrickscfg profile, or explicit PAT)."""
    from databricks.sdk import WorkspaceClient

    if host and token:
        return WorkspaceClient(host=host, token=token)
    return WorkspaceClient(profile=profile) if profile else WorkspaceClient()
