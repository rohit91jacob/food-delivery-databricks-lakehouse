from __future__ import annotations

from dataclasses import dataclass

from conftest import DAYS
from fooddelivery.config import LakehouseTarget
from fooddelivery.landing.uploader import VolumeSync


class NotFound(Exception):
    pass


@dataclass
class Entry:
    path: str
    is_directory: bool = False


class FakeFiles:
    """In-memory stand-in for databricks.sdk FilesAPI."""

    def __init__(self):
        self.store: dict[str, bytes] = {}
        self.uploads = 0

    def upload(self, file_path, contents, *, overwrite=None):
        self.store[file_path] = contents.read()
        self.uploads += 1

    def download(self, file_path):
        if file_path not in self.store:
            raise NotFound(file_path)
        data = self.store[file_path]

        class Resp:
            class contents:
                @staticmethod
                def read():
                    return data

        return Resp

    def list_directory_contents(self, directory_path):
        prefix = directory_path.rstrip("/") + "/"
        return [Entry(p) for p in self.store if p.startswith(prefix) and "/" not in p[len(prefix) :]]

    def delete(self, file_path):
        del self.store[file_path]

    def create_directory(self, directory_path):
        pass


def test_sync_is_idempotent_and_prunes_stale_files(landing):
    files = FakeFiles()
    sync = VolumeSync(files, LakehouseTarget(schema="s"))
    bd = DAYS[0].isoformat()

    first = sync.sync(landing, bd)
    assert first.uploaded == 15 and first.skipped == 0  # 14 feeds + manifest
    assert f"/Volumes/workspace/s/landing/orders/dt={bd}/part-00000-v1.json.gz" in files.store

    uploads_before = files.uploads
    second = sync.sync(landing, bd)
    assert (second.uploaded, second.skipped, second.deleted) == (0, 15, 0)
    assert files.uploads == uploads_before + 1  # only the checksum index (commit marker)

    stale = f"/Volumes/workspace/s/landing/orders/dt={bd}/part-00000-v0.json.gz"
    files.store[stale] = b"old generator version"
    third = sync.sync(landing, bd)
    assert third.deleted == 1 and stale not in files.store
