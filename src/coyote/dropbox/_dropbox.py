import logging
import json
import os
import os.path
import pickle
from typing import List, Any

from ..datetime import (
    TIMESTAMP_FILE,
    make_timestamp,
    has_timestamp,
    read_timestamp,
    write_timestamp,
)

logger = logging.getLogger(__name__)


class Dropbox:
    def __init__(self, remote_root_dir: str, local_root_dir: str):
        self.remote_root_dir = remote_root_dir
        self.local_root_dir = local_root_dir

    def remote_cd(self, path: str = '/'):
        raise NotImplementedError

    def remote_cd_back(self):
        raise NotImplementedError

    @property
    def remote_pwd(self) -> str:
        raise NotImplementedError

    def local_cd(self, path: str = '/'):
        raise NotImplementedError

    def local_cd_back(self):
        raise NotImplementedError

    @property
    def local_pwd(self) -> str:
        raise NotImplementedError

    def remote_is_file(self, path: str, *paths) -> bool:
        raise NotImplementedError

    def remote_is_dir(self, path: str, *paths) -> bool:
        raise NotImplementedError

    def local_is_file(self, path: str, *paths) -> bool:
        raise NotImplementedError

    def local_is_dir(self, path: str, *paths) -> bool:
        raise NotImplementedError

    def remote_ls(self, path: str = './', *paths, recursive: bool = False) -> List[str]:
        raise NotImplementedError

    def local_ls(self, path: str = './', *paths, recursive: bool = False) -> List[str]:
        raise NotImplementedError

    def remote_get_bytes(self, path: str, *paths) -> bytes:
        raise NotImplementedError

    def remote_get_text(self, path, *paths) -> str:
        return self.remote_get_bytes(path, *paths).decode()

    def remote_get_json(self, path, *paths) -> Any:
        return json.loads(self.remote_get_text(path, *paths))

    def remote_get_pickle(self, path, *paths) -> Any:
        return pickle.loads(self.remote_get_bytes(path, *paths))

    def remote_put_bytes(self, data: bytes, path: str, *paths, overwrite: bool = False) -> None:
        raise NotImplementedError

    def remote_put_text(self, text, path, *paths, overwrite=False):
        self.remote_put_bytes(text, path, *paths, overwrite=overwrite)

    def remote_put_json(self, x, path, *paths, overwrite=False):
        self.remote_put_text(json.dumps(x), path, *paths, overwrite=overwrite)

    def remote_put_pickle(self, x, path, *paths, overwrite=False):
        self.remote_put_bytes(pickle.dumps(x), path, *
                              paths, overwrite=overwrite)

    def download_file(self, remote_file: str, local_path: str, overwrite: bool = False):
        raise NotImplementedError

    def upload_file(self, local_file: str, remote_path: str, overwrite: bool = False):
        raise NotImplementedError

    def download_dir(self,
                     remote_dir: str,
                     local_dir: str,
                     *,
                     overwrite: bool = False,
                     clear_local_dir: bool = False,
                     verbose: bool = True):
        raise NotImplementedError

    def upload_dir(self, local_dir: str, remote_dir: str, *,
                   overwrite: bool = False,
                   clear_remote_dir: bool = False,
                   verbose: bool = True):
        raise NotImplementedError

    def remote_rm_file(self, path, *paths, forced: bool = False):
        raise NotImplementedError

    def remote_rm_dir(self, path, *paths, verbose: bool = True):
        raise NotImplementedError

    def remote_has_timestamp(self, *paths) -> bool:
        return self.remote_is_file(*paths, TIMESTAMP_FILE)

    def remote_write_timestamp(self, *paths) -> None:
        self.remote_put_text(make_timestamp(), *paths, TIMESTAMP_FILE)

    def remote_read_timestamp(self, *paths) -> str:
        return self.remote_get_text(*paths, TIMESTAMP_FILE)

    def remote_make_dir(self, path: str, *paths):
        if not self.remote_is_dir(path, *paths):
            self.remote_put_text(
                make_timestamp(), path, *paths, TIMESTAMP_FILE)

    def remote_clear(self, verbose: bool = True):
        self.remote_rm_dir('./', verbose=verbose)
