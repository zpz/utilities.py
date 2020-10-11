import logging
import json
import os
import os.path
import pickle
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import List, Any, Union

from ..path import join_path
from ._local import LocalFileStore

logger = logging.getLogger(__name__)


TIMESTAMP_FILE = 'updated_at_utc.txt'


def make_timestamp() -> str:
    '''
    This function creates a timestamp string with fixed format like

        '2020-08-22T08:09:13.401346'

    Strings created by this function can be compared to
    determine time order. There is no need to parse the string
    into `datetime` objects.

    The returned string is often written as a timestamp file, like

        open(file_name, 'w').write(make_timestamp())
    '''
    return datetime.utcnow().isoformat(timespec='microseconds')


def write_timestamp(local_dir: Union[Path, str]) -> None:
    if isinstance(local_dir, str):
        local_dir = Path(local_dir)
    if not local_dir.exists():
        local_dir.mkdir(parents=True)
    else:
        if not local_dir.is_dir():
            raise ValueError("`local_dir` should be a directory")
    (local_dir / TIMESTAMP_FILE).write_text(make_timestamp())


def read_timestamp(local_dir: Union[Path, str]) -> str:
    if isinstance(local_dir, str):
        local_dir = Path(local_dir)
    return (local_dir / TIMESTAMP_FILE).read_text()


def has_timestamp(local_dir: Union[Path, str]) -> bool:
    if isinstance(local_dir, str):
        local_dir = Path(local_dir)
    return (local_dir / TIMESTAMP_FILE).is_file()


def _is_abs(path: str) -> bool:
    return path.startswith('/')


def _is_file_name(abs_path: str) -> bool:
    return not abs_path.endswith('/')


def _is_dir_name(abs_path: str) -> bool:
    return abs_path.endswith('/')


# def _get_cp_dest(abs_source_file: str, abs_dest_path: str) -> str:
#     # Get the destination file path as if we do
#     #    cp abs_source_file abs_dest_path
#     if abs_dest_path.endswith('/'):
#         assert not abs_source_file.endswith('/')
#         return os.path.join(abs_dest_path, os.path.basename(abs_source_file))
#     return abs_dest_path


class Dropbox:
    def __init__(self, remote_root_dir: str, local_root_dir: str):
        self.remote_root_dir = remote_root_dir
        self.local_root_dir = local_root_dir
        self.pwd = '/'
        self.pwd_history = []
        self.remote_store = LocalFileStore()
        self.local_store = LocalFileStore()

    def _abs_path(self, path: str, *paths):
        # The return starts with '/', which indicates
        # `self.remote_root_dir` and `self.local_root_dir`.
        # 'absolute' is relative to that root.
        if paths:
            path = os.path.join(path, *paths)
        if not path.startswith('/'):
            path = join_path(self.pwd, path)
        return path

    def _remote_real_path(self, path: str, *paths):
        path = self._abs_path(path, *paths).lstrip('/')
        return os.path.join(self.remote_root_dir, path)

    def _local_real_path(self, path: str, *paths):
        path = self._abs_path(path, *paths).lstrip('/')
        return os.path.join(self.local_root_dir, path)

    @contextmanager
    def cd(self, path: str, *paths):
        # This command can be nested, e.g.
        #
        #    with dropbox.cd('abc') as box1:
        #        with box1.cd('de') as box2:
        #            ...
        pwd = self._abs_path(path, *paths)
        if not pwd.endswith('/'):
            pwd += '/'
        self.pwd_history.append(self.pwd)
        self.pwd = pwd
        try:
            return self
        finally:
            self.pwd = self.pwd_history.pop()

    def remote_is_file(self, path: str) -> bool:
        f = self._remote_real_path(path)
        if _is_file_name(f):
            return self.remote_store.is_file(f)
        return False

    def remote_is_dir(self, path: str) -> bool:
        f = self._remote_real_path(path)
        if not f.endswith('/'):
            f += '/'
        return self.remote_store.is_dir(f)

    def local_is_file(self, path: str) -> bool:
        f = self._local_real_path(path)
        if _is_file_name(f):
            return Path(f).is_file()
        return False

    def local_is_dir(self, path: str) -> bool:
        f = self._local_real_path(path)
        if not f.endswith('/'):
            f += '/'
        return Path(f).is_dir()

    def remote_ls(self, path: str = './', recursive: bool = False) -> List[str]:
        f = self._remote_real_path(path)
        zz = self.remote_store.ls(f, recursive=recursive)
        n = len(self.remote_root_dir)
        return sorted(v[n:] for v in zz)
        # TODO: get absolute '/' right.

    def local_ls(self, path: str = './', recursive: bool = False) -> List[str]:
        f = self._local_real_path(path)
        zz = self.local_store.ls(f, recursive=recursive)
        n = len(self.local_root_dir)
        return sorted(v[n:] for v in zz)
        # TODO: get absolute '/' right.

    def remote_read_bytes(self, file_path: str) -> bytes:
        f = self._remote_real_path(file_path)
        return self.remote_store.read_bytes(f)

    def remote_read_text(self, file_path) -> str:
        return self.remote_read_bytes(file_path).decode()

    def remote_read_json(self, file_path) -> Any:
        return json.loads(self.remote_read_text(file_path))

    def remote_read_pickle(self, file_path) -> Any:
        return pickle.loads(self.remote_read_bytes(file_path))

    def remote_write_bytes(self, data: bytes, file_path: str, overwrite: bool = False) -> None:
        f = self._remote_real_path(file_path)
        self.remote_store.write_bytes(data, f, overwrite=overwrite)

    def remote_write_text(self, text, file_path, overwrite=False):
        self.remote_write_bytes(text, file_path, overwrite=overwrite)

    def remote_write_json(self, x, file_path, overwrite=False):
        self.remote_write_text(json.dumps(x), file_path, overwrite=overwrite)

    def remote_write_pickle(self, x, file_path, overwrite=False):
        self.remote_write_bytes(pickle.dumps(
            x), file_path, overwrite=overwrite)

    def local_read_bytes(self, file_path: str) -> bytes:
        f = self._local_real_path(file_path)
        return self.local_store.read_bytes(f)

    def local_read_text(self, file_path) -> str:
        return self.local_read_bytes(file_path).decode()

    def local_read_json(self, file_path) -> Any:
        return json.loads(self.local_read_text(file_path))

    def local_read_pickle(self, file_path) -> Any:
        return pickle.loads(self.local_read_bytes(file_path))

    def local_write_bytes(self, data: bytes, file_path: str, overwrite: bool = False) -> None:
        f = self._local_real_path(file_path)
        self.local_store.write_bytes(data, f, overwrite=overwrite)

    def local_write_text(self, text, file_path, overwrite=False):
        self.local_write_bytes(text, file_path, overwrite=overwrite)

    def local_write_json(self, x, file_path, overwrite=False):
        self.local_write_text(json.dumps(x), file_path, overwrite=overwrite)

    def local_write_pickle(self, x, file_path, overwrite=False):
        self.local_write_bytes(pickle.dumps(
            x), file_path, overwrite=overwrite)

    def download(self, file_path: str, overwrite: bool = False):
        assert _is_file_name(file_path)
        remote_file = self._remote_real_path(file_path)
        local_file = self._local_real_path(file_path)
        self.remote_store.download(
            remote_file, local_file, overwrite=overwrite)

    def upload(self, file_path: str, overwrite: bool = False):
        assert _is_file_name(file_path)
        local_file = self._local_real_path(file_path)
        remote_file = self._remote_real_path(file_path)
        self.remote_store.upload(
            local_file, remote_file, overwrite=overwrite)

    def download_dir(self,
                     dir_path: str,
                     *,
                     overwrite: bool = False,
                     clear_local_dir: bool = False,
                     verbose: bool = True):
        raise NotImplementedError

    def upload_dir(self, dir_path: str,
                   *,
                   overwrite: bool = False,
                   clear_remote_dir: bool = False,
                   verbose: bool = True):
        raise NotImplementedError

    def remote_rm(self, file_path, missing_ok: bool = False):
        f = self._remote_real_path(file_path)
        self.remote_store.rm(f, missing_ok=missing_ok)

    def remote_rm_dir(self, file_path, missing_ok: bool = False, verbose: bool = True):
        f = self._remote_real_path(file_path)
        self.remote_store.rm_dir(f, missing_ok=missing_ok, verbose=verbose)

    def local_rm(self, file_path, missing_ok: bool = False):
        f = self._remote_real_path(file_path)
        self.local_store.rm(f, missing_ok=missing_ok)

    def local_rm_dir(self, file_path, missing_ok: bool = False, verbose: bool = True):
        f = self._remote_real_path(file_path)
        self.local_store.rm_dir(f, missing_ok=missing_ok, verbose=verbose)

    def remote_has_timestamp(self, *paths) -> bool:
        return self.remote_is_file(*paths, TIMESTAMP_FILE)

    def remote_write_timestamp(self, *paths) -> None:
        self.remote_write_text(make_timestamp(), *paths, TIMESTAMP_FILE)

    def remote_read_timestamp(self, *paths) -> str:
        return self.remote_read_text(*paths, TIMESTAMP_FILE)

    def local_has_timestamp(self, *paths) -> bool:
        return self.local_is_file(*paths, TIMESTAMP_FILE)

    def local_write_timestamp(self, *paths) -> None:
        self.local_write_text(make_timestamp(), *paths, TIMESTAMP_FILE)

    def local_read_timestamp(self, *paths) -> str:
        return self.local_read_text(*paths, TIMESTAMP_FILE)

    def remote_make_dir(self, path: str, *paths):
        if not self.remote_is_dir(path, *paths):
            self.remote_write_text(
                make_timestamp(), path, *paths, TIMESTAMP_FILE)

    def local_make_dir(self, path: str, *paths):
        if not self.local_is_dir(path, *paths):
            self.local_write_text(
                make_timestamp(), path, *paths, TIMESTAMP_FILE)

    def remote_clear(self, verbose: bool = True):
        self.remote_rm_dir('./', verbose=verbose)

    def local_clear(self, verbose: bool = True):
        self.local_rm_dir('./', verbose=verbose)
