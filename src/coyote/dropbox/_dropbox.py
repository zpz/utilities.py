import logging
import json
import os
import os.path
import pickle
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


def _get_cp_dest(abs_source_file: str, abs_dest_path: str) -> str:
    # Get the destination file path as if we do
    #    cp abs_source_file abs_dest_path
    if abs_dest_path.endswith('/'):
        assert not abs_source_file.endswith('/')
        return os.path.join(abs_dest_path, os.path.basename(abs_source_file))
    return abs_dest_path


class Dropbox:
    def __init__(self, remote_root_dir: str, local_root_dir: str):
        self.remote_root_dir = remote_root_dir
        self.local_root_dir = local_root_dir
        self.remote_pwd = '/'
        self.remote_pwd_history = []
        self.local_pwd = '/'
        self.local_pwd_history = []
        self.remote_store = LocalFileStore()

    def _remote_abs_path(self, path: str, *paths):
        # The return starts with '/', which indicates
        # `self.remote_root_dir`.
        # 'absolute' is relative to that root.
        if paths:
            path = os.path.join(path, *paths)
        if not path.startswith('/'):
            path = join_path(self.remote_pwd, path)
        return path

    def _remote_real_path(self, path: str, *paths):
        path = self._remote_abs_path(path, *paths).lstrip('/')
        return os.path.join(self.remote_root_dir, path)

    def remote_cd(self, path: str = '/', *paths):
        pwd = self._remote_abs_path(path, *paths)
        if not pwd.endswith('/'):
            pwd += '/'
        self.remote_pwd_history.append(self.remote_pwd)
        self.remote_pwd = pwd
        return self

    def remote_cd_back(self):
        if self.remote_pwd_history:
            self.remote_pwd = self.remote_pwd_history.pop()
            return self
        raise Exception(
            "can not find a matching `remote_cd` for `remote_cd_back`")

    def _local_abs_path(self, path: str, *paths):
        # The return starts with '/', which indicates
        # `self.local_root_dir`.
        # 'absolute' is relative to that root.
        if paths:
            path = os.path.join(path, *paths)
        if not path.startswith('/'):
            path = join_path(self.local_pwd, path)
        return path

    def _local_real_path(self, path: str, *paths):
        path = self._local_abs_path(path, *paths).lstrip('/')
        return os.path.join(self.local_root_dir, path)

    def local_cd(self, path: str = '/', *paths):
        pwd = self._local_abs_path(path, *paths)
        if not pwd.endswith('/'):
            pwd += '/'
        self.local_pwd_history.append(self.local_pwd)
        self.local_pwd = pwd
        return self

    def local_cd_back(self):
        if self.local_pwd_history:
            self.local_pwd = self.local_pwd_history.pop()
            return self
        raise Exception(
            "can not find a matching `local_cd` for `local_cd_back`")

    def remote_is_file(self, path: str, *paths) -> bool:
        f = self._remote_real_path(path, *paths)
        if _is_file_name(f):
            return self.remote_store.is_file(f)
        return False

    def remote_is_dir(self, path: str, *paths) -> bool:
        f = self._remote_real_path(path, *paths)
        if not f.endswith('/'):
            f += '/'
        return self.remote_store.is_dir(f)

    def local_is_file(self, path: str, *paths) -> bool:
        f = self._local_real_path(path, *paths)
        if _is_file_name(f):
            return Path(f).is_file()
        return False

    def local_is_dir(self, path: str, *paths) -> bool:
        f = self._local_real_path(path, *paths)
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
        zz = LocalFileStore().ls(f, recursive=recursive)
        n = len(self.local_root_dir)
        return sorted(v[n:] for v in zz)
        # TODO: get absolute '/' right.

    def remote_read_bytes(self, path: str, *paths) -> bytes:
        f = self._remote_real_path(path, *paths)
        return self.remote_store.read_bytes(f)

    def remote_read_text(self, path, *paths) -> str:
        return self.remote_read_bytes(path, *paths).decode()

    def remote_read_json(self, path, *paths) -> Any:
        return json.loads(self.remote_read_text(path, *paths))

    def remote_read_pickle(self, path, *paths) -> Any:
        return pickle.loads(self.remote_read_bytes(path, *paths))

    def remote_write_bytes(self, data: bytes, path: str, *paths, overwrite: bool = False) -> None:
        f = self._remote_real_path(path, *paths)
        self.remote_store.write_bytes(data, f, overwrite=overwrite)

    def remote_write_text(self, text, path, *paths, overwrite=False):
        self.remote_write_bytes(text, path, *paths, overwrite=overwrite)

    def remote_write_json(self, x, path, *paths, overwrite=False):
        self.remote_write_text(json.dumps(x), path, *
                               paths, overwrite=overwrite)

    def remote_write_pickle(self, x, path, *paths, overwrite=False):
        self.remote_write_bytes(pickle.dumps(x), path, *
                                paths, overwrite=overwrite)

    def download(self, remote_file: str, local_path: str, overwrite: bool = False):
        remote_file = self._remote_real_path(remote_file)
        assert _is_file_name(remote_file)
        local_file = self._local_real_path(local_path)
        if not _is_file_name(local_file):
            local_file = _get_cp_dest(remote_file, local_file)
        self.remote_store.download(
            remote_file, local_file, overwrite=overwrite)

    def upload(self, local_file: str, remote_path: str, overwrite: bool = False):
        local_file = self._local_real_path(local_file)
        assert _is_file_name(local_file)
        remote_file = self._remote_real_path(remote_path)
        if not _is_file_name(remote_file):
            remote_file = _get_cp_dest(local_file, remote_file)
        self.remote_store.upload(
            local_file, remote_file, overwrite=overwrite)

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

    def remote_rm(self, path, *paths, missing_ok: bool = False):
        f = self._remote_real_path(path, *paths)
        
        raise NotImplementedError

    def remote_rm_dir(self, path, *paths, missing_ok: bool = False, verbose: bool = True):
        raise NotImplementedError

    def remote_has_timestamp(self, *paths) -> bool:
        return self.remote_is_file(*paths, TIMESTAMP_FILE)

    def remote_write_timestamp(self, *paths) -> None:
        self.remote_write_text(make_timestamp(), *paths, TIMESTAMP_FILE)

    def remote_read_timestamp(self, *paths) -> str:
        return self.remote_read_text(*paths, TIMESTAMP_FILE)

    def remote_make_dir(self, path: str, *paths):
        if not self.remote_is_dir(path, *paths):
            self.remote_write_text(
                make_timestamp(), path, *paths, TIMESTAMP_FILE)

    def remote_clear(self, verbose: bool = True):
        self.remote_rm_dir('./', verbose=verbose)
