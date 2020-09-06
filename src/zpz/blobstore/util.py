from datetime import datetime
import os
import os.path
import string
from typing import Callable, List, Type


def get_container():
    raise NotImplementedError


TIMESTAMP_FILE = 'updated_at_utc.txt'


def make_timestamp() -> str:
    return datetime.utcnow().isoformat(timespec='microseconds')


def has_local_timestamp(local_abs_dir: str) -> bool:
    assert local_abs_dir.startswith('/')
    return os.path.isfile(os.path.join(local_abs_dir, TIMESTAMP_FILE))

    
def write_local_timestamp(local_abs_dir: str) -> None:
    assert local_abs_dir.startswith('/')
    open(os.path.join(local_abs_dir, TIMESTAMP_FILE), 'w').write(make_timestamp())


def read_local_timestamp(local_abs_dir: str) -> str:
    assert local_abs_dir.startswith('/')
    return open(os.path.join(local_abs_dir, TIMESTAMP_FILE)).read()


def has_remote_timestamp(container: 'Container', remote_dir: str='./') -> bool:
    container.cd(remote_dir)
    z = container.isfile(TIMESTAMP_FILE)
    container.cd_back()
    return z


def write_remote_timestamp(container: 'Container', remote_dir: str='./') -> None:
    container.cd(remote_dir)
    container.put_text(make_timestamp(), TIMESTAMP_FILE)
    container.cd_back()


def read_remote_timestamp(container: 'Container', remote_dir: str='./') -> str:
    container.cd(remote_dir)
    ts = container.get_text(TIMESTAMP_FILE)
    container.cd_back()
    return ts


def make_version() -> str:
    return datetime.utcnow().strftime('%Y%m%d-%H%M%S')


ALNUM = string.ascii_letters + string.digits


def is_version(version: str) -> bool:
    if not version:
        return False
    return (version[0] in ALNUM) and all(v in ALNUM + '._-' for v in version)


def upload_dir(
        local_abs_dir: str,
        container: 'Container',
        remote_dir: str,
        has_timestamp: bool=True,
        progress_printer: Callable[[str], None]=None) -> int:
    assert local_abs_dir.startswith('/')
    container.cd(remote_dir)

    if has_timestamp:
        if not has_local_timestamp(local_abs_dir):
            raise RuntimeError(f"local directory `{local_abs_dir}` does not contain timestamp file `{TIMESTAMP_FILE}`")
        if has_remote_timestamp(container):
            ts_local = read_local_timestamp(local_abs_dir)
            ts_remote = read_remote_timestamp(container)
            if ts_remote >= ts_local:
                container.cd_back()
                return 0

    n = container.put_dir(local_abs_dir, './', clear_remote_dir=True, progress_printer=progress_printer)
    container.cd_back()
    return 0


def download_dir(
        container: 'Container',
        remote_dir: str,
        local_abs_dir: str,
        has_timestamp: bool=True,
        progress_printer: Callable[[str], None]=None) -> int:
    assert local_abs_dir.startswith('/')
    container.cd(remote_dir)

    if has_timestamp:
        if not has_remote_timestamp(container):
            raise RuntimeError(f"remote directory `{contain.pwd}` does not contain timestamp file `{TIMESTAMP_FILE}`")
        if has_local_timestamp(local_abs_dir):
            ts_local = read_local_timestamp(local_abs_dir)
            ts_remote = read_remote_timestamp(container)
            if ts_local >= ts_remote:
                container.cd_back()
                return 0

    n = container.get_dir('./', local_abs_dir, clear_local_dir=True, progress_printer=progress_printer)
    container.cd_back()
    return 0


def get_local_versions(cls: Type) -> List[str]:
    path = os.path.join(os.environ['DATADIR'], cls.__name__, 'versions')
    if not os.path.isdir(path):
        return []
    dd = [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))]
    return dd


def get_remote_versions(container: 'Container', cls: Type) -> List[str]:
    raise NotImplementedError
