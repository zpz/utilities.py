from typing import Callable


def get_container():
    raise NotImplementedError


def upload_dir(
        local_abs_dir: str,
        container: 'Container',
        remote_dir: str,
        has_timestamp: bool = True,
        progress_printer: Callable[[str], None] = None) -> int:
    assert local_abs_dir.startswith('/')
    container.cd(remote_dir)

    if has_timestamp:
        if not has_local_timestamp(local_abs_dir):
            raise RuntimeError(
                f"local directory `{local_abs_dir}` does not contain timestamp file `{TIMESTAMP_FILE}`")
        if has_remote_timestamp(container):
            ts_local = read_local_timestamp(local_abs_dir)
            ts_remote = read_remote_timestamp(container)
            if ts_remote >= ts_local:
                container.cd_back()
                return 0

    n = container.put_dir(
        local_abs_dir, './', clear_remote_dir=True, progress_printer=progress_printer)
    container.cd_back()
    return 0


def download_dir(
        container: 'Container',
        remote_dir: str,
        local_abs_dir: str,
        has_timestamp: bool = True,
        progress_printer: Callable[[str], None] = None) -> int:
    assert local_abs_dir.startswith('/')
    container.cd(remote_dir)

    if has_timestamp:
        if not has_remote_timestamp(container):
            raise RuntimeError(
                f"remote directory `{contain.pwd}` does not contain timestamp file `{TIMESTAMP_FILE}`")
        if has_local_timestamp(local_abs_dir):
            ts_local = read_local_timestamp(local_abs_dir)
            ts_remote = read_remote_timestamp(container)
            if ts_local >= ts_remote:
                container.cd_back()
                return 0

    n = container.get_dir(
        './', local_abs_dir, clear_local_dir=True, progress_printer=progress_printer)
    container.cd_back()
    return 0
