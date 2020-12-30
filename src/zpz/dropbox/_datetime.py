import os.path
from datetime import datetime
from pathlib import Path
from typing import Union

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
            raise ValueError(f"`local_dir` should be a directory")
    (local_dir / TIMESTAMP_FILE).write_text(make_timestamp())


def read_timestamp(local_dir: Union[Path, str]) -> str:
    if isinstance(local_dir, str):
        local_dir = Path(local_dir)
    return (local_dir / TIMESTAMP_FILE).read_text()


def has_timestamp(local_dir: Union[Path, str]) -> bool:
    if isinstance(local_dir, str):
        local_dir = Path(local_dir)
    f = local_dir / TIMESTAMP_FILE
    return f.is_file()
