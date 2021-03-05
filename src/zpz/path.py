import inspect
import os.path
from pathlib import Path
import tempfile
import uuid
from typing import Union


def join_path(base_dir: str, *rel_paths: str) -> str:
    return os.path.abspath(os.path.join(base_dir, *rel_paths))


def relative_path(path: str) -> str:
    '''
    Given path `path` relative to the file where this function is called,
    return absolute path.

    For example, suppose this function is called in file '/home/work/src/repo1/scripts/abc.py', then

        relative_path('../../make_data.py')

    returns '/home/work/src/make_data.py', whereas

        relative_path('./details/sum.py')

    returns '/home/work/src/repo1/scripts/details/sum.py'.

    Alternatively, at the place where one would have used this function,
    they could use something along the lines of

        str((Path(__file__).parent / path).resolve())
    '''
    caller = inspect.getframeinfo(inspect.stack()[1][0]).filename
    assert caller.endswith('.py')
    p = Path(caller).parent
    return join_path(str(p), path)


def get_temp_file(suffix: str = '', dir: str = None) -> str:
    '''
    Get a temporary file name. The file does not exist, and is not created.

    Return the absolute path. The parent path is created if not already
    existent.

    The caller should take care to delete the temporary file after use,
    e.g. using `os.remove`.
    '''
    if dir is None:
        dir = tempfile.gettempdir()
    os.makedirs(dir, exist_ok=True)
    while True:
        relname = str(uuid.uuid4()) + suffix
        absname = os.path.join(dir, relname)
        if not os.path.isfile(absname):
            return absname


def prepare_path(path: Union[str, Path], *path_elements):
    '''
    The arguments specify a file name.
    This function makes sure the full path above the file
    exists, and returns the file path.
    '''
    if isinstance(path, str):
        ff = os.path.abspath(os.path.join(path, *path_elements))
        dirname = os.path.dirname(ff)
        if not os.path.isdir(dirname):
            os.makedirs(dirname)
    else:
        ff = Path(path, *path_elements).resolve()
        dirpath = ff.parent
        if not dirpath.is_dir():
            dirpath.mkdir()

    return ff
