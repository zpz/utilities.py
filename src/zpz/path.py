import inspect
import os.path
from pathlib import Path
import tempfile
import uuid
from typing import Union


def join_path(base_dir: str, rel_path: str) -> str:
    '''
    Returns the absolute full path given path `rel_path` relative to the absolute path of
    directory `base_dir`.

    Args:
        `base_dir`: absolute path of a directory. This may or may not end with a trailing `/`.
            For example, `/a/b/c/d.txt` will be consdered a directory, not a file.

        `rel_path`: path relative to `base_dir`. If it starts with `/`, then it's an absolute path,
            in which case `base_dir` will be ignored.

    Returns:
        Absolute full path of `rel_path`.

    This function does not check whether the resultant path *exists*.
    It only guarantees the resultant path is *valid*.

    See the corresponding tests about the expected behaviors of this function in various
    scenarios.
    '''
    assert isinstance(base_dir, str) and base_dir.startswith(
        '/') and isinstance(rel_path, str)
    if rel_path in ['', '.', './']:
        return base_dir

    base_dir_0 = base_dir
    rel_path_0 = rel_path

    if not base_dir.endswith('/'):
        # Enforce `base_dir` to have a trailing `/`.
        base_dir = base_dir + '/'

    if rel_path == '..':
        rel_path = '../'

    if rel_path.startswith('/'):
        base_dir = '/'
        rel_path = rel_path[1:]
    elif rel_path.startswith('./'):
        # './' can occur at the beginning only, and only once.
        rel_path = rel_path[2:]
    elif rel_path.startswith('../'):
        # '../' can occur at the beginning one or more times consecutively.
        while rel_path.startswith('../'):
            if base_dir == '/':
                raise ValueError(
                    f'Invalid path operation with "{base_dir_0}" and "{rel_path_0}": trying to go up beyond "/"')
            base_dir = base_dir[:-1]
            assert not base_dir.endswith('/')
            i = base_dir.rfind('/')
            assert i >= 0
            base_dir = base_dir[: (i+1)]
            rel_path = rel_path[3:]

    while rel_path:
        i = rel_path.find('/')
        if i == 0:
            raise ValueError(
                f'Invalid operation with "{base_dir_0}" and "{rel_path_0}""')
        if i < 0:
            name = rel_path
            rel_path = ''
            base_dir = base_dir + name
        else:
            name = rel_path[:i]
            rel_path = rel_path[(i+1):]
            base_dir = base_dir + name + '/'

        if all(e == '.' for e in name):
            raise ValueError(
                f'Invalid operation with "{base_dir_0}" and "{rel_path_0}""')

    return base_dir


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


def get_temp_file(dir: str = None, ext: str = '') -> str:
    '''
    Get a temporary file name. The file does not exist, and is not created.

    Return the absolute path if `dir` is not specified;
    otherwise return a file name under that directory.

    The caller should take care to delete the temporary file after use,
    e.g. using `os.remove`.
    '''
    if dir is None:
        dir = tempfile.gettempdir()
        return_abs_name = True
    else:
        assert dir.startswith('/')
        assert os.path.isdir(dir)
        return_abs_name = False
    if ext:
        ext = '.' + ext
    while True:
        relname = str(uuid.uuid4()) + ext
        absname = os.path.join(dir, relname)
        if not os.path.isfile(absname):
            if return_abs_name:
                return absname
            else:
                return relname


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
