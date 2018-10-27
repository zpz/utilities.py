import inspect
from pathlib import PurePath


def relative_path(path: str) -> str:
    '''
    Given path `path` relative to the file where this function is called,
    return absolute path.

    For example, suppose this function is called in file '/home/work/src/repo1/scripts/abc.py', then

        relative_path('../../make_data.py')

    returns '/home/work/src/make_data.py', whereas

        relative_path('./details/sum.py')

    returns '/home/work/src/repo1/scripts/details/sum.py'.
    '''
    if path.startswith('/'):
        return path

    caller = inspect.getframeinfo(inspect.stack()[1][0]).filename
    assert caller.endswith('.py')
    p = PurePath(caller).parent
    while True:
        if path.startswith('./'):
            path = path[2:]
        elif path.startswith('../'):
            path = path[3:]
            p = p.parent
        else:
            break
    return str(p.joinpath(path))