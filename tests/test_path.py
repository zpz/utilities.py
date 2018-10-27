from pathlib import PurePath

from zpz.path import relative_path


def test():
    this = PurePath(__file__).parent

    z = relative_path('../src/zpz/path.py')
    assert z == str(this.parent.joinpath('src/zpz/path.py'))

    p = './test_avro.py'
    z = relative_path(p)
    assert z == str(this.joinpath(p))