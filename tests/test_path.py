from pathlib import PurePath
import os
import os.path
import pytest
from zpz.path import join_path, relative_path
from zpz.path import get_temp_file
from zpz.exceptions import ZpzError


def test_join_path():
    for base_dir in ('/a/b/c/d', '/a/b/c/d/'):
        assert join_path(base_dir, '') == base_dir
        assert join_path(base_dir, '.') == base_dir
        assert join_path(base_dir, './') == base_dir
        assert join_path(base_dir, '..') == '/a/b/c/'
        assert join_path(base_dir, '../') == '/a/b/c/'
        assert join_path(base_dir, 'xy.txt') == '/a/b/c/d/xy.txt'
        assert join_path(base_dir, './xy.txt') == '/a/b/c/d/xy.txt'
        assert join_path(base_dir, './x/y/z') == '/a/b/c/d/x/y/z'
        assert join_path(base_dir, './x/y/z/') == '/a/b/c/d/x/y/z/'
        assert join_path(base_dir, '../../x/y.txt') == '/a/b/x/y.txt'
        assert join_path(base_dir, '../../../../x/y/z.txt') == '/x/y/z.txt'
        assert join_path(base_dir, 'x/.y') == '/a/b/c/d/x/.y'

        assert join_path(base_dir, '/x/y/z.txt') == '/x/y/z.txt'

        with pytest.raises(ZpzError) as e:
            z = join_path(base_dir, '../../../../../x')

        with pytest.raises(ZpzError) as e:
            z = join_path(base_dir, '.././x')

        with pytest.raises(ZpzError) as e:
            z = join_path(base_dir, './../x')

        with pytest.raises(ZpzError) as e:
            z = join_path(base_dir, '../../x/y/../z.txt')

        with pytest.raises(ZpzError) as e:
            z = join_path(base_dir, '../../x/y/./z.txt')

        with pytest.raises(ZpzError) as e:
            z = join_path(base_dir, '../../x/y//z.txt')


def test_relative_path():
    this = PurePath(__file__).parent

    z = relative_path('../src/zpz/path.py')
    assert z == str(this.parent.joinpath('src/zpz/path.py'))

    p = './test_avro.py'
    z = relative_path(p)
    assert z == str(this.joinpath(p))


def test_temp_file():
    z = get_temp_file()
    print(z)
    assert z.startswith('/')
    assert not os.path.exists(z)
    with open(z, 'w') as f:
        f.write('abc')
    assert os.path.isfile(z)
    assert open(z).read() == 'abc'
    os.remove(z)
    assert not os.path.exists(z)

    z = get_temp_file('/tmp')
    print(z)
    assert '/' not in z
    assert not os.path.exists(z)
    with open(z, 'w') as f:
        f.write('abcd')
    assert os.path.isfile(z)
    assert os.path.isfile(z)
    assert open(z).read() == 'abcd'
    os.remove(z)
    assert not os.path.exists(z)