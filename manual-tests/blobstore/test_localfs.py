import os
from pathlib import Path
import pytest
import shutil
import uuid

from coyote.filesys.localfs import LocalFS


ROOT = str(Path.home().joinpath('tmp', str(uuid.uuid4()))) + '/'

def clear(fs_):
    pwd = fs_.pwd
    if Path(pwd).is_dir():
        shutil.rmtree(pwd)
    os.mkdir(pwd)

@pytest.fixture(scope='module')
def fs():
    f = LocalFS().cd(ROOT)
    clear(f)
    yield f
    f.cd(ROOT)
    clear(f)
    os.rmdir(ROOT)


def test_put_get_text(fs):
    fs.cd(ROOT)
    clear(fs)

    fs.put_text('abc', 'ab/cd/x.txt')
    assert fs.exists('ab/cd/x.txt')
    fs.get_text('ab/cd/x.txt') == 'abc'
    fs.rm('ab/cd/x.txt')
    assert not fs.exists('ab/cd/x.txt')