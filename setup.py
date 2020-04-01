import pathlib
from setuptools import setup, find_packages

# The code below assumes the package to be installed is under `src/`
# in the repo, which is the recommended practice. If you follow this
# recommendation, then typically this script needs no change.


HERE = pathlib.Path(__file__).parent


def get_repo_name():
    name = ''
    if (HERE / '.git').exists():
        substr = 'github.com'
        for line in open(HERE / '.git' / 'config'):
            if substr in line:
                line = line.strip()
                idx = line.rindex('/')
                assert idx > 0
                name = line[(idx+1) :]
                if name.endswith('.git'):
                    name = name[:-4]
                break
    else:
        name = HERE.resolve().name
        folders = list((HERE / 'src').glob('*'))
        folders = [v.name for v in folders if v.is_dir() and not str(v).endswith('.egg-info')]
        if len(folders) > 1:
            raise Exception('Expecting a single directory in "src/" to infer package name')
        name = folders[0].name
    assert name
    return name

name = get_repo_name()

setup(
    name=name,
    description='Package ' + name,
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    include_package_data=True,
)
