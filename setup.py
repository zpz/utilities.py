import pathlib
from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent

README = (HERE / 'README.md').read_text()

full = '0'
exec((HERE / 'src/zpz/version.py').read_text(), globals())
version = full
del full

setup(
    name='zpz',
    version=version,
    description='Various Python utilities',
    long_description=README,
    long_description_content_type='text/markdown',
    url='https://github.com/zpz/utilities.py',
    license='BSD',
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    include_package_data=True,
)
