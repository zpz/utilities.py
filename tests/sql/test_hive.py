import pytest

from sqlink.util import read_ini_config

from sqlink.hive import HiveReader, HiveWriter

from .common import *


@pytest.fixture(scope='module')
def reader():
    return HiveReader(**read_ini_config('system.cfg')['hive-server'])


@pytest.fixture(scope='module')
def writer():
    return HiveWriter(**read_ini_config('system.cfg')['hive-server'])


def test_hive_read(reader):
    do_read(reader)


def test_hive_read_iter(reader):
    do_read_iter(reader)


def test_hive_write(reader, writer):
    do_write(reader, writer)

