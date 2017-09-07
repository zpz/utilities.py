import configparser

import pytest

from utilities.testing import config_test
from utilities.config import ConfigFileNotFoundError, ConfigFileExtWarning, Configger

config_test()


def test_file():
    with pytest.warns(ConfigFileExtWarning):
        conf2 = Configger('utilities/test.conf')

    with pytest.raises(ConfigFileNotFoundError):
        conf = Configger('utilities/myrandomtest')


def test_set():
    conf = Configger('utilities/test.cfg')

    with pytest.raises(configparser.NoSectionError):
        conf.set('section88', 'age', '38')

    if 'age' in conf['section1']:
        del conf['section1']['age']
    conf.save()

    with pytest.raises(configparser.NoOptionError):
        age = conf.get('section1', 'age')

    conf.set('section1', 'age', '38')

    # new option is not persisted yet
    with pytest.raises(configparser.NoOptionError):
        conf11 = Configger('utilities/test.cfg')
        age = conf11.get('section1', 'age')

    conf.save()

    conf2 = Configger('utilities/test.cfg')
    expected = 38
    assert expected == conf2.getint('section1', 'age')

    conf['section1']['age'] = '39'
    conf.save()

    assert expected == conf2['section1'].getint('age')

    conf3 = Configger('utilities/test.cfg')
    expected = 39
    assert expected == int(conf3['section1']['age'])

    del conf['section1']['age']
    conf.save()

    with pytest.raises(configparser.NoOptionError):
        conf21 = Configger('utilities/test.cfg')
        age = conf21.get('section1', 'age')


def test_get():
    conf = Configger('utilities/test.cfg')

    assert 'option 1' == conf.get('section1', 'option1')
    assert '' == conf.get('section1', 'option2')
    assert conf.get('section1', 'option3') is None
    assert 'some value' == conf.get('section1', 'option4')

    sec = conf.section('section 2')
    assert 'abc def' == sec.get('option1')
    assert 'abc def' == sec['option1']

    assert ';' == conf.get('special', 'separator')
    assert 'no # comment' == conf.get('special', 'bad')
    assert 'has; in middle' == conf.get('special', 'noescape')
