import os

from coyote.path import get_temp_file
from coyote.config import read_ini_config


CONFIG = '''
[section1]
option1 = option 1
# some comments
; more comments

option2 =

option3

option4 = some value


option5

# some comment

[section 2]
option1 = abc def


[special]
separator = ;
bad = no # comment
noescape = has; in middle
'''


def test_get():
    conf_file = get_temp_file()
    open(conf_file, 'w').write(CONFIG)

    try:
        conf = read_ini_config(conf_file)

        assert 'option 1' == conf.get('section1', 'option1')
        assert '' == conf.get('section1', 'option2')
        assert conf.get('section1', 'option3') is None
        assert 'some value' == conf.get('section1', 'option4')

        sec = conf['section 2']
        assert 'abc def' == sec.get('option1')
        assert 'abc def' == sec['option1']

        assert ';' == conf.get('special', 'separator')
        assert 'no # comment' == conf.get('special', 'bad')
        assert 'has; in middle' == conf.get('special', 'noescape')

        sec = read_ini_config(conf_file, 'section 2')
        assert 'abc def' == sec.get('option1')
    finally:
        os.remove(conf_file)
