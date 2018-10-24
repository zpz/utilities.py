from utilities.config import read_ini_config

def test_get():
    conf = read_ini_config('test.conf')

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
