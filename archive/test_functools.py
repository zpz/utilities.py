from zpz._functools import classproperty


class MyClass:
    @classproperty
    def age(self):
        return 18

    @classproperty
    def likes(cls):
        return {'food': 'rice'}


def test_classproperty():
    myclass = MyClass()
    assert MyClass.age == 18
    assert MyClass.likes == {'food': 'rice'}
    assert myclass.age == 18
    assert myclass.likes == {'food': 'rice'}

    x = MyClass.likes
    x['food'] = 'banana'
    assert MyClass.likes == {'food': 'rice'}
    assert myclass.likes == {'food': 'rice'}

    # the following are not always desirable:
    MyClass.likes = 'abc'
    assert MyClass.likes == 'abc'
    myclass.likes == 'abc'
