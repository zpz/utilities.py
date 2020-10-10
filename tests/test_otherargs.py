from argparse import ArgumentParser
from coyote.otherargs import parse_other_args


def test_otherargs():
    parser = ArgumentParser()
    parser.add_argument('x', type=int)
    parser.add_argument('--name')

    args, otherargs = parser.parse_known_args(
        ['38', '--name', 'peter', '--age', '44', '--title', 'president',
         '--show-details']
    )
    otherargs = parse_other_args(otherargs)
    assert otherargs == {
        'age': 44, 'title': 'president', 'show_details': True,
    }
