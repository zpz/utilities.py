'''
This is an example UDF for Hive, to be processed by `zpz.sql.hive.make_udf` and used in tests.

This module should be *self sufficient* except for using Python standard libraries.

The code in this module is written against the Python version that is available on the Hive cluster.
'''

import json
import sys

SEP = '\t'
NULL = '\\N'


def main(line):
    _, info_json = line.strip().split(SEP)
    info = json.loads(info_json)
    return info['make'] + SEP + str(info.get('price', NULL))


if __name__ == '__main__':
    for line in sys.stdin:
        print(main(line))