import logging
import textwrap
from typing import Sequence

import sqlparse

logger = logging.getLogger(__name__)


def cleanse_sql(x):
    return x.strip().strip('\n').strip().strip(';').strip()


def split_sql(sql: str) -> Sequence[str]:
    """
    Split a sequence of SQL statements into individual statements.

    The input is a block of text formatted like ::

        CREATE TABLE ...
        ;
        INSERT INTO ...
        SELECT ...
        ;
        DROP TABLE ...


    ``;\\n`` is treated as the separator between SQL statements.

    For each single SQL statement in the resultant list,
    trailing ';', line break, and spaces are removed;
    leading ';' and line break are removed;
    leading spaces are not removed b/c that may be part of intentional formatting
    for alignment.
    """
    sql.replace('\n\n', '\n')
    z = [cleanse_sql(x) for x in cleanse_sql(sql).split(';\n') if x]
    return [x for x in z if x]


def pretty_sql(sql: str, lpad: str = '        ', keyword_case=None) -> str:
    """
    Convert a SQL statement(s) into readable format (nicely line-split, indented, and aligned)
    suitable for printing and inspection.
    """
    assert keyword_case in (None, 'upper', 'lower')
    sql = sqlparse.format(
        cleanse_sql(sql),
        reindent=True,
        keyword_case=keyword_case,
        strip_whitespace=True,
    )
    sql = sql.lstrip('\n').rstrip(' \n').replace('\n\n\n', '\n\n')
    if lpad:
        sql = textwrap.indent(sql, lpad)
    return sql
