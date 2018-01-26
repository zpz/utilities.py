import configparser
import os
import textwrap
from typing import Sequence

import sqlparse


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
    cleanse = lambda x: x.rstrip(' ;\n').lstrip(';\n')
    return [cleanse(x) for x in cleanse(sql).split(';\n') if x]


def pretty_sql(sql: str, lpad: str='        ') -> str:
    """
    Convert a SQL statement(s) into readable format (nicely line-split, indented, and aligned)
    suitable for printing and inspection.
    """
    sql = sqlparse.format(
        sql,
        reindent=True,
        keyword_case='upper',
    )
    sql = sql.lstrip('\n').rstrip(' \n').replace('\n\n\n', '\n\n')
    if lpad:
        sql = textwrap.indent(sql, lpad)
    return sql


class ColumnMapper(object):
    """
    Definition of mapping from columns in a source table to a single column in a target table.
    """
    def __init__(self, name, type, select=None):
        """
        Args:
            name (str): name of the resultant column.
            type (str): SQL data type of the resultant column.
            select (str): SQL expression that defines the resultant column.
                The expression typically involves columns (of a source table),
                constants, SQL built-in functions, and operators.
                If ``None``, the resultant column is a column from the source table
                with the same name.

        Examples:
            ::

                ('def', 'double')

            'def' in the original table is carried over to the new table with no change.

            ::

                ('abc', 'double', 'col1 * EXP(col2)')

            columns 'col1' and 'col2' are combined to create 'abc' in the new table.

            ::

                ('pg', 'bigint', 'st')

            'st' in the original table is renamed to 'pg' in the new table.
        """
        self.name = name
        self.type = type.upper()

        self.define = '{} {}'.format(self.name, self.type)
        """Used in table creation statements to define the column."""

        if select:
            self.select = '{} AS {}'.format(select, name)
        else:
            self.select = name
        """Used in ``SELECT`` statements to extract the column."""

    def __repr__(self):
        select = self.select.split()[0]
        if select == self.name:
            select = None
        if select:
            return "{}('{}', '{}', '{}')".format(
                self.__class__.__name__,
                self.name,
                self.type,
                select
            )
        else:
            return "{}('{}', '{}')".format(
                self.__class__.__name__,
                self.name,
                self.type
            )


class RowMapper(object):
    """Definition of mapping from a row in a source table to a row in a target table.

    A ``RowMapper`` is a list of :class:`ColumnMapper` s.
    """
    def __init__(self, *column_mappers):
        """
        Args:
            column_mappers: ``ColumnMapper`` objects or iterables
                that would serve as input to the initiator of
                :class:`ColumnMapper`.

                Examples::

                    ColumnMapper('def', 'double'), ColumnMapper('abc', 'double', 'col1 * EXP(col2)')
                    ('def', 'double'), ColumnMapper('abc', 'double', 'col1 * EXP(col2)')
                    ('def', 'double'), ('abc', 'double', 'col1 * EXP(col2)')

        """
        if column_mappers:
            self._col_mappers = [
                v if isinstance(v, ColumnMapper)
                else ColumnMapper(*v)
                for v in column_mappers]
        else:
            self._col_mappers = []

    def add_cols(self, *column_mappers):
        self._col_mappers.extend(
            v if isinstance(v, ColumnMapper)
            else ColumnMapper(*v)
            for v in column_mappers
        )
        return self

    @property
    def selects(self):
        return [x.select for x in self._col_mappers]

    @property
    def defines(self):
        return [x.define for x in self._col_mappers]

    @property
    def names(self):
        return [x.name for x in self._col_mappers]

    def map(self, table_or_sql_in):
        sql = """\
        SELECT {}
        FROM {} AS t_in
        """.format(
            ',\n'.join(self.selects),
            table_or_sql_in.as_subclause,
        )
        return pretty_sql(sql)



def select_to_table(sql_select, table_out, col_defs=None):
    """Create a table that is populated by the output of a ``SELECT`` statement.

    Args:
        sql_select (str): a ``SELECT`` statement.
        table_out (str): name of output table in the format of 'database.table'.
        col_defs (str): definition of columns, such as::

            'abc INT, efg CHAR(20)'

    Returns:
        str: a SQL statement for creating and populating the table.
    """
    if col_defs:
        sql = """\
        DROP TABLE IF EXISTS {t_out}
        ;
        CREATE TABLE {t_out} (
            {defs}
            )
            STORED AS PARQUET
        ;
        INSERT OVERWRITE TABLE {t_out}
        {select}\
        """.format(t_out=table_out, defs=col_defs, select=sql_select)
    else:
        sql = """\
        DROP TABLE IF EXISTS {t_out}
        ;
        CREATE TABLE {t_out}
            STORED AS PARQUET
        AS
        {select}\
        """.format(t_out=table_out, select=sql_select)

    return pretty_sql(sql)

