import logging
from pprint import pprint
from typing import List, Tuple

from ..s3 import Bucket


logger = logging.getLogger(__name__)


class HiveTableMixin:
    def __init__(self, *,
                 db_name: str,
                 tb_name: str,
                 columns: List[Tuple[str, str]],
                 partitions: List[Tuple[str, str]] = None,
                 stored_as: str = 'ORC',
                 field_delimiter: str = '\\t',
                 compression: str = 'ZLIB',
                 location: str = None) -> None:
        self.db_name = db_name
        self.tb_name = tb_name
        self.columns = [(name, type_.upper()) for (name, type_) in columns]
        if partitions:
            self.partitions = [(name, type_.upper())
                               for (name, type_) in partitions]
        else:
            self.partitions = []

        stored_as = stored_as.upper()
        assert stored_as in ('ORC', 'PARQUET', 'TEXTFILE')
        self.stored_as = stored_as
        self.field_delimiter = field_delimiter
        self.compression = compression

        self.external = False
        self.s3external = False
        if location:
            self.location = location.rstrip('/') + '/'   # Ensure trailing '/'
            loc = self.location
            if loc.startswith('s3://') or loc.startswith('s3n://'):
                if loc.startswith('s3://'):
                    z = self.location[len('s3://'):]
                else:
                    z = self.location[len('s3n://'):]
                assert '/' in z
                self._s3_bucket_key = z[: z.find('/')]
                self._s3_bucket_path = z[(z.find('/') + 1):]
                self.s3external = True
            self.external = True

    @ property
    def full_name(self):
        return self.db_name + '.' + self.tb_name

    def create(self, engine, drop_if_exists: bool = False) -> None:
        '''
        `engine` is a `Hive` or `Athena` object.
        '''
        def collapse(spec):
            return ', '.join(name + ' ' + type_ for (name, type_) in spec)

        columns = collapse(self.columns)

        if self.partitions:
            partitions = f"PARTITIONED BY ({collapse(self.partitions)})"
        else:
            partitions = ''

        if self.external:
            location = f"LOCATION '{self.location}'"
            external = 'EXTERNAL'
        else:
            location = ''
            external = ''

        if self.stored_as in ('ORC', 'PARQUET'):
            stored_as = f'''
                STORED AS {self.stored_as}
                {location}
                TBLPROPERTIES ('{self.stored_as.lower()}.compress' = '{self.compression}')
                '''
        else:
            stored_as = f'''
                ROW FORMAT DELIMITED FIELDS TERMINATED BY '{self.field_delimiter}'
                STORED AS {self.stored_as}
                {location}
                '''

        sql = f'''
            CREATE {external} TABLE {self.full_name}
            ({columns})
            {partitions}
            {stored_as}
            '''

        if drop_if_exists:
            self.drop(engine)

        engine.write(sql)
        if self.external:
            engine.write(f'MSCK REPAIR TABLE {self.full_name}')

    def drop(self, engine) -> None:
        engine.write(f'DROP TABLE IF EXISTS {self.full_name}')

    def _partition_values_to_path(self, *partition_values) -> str:
        assert 0 < len(partition_values) <= len(self.partitions)
        return '/'.join(f"{k}={v}" for v, (k, _) in zip(partition_values, self.partitions[: len(partition_values)]))

    def _partition_values_to_condition(self, *partition_values) -> str:
        '''
        `partition_values` appear in the order they are listed
        in the table definition.
        '''
        assert 0 < len(partition_values) <= len(self.partitions)
        elements = []
        for v, (k, t) in zip(partition_values, self.partitions[: len(partition_values)]):
            if t.lower() in ('string', 'char', 'varchar'):
                elements.append(f"{k}='{v}'")
            else:
                elements.append(f"{k}={v}")
        return ', '.join(elements)

    def get_partitions(self, engine, *partition_values) -> List[str]:
        if not self.partitions:
            raise Exception('not a partitioned table')
        sql = f'SHOW PARTITIONS {self.full_name}'
        if partition_values:
            cond_str = self._partition_values_to_condition(*partition_values)
            sql += f' PARTITION({cond_str})'
        z = engine.read(sql).fetchall()
        return [v[0] for v in z]

    def show_partitions(self, engine, *partition_values) -> None:
        pprint(sorted(self.get_partitions(engine, *partition_values)))

    def show_partition_counts(self, engine, *partition_values):
        if not self.partitions:
            raise Exception('not a partitioned table')
        cols = ', '.join(v[0] for v in self.partitions)
        sql = f'''
            SELECT
                {cols},
                COUNT(*) AS count
            FROM {self.full_name}'''
        if partition_values:
            cond_str = self._partition_values_to_condition(*partition_values)
            sql = sql + f'''
                WHERE {cond_str}'''
        sql = sql + f'''
            GROUP BY {cols}
            ORDER BY {cols}'''
        z = engine.read(sql).fetchall_pandas()
        print(z)

    def drop_partitions(self, engine, *partition_values):
        if partition_values:
            cond_str = self._partition_values_to_condition(*partition_values)
            sql = f"ALTER TABLE {self.full_name} DROP IF EXISTS PARTITION({cond_str})"
            engine.write(sql)
        else:
            logger.warning(
                f'dropping all partitions in table {self.full_name}')
            parts = self.get_partitions(engine)
            for p in parts:
                cond_str = p.split('/')[0]
                sql = f"ALTER TABLE {self.full_name} DROP IF EXISTS PARTITION({cond_str})"
                engine.write(sql)

    def add_partitions(self, engine, *partition_values):
        '''
        Add partitions for an external table to the table meta data.

        Use case is adding partition to the meta data of an external table
        after new data has been added w/o the knowledge of the meta store.

        `*partition_values`: values of first partition columns; update below that.

        If empty, update under root location.
        '''
        assert self.s3external

        existing_partitions = self.get_partitions(engine, *partition_values)
        if not partition_values:
            root = self.location
        else:
            parts_path_str = self._partition_values_to_path(*partition_values)
            parts_cond_str = self._partition_values_to_condition(
                *partition_values)
            root = self.location + parts_path_str + '/'

        bucket = Bucket(self._s3_bucket_key)
        files = bucket.ls(root, recursive=True)

        if len(partition_values) == len(self.partitions):
            if list(files):
                sql = f'''ALTER TABLE {self.full_name} ADD PARTITION({parts_cond_str}) LOCATION '{self.location + parts_path_str}' '''
                logger.debug(sql)
                engine.write(sql)
            return

        dirs = set(v[: v.rfind('/')]
                   for v in files if '/' in v)  # no trailing '/'
        for dd in dirs:
            path_str = dd
            if partition_values:
                path_str = parts_path_str + '/' + path_str
            if path_str not in existing_partitions:
                cond_str = ', '.join(f"{k}='{v}'" for k, v in (
                    v.split('=') for v in dd.split('/')))
                if partition_values:
                    cond_str = parts_cond_str + ', ' + cond_str
                sql = f'''ALTER TABLE {self.full_name} ADD PARTITION({cond_str}) LOCATION '{self.location + path_str}' '''
                logger.debug(sql)
                engine.write(sql)

    def update_partitions(self, engine, *partition_values):
        assert self.s3external
        self.drop_partitions(engine, *partition_values)
        self.add_partitions(engine, *partition_values)

    def partition_location(self, *partition_values):
        if not self.s3external:
            raise NotImplementedError
        if not partition_values:
            return self.location
        path = self.location + \
            self._partition_values_to_path(*partition_values) + '/'
        return path

    def purge_data(self, *partition_values) -> int:
        if not self.s3external:
            raise NotImplementedError
        bucket = Bucket(self._s3_bucket_key)
        path = self.partition_location(*partition_values)
        return bucket.delete_tree(path)


class HiveTable(HiveTableMixin):
    def __init__(self, location: str = None, **kwargs) -> None:
        if location:
            assert location.startswith('s3n://')
        super().__init__(**kwargs, location=location)

    def to_athena_table(self, db_name: str, tb_name: str = None) -> 'AthenaTable':
        '''
        Use case of this method:

            If we have defined and manipulated an _external_ table in Hive,
            then the data is in S3 but meta data is in Hadoop.
            This table is not available to Athena because the meta data is not in S3.
            The current method takes the existing Hive (external) table definition
            and creates an AthenaTable object, so that the table's meta data is in sync
            with the Hive table. After calling `create` on the AthenaTable object,
            the table is present in the S3 meta data store, so that the table can be used
            by Athena.

            The table meta data in Hadoop and in S3 are independent of each other.
            Therefore if the table's data is modified by either side of Hive and Athena,
            the other side needs to update partitions.
        '''
        from .athena import AthenaTable
        assert self.s3external
        location = self.location
        if location.startswith('s3n://'):
            location = 's3://' + location[len('s3n://'):]
        return AthenaTable(
            db_name=db_name,
            tb_name=tb_name or self.tb_name,
            columns=self.columns,
            partitions=self.partitions,
            stored_as=self.stored_as,
            field_delimiter=self.field_delimiter,
            compression=self.compression,
            location=location)

    @ classmethod
    def from_athena_table(cls, table: 'AthenaTable', db_name: str, tb_name: str = None) -> 'HiveTable':
        '''
        Use case is analogous to `to_athena_table`.
        '''
        assert table.s3external
        location = table.location
        if location.startswith('s3://'):
            location = 's3n://' + location[len('s3://'):]
        return cls(
            db_name=db_name,
            tb_name=tb_name or table.tb_name,
            columns=table.columns,
            partitions=table.partitions,
            stored_as=table.stored_as,
            field_delimiter=table.field_delimiter,
            compression=table.compression,
            location=location)
