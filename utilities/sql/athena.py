import os

import pyathena
from .sql import SQLClient


class Athena(SQLClient):
    def __init__(self,
                 s3_result_dir: str=None,
                 *,
                 aws_access_key_id: str=None,
                 aws_secret_access_key: str=None,
                 region_name: str=None) -> None:
        # `s3_result_dir` is where Athena query results are stored.
        # The default location is used if not specified.
        # `aws_access_key_id`, `aws_secret_access_key`, `region_name` will get values from
        # environment variables if not specified.
        if region_name is None:
            region_name = os.environ['AWS_DEFAULT_REGION']
        if s3_result_dir is None:
            s3_result_dir = 's3://aws-athena-query-results-{}-{}'.format(
                os.environ['AWS_ACCOUNT_ID'], region_name
            )
        else:
            assert s3_result_dir.startswith('s3://')
        super().__init__(
            conn_func=pyathena.connect,
            aws_access_key_id=aws_access_key_id or os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=aws_secret_access_key or os.environ['AWS_SECRET_ACCESS_KEY'],
            s3_staging_dir=s3_result_dir,
            region_name=region_name or os.environ['AWS_DEFAULT_REGION'],
            cursor_arraysize=1000,  # 1000 is Athena's upper limit
            )