from pathlib import Path

import boto3

# This module requires a directory `.aws/` containing credentials in the home directory.
# Or environment variables `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY`.


def _get_client():
    return boto3.session.Session().client('s3')


def _has_key(s3_client, bucket: str, key: str) -> bool:
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
    for obj in response.get('Contents', []):
        if obj['Key'] == key:
            return True
    return False


def _delete_key(s3_client, bucket: str, key: str) -> None:
    s3_client.delete_object(Bucket=bucket, Key=key)


def has_key(bucket: str, key: str) -> bool:
    return _has_key(_get_client(), bucket, key)


def delete_key(bucket: str, key: str) -> None:
    return _delete_key(_get_client(), bucket, key)


class Bucket:
    def __init__(self, bucket):
        self._bucket = boto3.resource('s3').Bucket(bucket)

    def upload(self, local_file: str, s3_key: str) -> None:
        '''
        Upload a single file to S3.

        `local_file`: path to local file.
        `s3_key`: S3 'key'.
        
        Example: suppose current bucket is s3://my-company, with

        local_file: /home/zepu/work/data/xyz/memo.txt
        s3_key: cool_idea/memo
        --> remote file: s3://my-company/cool_idea/memo

        Existing file with the same name with be overwritten.
        '''
        local_file = Path(local_file)
        if not local_file.is_file():
            raise Exception('a file name is expected')
        data = open(local_file, 'rb')
        self._bucket.put_object(Key=s3_key, Body=data)

    def upload_tree(self, local_path: str, s3_path: str,
                    pattern: str = '**/*') -> None:
        '''
        `local_path`: directory whose content will be uploaded.
            If `local_path` contains a trailing `/`, then no part of this path name
            becomes part of the remote name; otherwise, the final node in this path name
            becomes the leading segment of the remote name.
        `pattern`: 
            '*'    (everything directly under `local_path`),
            '**/*' (everything recursively under `local_path`),
            '*.py' (every Python module directly under `local_path`),
            '**/*.py' (every Python module recursively under `local_path`),
            etc.

        Example: suppose current bucket is s3://my-company, with
        
        local_path: /home/zepu/work/data/xyz, containing
            .../xyz/a.txt, 
            .../xyz/b.txt,
            ../xyz/zyx/aa.txt)
        s3_path: cool_idea
        s3_name: '**/*'
        --> remote files: 
            s3://my-company/cool_idea/xyz/a.txt
            s3://my-company/cool_idea/xyz/b.txt
            s3://my-compnay/cool_idea/xyz/zyx/aa.txt
            
        local_path: /home/zepu/work/data/xyz/ (note the trailing '/')
        --> remote files: 
            s3://my-company/cool_idea/a.txt
            s3://my-company/cool_idea/b.txt
            s3://my-company/cool_idea/zyx/aa.txt
        '''
        with_root = not local_path.endswith('/')
        local_path = Path(local_path)
        if not local_path.is_dir():
            raise Exception('a directory name is expected')
        nodes = [v for v in local_path.glob(pattern) if v.is_file()]
        for node in nodes:
            key = node.relative_to(local_path)
            if with_root:
                key = local_path.name / key
            key = s3_path / key
            self.upload(node, str(key))

    def download(self, s3_key: str, local_file: str = None) -> None:
        if local_file is None:
            local_file = str(Path(s3_key).name)
        self._bucket.download_file(s3_key, local_file)

    def download_tree(self, s3_path: str, local_path: str = None) -> None:
        raise NotImplementedError

    def has(self, key: str) -> bool:
        if not hasattr(self, '_s3'):
            self._s3 = _get_client()
        return _has_key(self._s3, self._bucket.name, key)

    def delete(self, key: str) -> None:
        if not hasattr(self, '_s3'):
            self._s3 = _get_client()
        _delete_key(self._s3, self._bucket.name, key)

    def delete_tree(self, s3_path: str) -> None:
        raise NotImplementedError