import json
import pkgutil
import random
import textwrap
import time
from typing import Union, Optional, Iterable, Iterator

import pandas as pd
import requests

# Ideas are borrowed from the python package 'pylivy'.


class SparkSessionError(Exception):
    def __init__(self, name, value, traceback, kind) -> None:
        self._name = name
        self._value = value
        self._tb = traceback
        self._kind = kind
        super().__init__(name, value, traceback)

    def __str__(self) -> str:
        kind = 'Spark' if self._kind == 'spark' else 'PySpark'
        return '{}: {} error while processing submitted code.\nname: {}\nvalue: {}\ntraceback:\n{}'.format(
            self.__module__ + '.' + self.__class__.__name__, kind, self._name,
            self._value, ''.join(self._tb))


def _get_livy_host(livy_server_url: str):
    '''
    `livy_server_url` is server IP including port number, like '1.2.3.4:8998'.
    '''
    # remove protocol
    idx = livy_server_url.find('://')
    if idx >= 0:
        livy_server_url = livy_server_url[(idx + 3):]

    return 'http://' + livy_server_url


def unquote(s: str) -> str:
    return s[1:][:-1]  # remove the quotes in the string value


def polling_intervals(start: Iterable[float],
                      rest: float,
                      max_duration: float = None) -> Iterator[float]:
    def _intervals():
        yield from start
        while True:
            yield rest

    cumulative = 0.0
    for interval in _intervals():
        cumulative += interval
        if max_duration is not None and cumulative > max_duration:
            break
        yield interval


class JsonClient:
    def __init__(self, url: str) -> None:
        self.url = url
        self.session = requests.Session()
        self.session.headers.update({'X-Requested-By': 'ambari'})

    def close(self) -> None:
        self.session.close()

    def get(self, endpoint: str = '') -> dict:
        return self._request('GET', endpoint)

    def post(self, endpoint: str, data: dict = None) -> dict:
        return self._request('POST', endpoint, data)

    def delete(self, endpoint: str = '') -> dict:
        return self._request('DELETE', endpoint)

    def _request(self, method: str, endpoint: str, data: dict = None) -> dict:
        url = self.url.rstrip('/') + endpoint
        response = self.session.request(method, url, json=data)
        response.raise_for_status()
        return response.json()


class SparkSession:
    def __init__(self,
                 livy_server_url,
                 kind: str = 'spark'):
        '''
        When `kind` is 'spark', this takes Scala code.
        When `kind` is 'pyspark', this takes Python code.
        '''
        if kind == 'scala':
            kind = 'spark'
        assert kind in ('spark', 'pyspark')

        self._host = _get_livy_host(livy_server_url)
        self._client = JsonClient(self._host)
        self._kind = kind
        self._start()

    def _wait(self, endpoint: str, wait_for_state: str):
        intervals = polling_intervals([0.1, 0.2, 0.3, 0.5], 1.0)
        while True:
            rr = self._client.get(endpoint)
            if rr['state'] == wait_for_state:
                return rr
            time.sleep(next(intervals))

    def _start(self):
        r = self._client.post('/sessions', data={'kind': self._kind})
        session_id = r['id']
        self._session_id = session_id
        self._wait('/sessions/{}'.format(session_id), 'idle')

    def run(self, code: str) -> str:
        data = {'code': textwrap.dedent(code)}
        r = self._client.post(
            '/sessions/{}/statements'.format(self._session_id), data=data)
        statement_id = r['id']
        z = self._wait(
            '/sessions/{}/statements/{}'.format(self._session_id,
                                                statement_id), 'available')
        output = z['output']
        if output['status'] == 'error':
            raise SparkSessionError(output['ename'], output['evalue'],
                                    output['traceback'], self._kind)
        assert output['status'] == 'ok'
        return output['data']['text/plain']

    def __del__(self):
        if hasattr(self, '_session_id'):
            try:
                self._client.delete('/sessions/{}'.format(self._session_id))
            finally:
                pass


class ScalaSparkSession(SparkSession):
    def __init__(self, livy_server_url):
        super().__init__(livy_server_url, kind='spark')



class PySparkSession(SparkSession):
    def __init__(self, livy_server_url):
        super().__init__(livy_server_url, kind='pyspark')
        self._tmp_var_name = '_tmp_' + str(random.randint(100, 100000))
        self.run('import pyspark; import json')

    def run_module(self, module_name: str) -> None:
        '''
        `module_name` is an absolute module name, meaning it's not relative
        to where this code is called. The named module must be reachable
        on the system path via `import`

        Run the source of a Python module in Spark.
        Functions and variables defined in that module are available to subsequent code.
        This module must depend on only Python's stblib and Spark models (plus other modules
        you are sure exist on the Spark cluster). Modules loaded this way should not 'import'
        each other. Imagine the source code of the module is copied and submitted to Spark,
        and the module name is lost.

        The named module is written against the Python version and packages available on the
        Spark cluster. The module is NOT imported by this method; it's entirely possible that
        loading the module in the current environment (not on the Spark cluster) would fail.
        The current method retrieves the source code of the module as text and sends it to Spark.

        Similar concerns about dependencies apply to `run_file`.

        Example (assuming it is meaningful to submit the module `coyote.spark` to Spark):

            sess = PySparkSession()
            sess.run_module('coyote.spark')

        The module shoule 'define' functions, variables only; it should not actually run
        and return anything meaningful.
        '''
        pack = pkgutil.get_loader(module_name)
        self.run_file(pack.path)

    def run_file(self, file_path: str) -> str:
        '''
        `file_path` the absolute path to a Python (script or module) file.

        When you know the relative location of the file, refer to `coyote.path.relative_path`
        and test case `test_file` in `tests/test_spark.py`.

        This returns a string, which is not supposed to be very useful.
        If one wants to capture the result in type-controlled ways, it's better
        to let the file define functions only, then call `fun_file` followed by
        `run_function`.
        '''
        assert file_path.startswith('/')
        code = open(file_path, 'r').read()
        return self.run(code)

    def run_function(self, fname: str, *args, **kwargs):
        '''
        `fname`: name of a function existing in the Spark session environment.

        `args`, `kwargs`: simple arguments to the function, such as string, number, short list.
        '''
        code = f'{fname}(*{args}, **{kwargs})'
        return self.read(code)

    def read(self, code: str):
        '''
        `code` is a variable name or a single, simple expression in the Spark session.        
        '''
        code = '{} = {}; type({}).__name__'.format(self._tmp_var_name, code,
                                                   self._tmp_var_name)
        z_type = unquote(self.run(code))

        if z_type in ('int', 'float', 'bool'):
            z = self.run(self._tmp_var_name)
            return eval(z_type)(z)

        if z_type == 'str':
            z = self.run(self._tmp_var_name)
            return unquote(z)

        if z_type == 'DataFrame':
            code = """\
                for _livy_client_serialised_row in {}.toJSON().collect():
                    print(_livy_client_serialised_row)""".format(
                self._tmp_var_name)
            output = self.run(code)

            rows = []
            for line in output.split('\n'):
                if line:
                    rows.append(json.loads(line))
            z = pd.DataFrame.from_records(rows)
            return z

        try:
            z = self.run('json.dumps({})'.format(self._tmp_var_name))
            zz = json.loads(unquote(z))
            return eval(z_type)(zz)
        except Exception as e:
            raise Exception(
                'failed to fetch an "{}" object: {};  additional error info: {}'.
                format(z_type, z, e))
