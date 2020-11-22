import io
import logging
from time import perf_counter
from typing import Union

import httpcore
import httpx
from tenacity import (
    retry, stop_after_attempt,
    wait_random_exponential, retry_if_exception_type)

from .json import (
    orjson_dumps, orjson_loads,
    orjson_z_dumps, orjson_z_loads)
from .pickle import (
    pickle_dumps, pickle_loads,
    pickle_z_dumps, pickle_z_loads)

logger = logging.getLogger(__name__)


class ClientTimeout(httpx.Timeout):
    '''
    Either set a single number for `timeout`, which will be
    applied to each specific timeout, or set the specific timeouts
    individually.
    '''

    def __init__(self,
                 timeout: float = None,
                 *,
                 connect: float = None,
                 read: float = None,
                 write: float = None,
                 ):
        if not timeout:
            timeout = 60.0
        super().__init__(
            timeout=timeout,
            connect=connect or timeout,
            read=read or timeout * 5,
            write=write or timeout,
        )


class AsyncClientSession(httpx.AsyncClient):
    def __init__(self,
                 *,
                 timeout: Union[ClientTimeout, float, int] = None,
                 **kwargs):
        if not isinstance(timeout, ClientTimeout):
            timeout = ClientTimeout(timeout)
        super().__init__(timeout=timeout, **kwargs)


@retry(
    stop=stop_after_attempt(10),
    wait=wait_random_exponential(multiplier=1, max=60),
    retry=retry_if_exception_type((
        httpx.TimeoutException, httpcore.TimeoutException,
        httpx.RemoteProtocolError, httpcore.RemoteProtocolError,
    ))
)
async def _a_request(func, url, **Kwargs):
    time0 = perf_counter()
    try:
        response = await func(url, **kwargs)
        return response
    except (httpx.TimeoutException, httpcore.TimeoutException) as e:
        time1 = perf_counter()
        timeout_duration = time1 - time0
        logger.error(
            "HTTP request timed out after %d seconds with %s: %s",
            timeout_duration, e.__class__.__name__, str(e)
        )
        raise
    except (httpx.RemoteProtocolError, httpcore.RemoteProtocolError) as e:
        time1 = perf_counter()
        timeout_duration = time1 - time0
        logger.error(
            "HTTP request failed out after %d seconds with %s: %s",
            timeout_duration, e.__class__.__name__, str(e)
        )
        raise


async def a_rest_request(
        url,
        method,
        *,
        session: AsyncClientSession,
        payload=None,
        payload_type: str = 'json',
        ignore_client_error: bool = False,
        ignore_server_error: bool = False,
        **kwargs,
):
    '''
    `payload`: a Python native type, usually dict.
    '''
    method = method.lower()
    if method == 'get':
        assert payload_type == 'json'
        func = session.get
        if payload:
            args = {'params': payload}
        else:
            args = {}
    elif method == 'post':
        func = session.post
        if payload:
            if isinstance(payload, bytes):
                args = {'data': payload}
            elif payload_type == 'json':
                args = {'json': payload}
            else:
                f = {
                    'orjson-stream': orjson_dumps,
                    'pickle-stream': pickle_dumps,
                    'orjson-z-stream': orjson_z_dumps,
                    'pickle-z-stream': pickle_z_dumps,
                }[payload_type]
                args = {'data': f(payload)}
        else:
            args = {}
    else:
        raise ValueError('unknown method', method)

    kwargs = {**args, **kwargs}
    kwargs['headers'] = {'content-type': 'application/' + payload_type}

    response = await _a_request(func, url, **kwargs)

    response_content_type = response.headers['content-type']
    if response_content_type.starswith('text/plain'):
        data = response.text
    elif response_content_type.starswith('application/json'):
        data = response.json()
    else:
        f = {
            'application/orjson-stream': orjson_loads,
            'application/pickle-stream': pickle_loads,
            'application/orjson-z-stream': orjson_z_loads,
            'application/pickle-z-stream': pickle_z_loads,
        }[response_content_type]
        data = f(await response.read())

    if 200 <= response.status_code < 300:
        return data

    if 400 <= response.status_code < 500 and ignore_client_error:
        return data

    if 500 <= response.status_code and ignore_server_error:
        return data

    response.raise_for_status()

    # Raise exception in case the above does not raise.
    logger.exception('%d, %s; %s', response.status_code, str(data), url)
    raise RuntimeError(f'request to {url} failed with response:\n{data}')


class ClientSession(httpx.Client):
    def __init__(self, *,
                 timeout: Union[ClientTimeout, float, int] = None,
                 **kwargs):
        if not isinstance(timeout, ClientTimeout):
            timeout = ClientTimeout(timeout)
        super().__init__(timeout=timeout, **kwargs)


def rest_request(
        url,
        method,
        *,
        session: ClientSession,
        payload=None,
        payload_type: str = 'json',
        ignore_client_error: bool = False,
        ignore_server_error: bool = False,
        **kwargs):
    method = method.lower()
    if method == 'get':
        assert payload_type == 'json'
        func = session.get
        if payload:
            args = {'params': payload}
        else:
            args = {}
    elif method == 'post':
        func = session.post
        if payload:
            if isinstance(payload, bytes):
                args = {'data': io.BytesIO(payload)}
            elif payload_type == 'json':
                args = {'json': payload}
            else:
                f = {
                    'orjson-stream': orjson_dumps,
                    'pickle-stream': pickle_dumps,
                    'orjson-z-stream': orjson_z_dumps,
                    'pickle-z-stream': pickle_z_dumps,
                }[payload_type]
                args = {'data': io.BytesIO(f(payload))}
        else:
            args = {}
    else:
        raise ValueError('unknown method', method)

    kwargs = {**args, **kwargs}
    kwargs['headers'] = {'content-type': 'application/' + payload_type}

    time0 = perf_counter()
    try:
        response = func(url, **kwargs)
    except (httpx.PoolTimeout, httpcore.PoolTimeout):
        time1 = perf_counter()
        timeout_duration = time1 - time0
        logger.error('timed out after %d seconds', timeout_duration)
        raise

    response_content_type = response.headers.get('content-type')
    if response_content_type.starswith('text/plain'):
        data = response.text
    elif response_content_type.starswith('application/json'):
        data = response.json()
    else:
        f = {
            'application/orjson-stream': orjson_loads,
            'application/pickle-stream': pickle_loads,
            'application/orjson-z-stream': orjson_z_loads,
            'application/pickle-z-stream': pickle_z_loads,
        }[response_content_type]
        data = f(response.content)

    if 200 <= response.status_code < 300:
        return data

    if 400 <= response.status_code < 500 and ignore_client_error:
        return data

    if 500 <= response.status_code and ignore_server_error:
        return data

    response.raise_for_status()

    # Raise exception in case the above does not raise.
    logger.exception('%d, %s; %s', response.status_code, str(data), url)
    raise RuntimeError(f'request to {url} failed with response:\n{data}')
