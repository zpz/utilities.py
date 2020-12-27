import io
import logging
import warnings
from time import perf_counter
from typing import Union

import httpcore
import httpx
from tenacity import (
    retry, stop_after_attempt,
    wait_random_exponential, retry_if_exception_type)

from .serde import (
    orjson_dumps, orjson_loads,
    orjson_z_dumps, orjson_z_loads,
    pickle_dumps, pickle_loads,
    pickle_z_dumps, pickle_z_loads,
)

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
async def _a_request(func, url, **kwargs):
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
            elif payload_type == 'orjson-stream':
                args = {'data': orjson_dumps(payload)}
            elif payload_type == 'orjson-z-stream':
                args = {'data': orjson_z_dumps(payload)}
            elif payload_type == 'pickle-stream':
                args = {'data': pickle_dumps(payload)}
            elif payload_type == 'pickle-z-stream':
                args = {'data': pickle_z_dumps(payload)}
            else:
                raise ValueError(
                    f"payload_type '{payload_type}'' is not supported")
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
    elif response_content_type == 'application/orjson-stream':
        data = orjson_loads(await response.aread())
    elif response_content_type == 'application/orjson-z-stream':
        data = orjson_z_loads(await response.aread())
    elif response_content_type == 'application/pickle-stream':
        data = pickle_loads(await response.aread())
    elif response_content_type == 'application/pickle-z-stream':
        data = pickle_z_loads(await response.aread())
    elif response_content_type in ('image/jpeg', 'image/png', 'image/gif'):
        data = response.content
    else:
        warnings.warn(
            f"unknown content-type {response_content_type}; raw data is returned")
        data = response.content

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        # `e` carries a message, and attributes `request`, `response`.
        # User can get code via `e.response.status_code`.
        e.response_data = data
        e.status_code = response.status_code
        e.is_client_error = httpx.codes.is_client_error(response.status_code)
        e.is_server_error = httpx.codes.is_server_error(response.status_code)
        raise e

    return data


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
            elif payload_type == 'orjson-stream':
                args = {'data': io.BytesIO(orjson_dumps(payload))}
            elif payload_type == 'orjson-z-stream':
                args = {'data': io.BytesIO(orjson_z_dumps(payload))}
            elif payload_type == 'pickle-stream':
                args = {'data': io.BytesIO(pickle_dumps(payload))}
            elif payload_type == 'pickle-z-stream':
                args = {'data': io.BytesIO(pickle_z_dumps(payload))}
            else:
                raise ValueError(
                    f"payload_type '{payload_type}' is not supported")
        else:
            args = {}
    else:
        raise ValueError('unknown method', method)

    kwargs = {**args, **kwargs}
    kwargs['headers'] = {'content-type': 'application/' + payload_type}

    response = func(url, **kwargs)

    response_content_type = response.headers.get('content-type')
    if response_content_type.starswith('text/plain'):
        data = response.text
    elif response_content_type.starswith('application/json'):
        data = response.json()
    elif response_content_type == 'application/orjson-stream':
        data = orjson_loads(response.content)
    elif response_content_type == 'application/orjson-z-stream':
        data = orjson_z_loads(response.content)
    elif response_content_type == 'application/pickle-stream':
        data = pickle_loads(response.content)
    elif response_content_type == 'application/pickle-z-stream':
        data = pickle_z_loads(response.content)
    elif response_content_type in ('image/jpeg', 'image/png', 'image/gif'):
        data = response.content
    else:
        warnings.warn(
            f"unknown content-type {response_content_type}; raw data is returned")
        data = response.content

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        # `e` carries a message, and attributes `request`, `response`.
        # User can get code via `e.response.status_code`.
        e.response_data = data
        e.status_code = response.status_code
        e.is_client_error = httpx.codes.is_client_error(response.status_code)
        e.is_server_error = httpx.codes.is_server_error(response.status_code)
        raise e

    return data
