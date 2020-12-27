import logging
from io import BytesIO
from typing import Union, List, Any

import orjson
import uvicorn
from starlette.applications import Starlette
from starlette.responses import (
    Response,
    JSONResponse, PlainTextResponse, HTMLResponse,
    StreamingResponse,
)
from starlette.routing import Route

from .serde import (
    orjson_loads, orjson_z_loads,
    orjson_dumps, orjson_z_dumps,
    pickle_loads, pickle_z_loads,
    pickle_dumps, pickle_z_dumps,
)
from .logging import log_level_to_str


logger = logging.getLogger(__name__)

REQUEST_LOADERS = {
    'application/orjson-stream': orjson_loads,
    'application/orjson-z-stream': orjson_z_loads,
    'application/pickle-stream': pickle_loads,
    'application/pickle-z-stream': pickle_z_loads,
}
RESPONSE_DUMPERS = {
    'application/orjson-stream': orjson_dumps,
    'application/orjson-z-stream': orjson_z_dumps,
    'application/pickle-stream': pickle_dumps,
    'application/pickle-z-stream': pickle_z_dumps,
}


async def get_request_data(request):
    request_content_type = request.headers['content-type']
    if request_content_type == 'application/json':
        data = await request.json()
    else:
        try:
            cmd = REQUEST_LOADERS[request_content_type]
        except KeyError as e:
            raise ValueError(
                f'unknown content-type "{request_content_type}"') from e
        data = cmd(await request.body())

    return request_content_type, data


# async def get_form_data(request):
#     # This needs the `python-multipart` package installed.
#     form = await request.form()
#     return form


class ORJSONResponse(Response):
    media_type = "application/orjson-stream"

    def render(self, content: Any) -> bytes:
        return orjson_dumps(content)


# def make_response(data, content_type, status=200):
#     if content_type == 'application/json':
#         return JSONResponse(data, status)

#     cmd = RESPONSE_DUMPERS[content_type]
#     return StreamingResponse(
#         BytesIO(cmd(data)),
#         media_type=content_type,
#         status_code=status,
#     )


# def make_text_response(text, status=200):
#     return PlainTextResponse(text, status_code=status)


# def make_json_response(data, status=200):
#     return JSONResponse(data, status_code=status)


# def make_orjson_response(data, status=200):
#     return ORJSONResponse(data, status_code=status)


# def make_html_response(text):
#     return HTMLResponse(text)


def make_exc_response(exc, data=None):
    try:
        status = exc.status_code
    except:
        status = 500
    return JSONResponse(
        {
            'status': 'Internal Server Error',
            'error': exc.__class__.__name__ + ': ' + str(exc),
            'data': str(data),
        },
        status_code=status,
    )


def run_app(app: Union[Starlette, str],
            *,
            port,
            backlog=512,
            log_level: str = None,
            debug: bool = None,
            access_log: bool = None,
            loop='none',
            workers: int = 1,
            **kwargs):
    '''
    `app`: an `Starlette` instance or the import string
    for an `Starlette` instance, like 'mymodule:app'.

    `loop`: usually, leave it at 'none', especially if you need
        to use the event loop before calling this function.
        Otherwise, `uvicorn` has some inconsistent behavior
        between asyncio and uvloop.

        If you want to use `uvloop`, call `a_sync.use_uvloop`
        before calling this function.

        If you don't need to use the event loop at all before
        calling this function, the it's OK to pass in `loop='auto'`.
        In that case, `uvicorn` will use `uvloop` if that package
        is installed (w/o creating a new loop);
        otherwise it will create a new `asyncio` native event loop
        and set it as the default loop.
    '''
    if workers > 1:
        assert isinstance(app, str)
        # TODO: use gunicorn?

    if log_level is None:
        log_level = logger.getEffectiveLevel()
    if not isinstance(log_level, str):
        log_level = log_level_to_str(log_level).lower()
    assert log_level in ('debug', 'info', 'warning')

    if debug is None:
        debug = (log_level == 'debug')
    else:
        debug = bool(debug)

    if access_log is None:
        access_log = debug
    else:
        access_log = bool(access_log)

    uvicorn.run(
        app,
        host='0.0.0.0',
        port=port,
        backlog=backlog,
        access_log=access_log,
        debug=debug,
        log_level=log_level,
        loop=loop,
        workers=workers,
        reload=debug and isinstance(app, str),
        **kwargs,
    )
