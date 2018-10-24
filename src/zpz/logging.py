"""
Configure logging, mainly the format.

A call to the function ``config_logger`` in a launching script is all that is needed to set up the logging format.
Usually the 'level' argument is the only argument one needs to customize::

  config_logger(level='info')

If `level` is not specified, environment variable `LOGLEVEL` is used;
if that is not set, a default level (currently 'info') is used.

Do not call this in library modules.
Library modules should have ::

   logger = logging.getLogger(__name__)

and then just use ``logger`` to write logs without concern about formatting,
destination of the log message, etc.
"""
from datetime import datetime
import logging
from logging import Formatter
import time
from typing import Union

import pytz

#import os
# raiseExceptions = os.environ.get('ENVIRONMENT_TYPE', None) in ('test', 'dev')
# When exceptions are raised during logging, then,
# The default implementation of handleError() in Handler checks to see if a module-level variable,
# raiseExceptions, is set.
# If set, a traceback is printed to sys.stderr.
# If not set, the exception is swallowed.
#
# If no logging configuration is provided, then for Python 2.x,
#   If logging.raiseExceptions is False (production mode), the event is silently dropped.
#   If logging.raiseExceptions is True (development mode), a message 'No handlers could be found for logger X.Y.Z' is printed once.

logging.logThreads = 0
logging.logProcesses = 0


def _make_config(
        *,
        level: Union[str, int]='info',
        format: str='[%(asctime)s; %(name)s, %(funcName)s, %(lineno)d; %(levelname)s]    %(message)s',
        timezone: str='US/Pacific',
        datefmt: str='%Y-%m-%d %H:%M:%S %Z',
        **kwargs) -> dict:
    # 'level' is string form of the logging levels: 'debug', 'info', 'warning', 'error', 'critical'.
    if level not in (logging.DEBUG, logging.INFO, logging.WARNING,
                     logging.ERROR, logging.CRITICAL):
        level = getattr(logging, level.upper())

    if timezone.lower() == 'UTC':
        Formatter.converter = time.gmtime
    elif timezone.lower() == 'local':
        Formatter.converter = time.localtime
    else:
        def custom_time(*args):
            utc_dt = pytz.utc.localize(datetime.utcnow())
            my_tz = pytz.timezone(timezone)
            converted = utc_dt.astimezone(my_tz)
            return converted.timetuple()
        Formatter.converter = custom_time

    return dict(format=format, datefmt=datefmt, level=level, **kwargs)


def config_logger(**kwargs) -> None:
    logging.basicConfig(**_make_config(**kwargs))
