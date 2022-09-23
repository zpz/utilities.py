"""
Configure logging, mainly the format.

A call to the function ``config_logger`` in a launching script
is all that is needed to set up the logging format.
Usually the 'level' argument is the only argument one needs to customize::

  config_logger(level='info')

Do not call this in library modules. Library modules should have ::

   logger = logging.getLogger(__name__)

and then just use ``logger`` to write logs without concern about formatting,
destination of the log message, etc.
"""
import inspect
import logging
import logging.handlers
import os
import sys
import warnings
from datetime import datetime
from typing import Union


# When exceptions are raised during logging, then,
# the default implementation of handleError() in Handler
# checks to see if a module-level variable,
#   raiseExceptions,
# is set.
# If set, a traceback is printed to sys.stderr.
# If not set, the exception is swallowed.
#
# If no logging configuration is provided, then for Python 2.x,
#   If logging.raiseExceptions is False (production mode),
#       the event is silently dropped.
#   If logging.raiseExceptions is True (development mode),
#       a message 'No handlers could be found for logger X.Y.Z'
#       is printed once.


# Turn off annoyance in ptpython when setting DEBUG logging
logging.getLogger("parso").setLevel(logging.ERROR)

logging.captureWarnings(True)
warnings.filterwarnings("default", category=ResourceWarning)
warnings.filterwarnings("default", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning, module="ptpython")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="jedi")


rootlogger = logging.getLogger()
logger = logging.getLogger(__name__)


def formatter(*, with_process_name: bool = False, with_thread_name: bool = False):
    tz = datetime.now().astimezone().tzname()
    msg = (
        "[%(asctime)s.%(msecs)03d "
        + tz
        + "; %(levelname)s; %(name)s, %(funcName)s, %(lineno)d"
    )

    if with_process_name and with_thread_name:
        fmt = f"{msg};  %(processName)s %(threadName)s]  %(message)s"
    elif with_process_name:
        fmt = f"{msg};  %(processName)s]  %(message)s"
    elif with_thread_name:
        fmt = f"{msg};  %(threadName)s]  %(message)s"
    else:
        fmt = f"{msg}]  %(message)s"

    return logging.Formatter(fmt=fmt, datefmt="%Y-%m-%d %H:%M:%S")


def set_level(level: Union[str, int] = logging.INFO):
    """
    In one application, call `set_level` on the top level once.
    Do not set level anywhere else, and do not set level on any handler.
    """
    if isinstance(level, str):
        level = getattr(logging, level.upper())
    level0 = rootlogger.level
    rootlogger.setLevel(level)
    return level0


def use_console_handler(**kwargs):
    if any(getattr(h, "_name_", None) == "console" for h in rootlogger.handlers):
        raise RuntimeError("the console handler is already in being used")
    h = logging.StreamHandler()
    h.setFormatter(formatter(**kwargs))
    h._name_ = "console"
    rootlogger.addHandler(h)


def _unuse_handler(name: str):
    for h in rootlogger.handlers:
        if getattr(h, "_name_", None) == name:
            rootlogger.removeHandler(h)
            return 1
    return 0


def unuse_console_handler():
    return _unuse_handler("console")


def use_disk_handler(
    *, foldername: str = None, maxBytes=1_000_000, backupCount=20, delay=True, **kwargs
):
    if any(getattr(h, "_name_", None) == "disk" for h in rootlogger.handlers):
        raise RuntimeError("the disk handler is already in being used")

    if foldername:
        foldername = foldername.rstrip("/")
    else:
        launcher = inspect.stack()[1].filename
        foldername == f"{os.environ.get('LOGDIR', '/tmp/log')}/{launcher.lstrip('/').replace('/', '-')}"
    print(f"Log files are located in '{foldername}'")
    os.makedirs(foldername, exist_ok=True)
    h = logging.handlers.RotatingFileHandler(
        filename=foldername + "/current",
        maxBytes=maxBytes,
        backupCount=backupCount,
        delay=delay,
    )
    h.setFormatter(formatter(**kwargs))
    h._name_ = "disk"
    rootlogger.addHandler(h)
    return foldername


def unuse_disk_handler():
    return _unuse_handler("disk")


def log_uncaught_exception(handlers=None, logger=logger):
    # locally bind `handlers` and `logger` in case the global references are gone
    # when the exception handler is invoked.
    if handlers is None:
        handlers = rootlogger.handlers

    def handle_exception(exc_type, exc_val, exc_tb):
        if not issubclass(exc_tb, KeyboardInterrupt):
            if sys.version_info.minor < 11:
                logging.currentframe = lambda: sys._getframe(1)
            fn, lno, func, sinfo = logger.findCaller(stack_info=False, stacklevel=1)
            record = logger.makeRecord(
                logger.name,
                logging.CRITICAL,
                fn,
                lno,
                msg=exc_val,
                args=(),
                exc_info=(exc_type, exc_val, exc_tb),
                func=func,
                sinfo=sinfo,
            )

            for h in rootlogger.handlers:
                if getattr(h, "_name_", None) != "console":
                    h.handle(record)

        sys.__excepthook__(exc_type, exc_val, exc_tb)

    sys.excepthook = handle_exception


def config_logger(level=logging.INFO, **kwargs):
    # For use in one-off scripts.
    set_level(level)
    use_console_handler(**kwargs)
