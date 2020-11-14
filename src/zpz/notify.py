from datetime import datetime, timedelta
import functools
import inspect
import json
import logging
import os
from pathlib import Path
from traceback import format_exc
from typing import Union, Optional

import arrow

from .timer import Timer
from .slack import post as post_to_slack


logger = logging.getLogger(__name__)


def notify_ok(subject: str, msg: str):
    # Need to a way to get the webhook URL of the slack channel, e.g.
    # via environment variables.
    url = 'abc' # for example
    post_to_slack(url, subject, msg)


def notify_error(subject: str, msg: str):
    # Need to a way to get the webhook URL of the slack channel, e.g.
    # via environment variables.
    url = 'def' # for example
    post_to_slack(url, subject, msg)


def log_notify(filename, status, info):
    dt = arrow.utcnow().format('YYYY-MM-DD HH:mm:ss.SSSSSS') + ' UTC'
    info = info.replace('\n', ' ')
    open(filename, 'w').write(status + '\n' + dt + '\n' + info)


def should_send_alert(status: str, ff: Path, silent_seconds: Union[float, int],
                      ok_silent_hours: Union[float, int], info: str) -> bool:
    if not ff.exists():
        return True

    info = info.replace('\n', ' ')

    try:
        old_status, old_dt, old_info = open(ff).read().split('\n')
        old_date, old_time, *_ = old_dt.split()
    except Exception as e:
        return True

    if old_status != status:
        return True

    if old_info != info:
        return True

    t0 = arrow.get(old_date + ' ' + old_time)
    t1 = arrow.utcnow()
    lapse = (t1 - t0).total_seconds()

    if lapse < float(silent_seconds):
        return False

    if status != 'OK':
        return True

    if lapse > float(ok_silent_hours) * 3600.:
        return True

    return False


def notify(exception_classes: Exception = None,
           debug: bool = False,
           with_args: bool = True,
           silent_seconds: Optional[Union[float, int]] = None,
           ok_silent_hours: Optional[Union[float, int]] = None):
    '''
    A decorator for writing a status file for a function for notification purposes.

    In addition, `slack_channel`, `silent_seconds`, `ok_silent_hours`, in combination with
    the current and previous statuses, determine whether to send alert to Slack.
    See `should_send_alert` for details.

    Args:
        exception_classes: exception class object, or tuple or list of multiple classes,
            to be captured; if `None`, all exceptions will be captured.

        silent_seconds: if new status is identical to the previous one and the previous
            status was written within the last `silent_seconds` seconds, do not send alert.

        ok_silent_hours: if new and previous statuses are both 'OK' and the previous status
            was written within the last `ok_silent_hours` hours, do not send alert.

        debug: if either `silent_seconds` or `ok_silent_hours` is `None`, their values are
            determined according whether `debug` is `True`.

    The status file is located in the directory specified by the environ variable `NOTIFYDIR`.
    The file name is constructed by the package/module of the decorated function as well as the function's name.
    For example, if function `testit` in package/module `my.models.regression.linear` is being decorated,
    then the status file is `my.models.regression.linear.testit` in the notify directory.
    
    If the decorated function is in a launching script (hence its `module` is named `__main__`),
    then the full path of the script is used to construct the status file's name.
    For example, if function `testthat` in script `/home/docker-user/work/scripts/do_this.py` is being decorated,
    then the status file is `home.docker-user.work.scripts.do_this.py.testthat` in the notify directory.
    
    This decorator writes 'OK' in the status file if the decorated function returns successfully.
    If the decorated function raises any exception, `CRITICAL` is written along with some additional info.
    
    This decorator does not write logs. If you wish to log the exception, you must handle that separately.
    If you handle the exception within the function, make sure you re-`raise` the exception so that this decorator
    can capture it (if you want it to).
    
    Usually you only need to decorate the top-level function in a pipeline's launching script, like this:
        
        # launcher.py

        from coyote.notify import notify

        @notify()
        def main():
            # do things that could raise exceptions
            # ...

        if __name__ == '__main__':
            main()

    You want to use this decorator at more refined places only when you want to handle a certain exception
    and then continue the program, but also want to notify about that exception, like this:

        # module `abc.py` in package `proj1.component2`

        from coyote.notify import notify

        class MySpecialError(Exception):
            pass

        @notify(MySpecialError)
        def func1():
            # do things that could raise MySpecialError
            # ...
            # result = ...
            if result is None:
                raise MySpecialError('omg!')

            #...
            #...

        def func2():
            try:
                func1()
                #...
            except MySpecialError as e:
                logger.info('MySpecialError has occurred!')
                return 3    # let program continue to run
    '''
    if not exception_classes:
        exception_classes = (Exception, )
    elif issubclass(exception_classes, Exception):
        exception_classes = (exception_classes, )
    else:
        if isinstance(exception_classes, list):
            exception_classes = tuple(exception_classes)
        assert isinstance(exception_classes, tuple)
        assert all(issubclass(v, Exception) for v in exception_classes)

    if silent_seconds is None:
        silent_seconds = 1.0

    if ok_silent_hours is None:
        if debug:
            ok_silent_hours = 1. / 60.  # 1 minute
        else:
            ok_silent_hours = 23.90  # 1 day

    def decorator(func):
        module = func.__module__
        if module == '__main__':
            module = str(Path.cwd() / inspect.getsourcefile(func))
        decloc = module + ' :: ' + func.__name__
        fname = module.strip('/').replace('/', '.') + '.' + func.__name__
        fdir = os.environ['NOTIFYDIR']
        Path(fdir).mkdir(parents=True, exist_ok=True)

        notifile = Path(fdir) / fname

        @functools.wraps(func)
        def decorated(*args, **kwargs):
            mytimer = Timer().start()
            if with_args:
                args_msg = f'args: {args}\nkwargs: {kwargs}\n'
            else:
                args_msg = ''
            status_msg = decloc + args_msg
            try:
                z = func(*args, **kwargs)
                status = 'OK'
                msg = '{}\nThe function took {} to finish.\n{}'.format(
                    decloc, timedelta(seconds=mytimer.stop().seconds), args_msg)
                if should_send_alert(status, notifile, silent_seconds,
                                     ok_silent_hours, status_msg):
                    notify_ok(status, msg)
                log_notify(notifile, status, status_msg)
                return z
            except exception_classes as e:
                status = 'ERROR'
                msg = '{}\n\n{}\n{}'.format(decloc, format_exc(), args_msg)
                if should_send_alert(status, notifile, silent_seconds,
                                     ok_silent_hours, status_msg):
                    notify_error(status, msg)
                log_notify(notifile, status, status_msg)
                raise
            except:
                status = 'OK'
                msg = '{}\nThe function took {} to finish.\n{}'.format(
                        decloc, timedelta(seconds=mytimer.stop().seconds), args_msg)
                if should_send_alert(status, notifile, silent_seconds,
                                     ok_silent_hours, status_msg):
                    notify_ok(status, msg)
                log_notify(notifile, status, status_msg)
                raise

        return decorated

    return decorator

