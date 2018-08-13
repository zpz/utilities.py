from datetime import datetime
import functools
import inspect
import json
import logging
import os
from pathlib import Path
import threading
from traceback import format_exc
from typing import Union
import urllib.request

logger = logging.getLogger(__name__)

SLACK_NOTIFY_CHANNELS = ['alerts']


def notify_slack(slack_channel: str, status: str, msg: str) -> None:
    if slack_channel not in SLACK_NOTIFY_CHANNELS:
        logger.error(
            'the requested Slack notification channel, %s, is not supported',
            slack_channel)
        return

    slack_channel = slack_channel.replace('-', '').replace('_', '').upper()
    url = os.environ['SLACK_' + slack_channel + '_WEBHOOK_URL']
    json_data = json.dumps({
        'text': '--- {} ---\n{}'.format(status, msg)
    }).encode('ascii')
    req = urllib.request.Request(
        url, data=json_data, headers={'Content-type': 'application/json'})
    thr = threading.Thread(target=urllib.request.urlopen, args=(req, ))
    try:
        thr.start()
    except Exception as e:
        logger.error('failed to send alert to Slack:\n%s', str(e))

    # TODO: is this the right way to send emails async?


def should_send_alert(status: str, ff: Path, silent_seconds: Union[float, int],
                      ok_silent_hours: Union[float, int]) -> bool:
    if not ff.exists():
        return True

    old_status, old_dt, *_ = open(ff).read()[:50].split('\n')
    old_date, old_time, *_ = old_dt.split()

    if old_status != status:
        return True

    t0 = datetime.strptime(old_date + ' ' + old_time, '%Y-%m-%d %H:%M:%S:%f')
    t1 = datetime.utcnow()
    lapse = (t1 - t0).total_seconds()

    if lapse < float(silent_seconds):
        return False

    if status != 'OK':
        return True

    if lapse > float(ok_silent_hours) * 3600.:
        return True

    return False


def notify(exception_classes: Exception = None,
           slack_channel: str = 'alerts',
           silent_seconds: Union[float, int] = 1,
           ok_silent_hours: Union[float, int] = 0.99):
    '''
    A decorator for writing a status file for a function for notification purposes.
   
    In addition, `slack_channel`, `silent_seconds`, `ok_silent_hours`, in combination with
    the current and previous statuses, determine whether to send alert to Slack.
    See `should_send_alert` for details.
   
    Args:
        exception_classes: exception class object, or tuple or list of multiple classes,
            to be captured; if `None`, all exceptions will be captured.
        
        slack_channel: if value is a supported channel, send alert to Slack if other conditions
            are met. Currently only 'alerts' is supported. To suppress Slack notification,
            pass in any other value, like '' or `None`.
        
        silent_seconds: if new status is identical to the previous one and the previous
            status was written within the last `silent_seconds` seconds, do not send alert.
        
        ok_silent_hours: if new and previous statuses are both 'OK' and the previous status
            was written within the last `ok_silent_hours` hours, do not send alert.
    
    The status file is located in the directory specified by the environ variable `NOTIFYDIR`.
    The file name is constructed by the package/module of the decorated function as well as the function's name.
    For example, if function `testit` in package/module `models.regression.linear` is being decorated,
    then the status file is `models.regression.linear.testit` in the notify directory.
    
    If the decorated function is in a launching script (hence its `module` is named `__main__`),
    then the full path of the script is used to construct the status file's name.
    For example, if function `testthat` in script `/home/docker-user/work/scripts/do_this.py` is being decorated,
    then the status file is `home.docker-user.work.scripts.do_this.py.testthat` in the notify directory.
    
    This decorator writes 'OK' in the status file if the decorated function returns successfully.
    If the decorated function raises any exception, `ERROR` is written along with some additional info.
    
    This decorator does not write logs. If you wish to log the exception, you must handle that separately.
    If you handle the exception within the function, make sure you re-`raise` the exception so that this decorator
    can capture it (if you want it to).
    
    Usually you only need to decorate the top-level function in a pipeline's launching script, like this:
        
        # launcher.py
        
        from utilities.notify import notify
        
        @notify()
        def main():
            # do things that could raise exeptions
            # ...
        
        if __name__ == '__main__':
            main()
    
    You want to use this decorator at more refined places only when you want to handle a certain exception
    and then continue the program, but also want to notify about that exception, like this:
    
        # module `abc.py` in package `proj1.component2`
    
        from utilities.notify import notify
    
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
            dt = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S:%f')
            try:
                z = func(*args, **kwargs)
                status = 'OK'
                msg = '{} UTC\n{}\n'.format(dt, decloc)
                return z
            except exception_classes as e:
                status = 'ERROR'
                msg = '{} UTC\n{}\n\n{}\n'.format(dt, decloc, format_exc())
                raise
            except:
                status = 'OK'
                msg = '{} UTC\n{}\n'.format(dt, decloc)
                raise
            finally:
                if should_send_alert(status, notifile, silent_seconds,
                                     ok_silent_hours):
                    notify_slack(slack_channel, status, msg)
                open(notifile, 'w').write(status + '\n' + msg)

        return decorated

    return decorator
