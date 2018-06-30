from datetime import datetime
import functools
import inspect
import os
from pathlib import Path
from traceback import format_exc 


def notify(exception_classes=None):
    '''
    A decorator for writing a status file for a function for notification purposes.
    
    Args:
        exception_classes: exception class object, or tuple or list of multiple classes,
            to be captured; if `None`, all exceptions will be captured.
    
    The status file is located in the directory specified by the environ variable `NOTIFYDIR`.
    The file name is constructed by the package/module of the decorated function as well as the function's name.
    For example, if function `testit` in package/module `models.regression.linear` is being decorated,
    then the status file is `models.regression.linear.testit` in the notify directory.
    
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
    
        from utilities.notify import notify
        @notify()
        def main():
            # do things that could raise exeptions
            # ...
    
        if __name__ == '__main__':
            main()
    
    You want to use this decorator at more refined places only when you want to handle a certain exception
    and then continue the program, but also want to notify about that exeption, like this:
    
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
    
    In this situation, the decorated function should not be one that is called frequently, because a `CRITICAL`
    status file may be overwritten by an `OK` status file in the next call to the function (before the `CRITICAL` status
    gets a chance to be checked).
    '''
    if not exception_classes:
        exception_classes = (Exception,)
    elif issubclass(exception_classes, Exception):
        exception_classes = (exception_classes,)
    else:
        if isinstance(exception_classes, list):
            exception_classes = tuple(exception_classes)
        assert isinstance(exception_classes, tuple)
        assert all(issubclass(v, Exception) for v in exception_classes)

    def decorator(func):
        module = func.__module__
        if module == '__main__':
            module = str(Path.cwd() / inspect.getsourcefile(func)).strip('/').replace('/', '.')
        fname = module + '.' + func.__name__
        fdir = os.environ['NOTIFYDIR']
        Path(fdir).mkdir(parents=True, exist_ok=True)

        notifile = Path(fdir) / fname

        @functools.wraps(func)
        def decorated(*args, **kwargs):
            try:
                z = func(*args, **kwargs)
                msg = 'OK  ' + str(datetime.now()) + '\n'
                open(notifile, 'w').write(msg)
                return z
            except exception_classes as e:
                msg = 'CRITICAL  {} (at {}) {}\n\n{}\n'.format(fname, fdir, datetime.now(), format_exc())
                open(notifile, 'w').write(msg)
                raise
            except:
                msg = 'OK  ' + str(datetime.now()) + '\n'
                open(notifile, 'w').write(msg)
                raise

        return decorated

    return decorator