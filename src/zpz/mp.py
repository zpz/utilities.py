import multiprocessing
import sys
from multiprocessing.managers import BaseManager
from traceback import format_exc

# Using `concurrent.futures.ProcessPoolExecutor`
# or `asyncio.get_event_loop().run_in_executor`
# will handle remote exceptions properly, hence
# using those in place of a raw Process is recommended
# when possible.


MAX_THREADS = min(32, multiprocessing.cpu_count() + 4)
# This default is suitable for I/O bound operations.
# For others, user may want to specify a smaller value.


def full_class_name(cls):
    mod = cls.__module__
    if mod is None or mod == 'builtins':
        return cls.__name__
    return mod + '.' + cls.__name__


# TODO
# check out `https://github.com/ionelmc/python-tblib`
#
# about pickling Exceptions with tracebacks.


class MpError(Exception):
    # An object of this class can be properly pickled
    # to survice transmission in `multiprocessing.Queue`.
    def __init__(self, e: Exception):
        # This instance must be created right after the exception `e`
        # was raised, before any other exception was raised,
        # for otherwise, we can't get the traceback for `e`.
        self.name = e.__class__.__name__
        self.qualname = full_class_name(e.__class__)
        self.message = str(e)
        self.trace_back = format_exc()
        self._args = e.args

    @property
    def args(self):
        return self._args

    def __repr__(self):
        return f"{self.__class__.__name__}({self.qualname}('{self.message}'))"

    def __str__(self):
        return self.qualname + ': ' + self.message


def _my_excepthook(type_, value, tb):
    if type_ is MpError:
        # With this hook, the printout upon
        #   `raise SubProcessError(ValueError('wrong value'))`
        # is like what happens upon
        #   `raise ValueError('wrong value')`
        # in the sub process.
        # The word `SubProcessError` does not appear in the printout.
        # The traceback is what's relevant in the subprocess where the error happened,
        # instead of what's relevant with respect to where `raise SubProcessError(...)` is called.
        print(value.trace_back, file=sys.stderr)
    else:
        sys.__excepthook__(type, value, tb)


sys.excepthook = _my_excepthook


class ServerProcess:
    # User should subclass this class to implement the
    # functionalities they need.
    # An instance of this class is created in a "server process"
    # and serves as shared data for other processes.
    #
    # Usage:
    #
    #   (1) in main process,
    #
    #       obj = ServerProcess.start(...)
    #
    #   (2) pass `obj` to other processes
    #
    #   (3) in other processes, cal public methods of `obj`.
    #
    # `obj` is NOT an instance of the class `ServerProcess`.
    # It's a "proxy" object, which is like a reference to a
    # `ServerProcess` object in the "server process".
    # All public methods of `ServerProcess` can be used on this
    # proxy object from other processes.
    # Input and output should all be small, pickle-able
    # objects.
    #
    # When all references to this proxy object have
    # been garbage-collected, the server process is shut down.
    # Usually user doesn't need to worry about it.
    # In order to proactively shut down the server process,
    # delete (all references to) the proxy object.
    #
    # `ServerProcess.start()` can be used multiple times
    # to have multiple shared objects, which reside in diff
    # processes and are independent of each other.
    @classmethod
    def start(cls, *args, **kwargs):
        BaseManager.register(
            cls.__name__,
            cls,
        )
        manager = BaseManager()
        manager.start()
        obj = getattr(manager, cls.__name__)(*args, **kwargs)
        return obj
