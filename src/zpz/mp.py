import sys
from traceback import format_exc

# Using `concurrent.futures.ProcessPoolExecutor`
# or `asyncio.get_event_loop().run_in_executor`
# will handle remote exceptions properly, hence
# using those in place of a raw Process is recommended
# when possible.


def full_class_name(cls):
    mod = cls.__module__
    if mod is None or mod == 'builtins':
        return cls.__name__
    return mod + '.' + cls.__name__


# TODO
# check out `https://github.com/ionelmc/python-tblib`


class SubProcessError(Exception):
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
    if type_ is SubProcessError:
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
