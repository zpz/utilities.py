from abc import ABC, abstractmethod
import os.path
from typing import List
from ..path import join_path
from ..exceptions import ZpzError


class Store(ABC):
    '''
    This class uses POSIX style of path representations, in particular,

    - root is '/'
    - segment separator is '/'

    In addition, it interprets any path ending with '/' as a *directory*,
    and otherwise a *file*.

    Note 'root' is the root *inside* the store.
    Think of an instance of this class as a 'box' or 'container'.
    It does not have to be located at the root of the file system;
    rather, it can be a 'directory', then '/' within this class refers to
    this directory. It is specified by the `home` parameter of `__init__`.
    Operations within this instance can not go beyond this store root.

    For example, if `home` is '/home/user/writings/`, then one is free
    to navigate through the Subdirectories of `/home/user/writtings/`,
    but can not access `/home/user/`.
    '''

    @classmethod
    def is_file(cls, full_path: str) -> bool:
        return not full_path.endswith('/')

    @classmethod
    def is_dir(cls, full_path: str) -> bool:
        return full_path.endswith('/')

    @classmethod
    def _assert_is_file(cls, full_path: str) -> None:
        if not cls.is_file(full_path):
            raise ZpzError(f"Expecting a file; got '{full_path}'")

    @classmethod
    def _assert_is_dir(cls, full_path: str) -> None:
        if not cls.is_dir(full_path):
            raise ZpzError(f"Expecting a directory; got '{full_path}'")

    def __init__(self, home: str='/'):
        if not home.endswith('/'):
            home += '/'
        self._home = home
        self._pwd = '/'

    @property
    def home(self) -> str:
        # Do not define setter and deleter for this property.
        # This is a read-only attribute.
        return self._home

    @property
    def pwd(self) -> str:
        '''
        Get the 'present working directory'.
        This is an 'absolute' path, but the '/' refers to `self.home`.

        Do not define setter and deleter for this property.
        This is a read-only attribute.

        To set `pwd`, use the `cd` method.
        '''
        return self._pwd

    def abspath(self, path: str) -> str:
        '''
        This gives the 'absolute' path within the store,
        in other words, the return value starts with '/'
        and that is refers to the root within the store,
        i.e. `self.home`.

        `path` may be given as relative to `self.pwd`,
        or as an absolute path within the store (which would be returned
        w/o change).
        '''
        return join_path(self.pwd, path)

    def realpath(self, path: str) -> str:
        '''
        This returns the 'real' absolute path in the file system.
        This is the concatenation of `self.home` and the path within the store.
        '''
        return os.path.join(self.home, self.abspath(path)[1:])

    def cd(self, path: str=None) -> 'Store':
        if path is None:
            self._pwd = '/'
        else:
            z = self.abspath(path)
            if not z.endswith('/'):
                z += '/'
            self._pwd = z
        return self

    @abstractmethod
    def _exists_file(self, full_path: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _exists_dir(self, full_path: str) -> bool:
        raise NotImplementedError

    def exists(self, path: str) -> bool:
        full_path = self.abspath(path)
        if self.is_file(full_path):
            return self._exists_file(full_path)
        else:
            return self._exists_dir(full_path)

    @abstractmethod
    def _ls_dir(self, full_path: str, recursive: bool=False) -> List[str]:
        '''
        List items below the directory `full_path` which is known to exist.
        The returned paths are relative to `full_path`.
        Subdirectories have a trailing `/`.
        '''
        raise NotImplementedError

    def ls(self, path: str='.', recursive: bool=False) -> List[str]:
        full_path = self.abspath(path)
        if self.is_file(full_path):
            if self._exists_file(full_path):
                return [path]
            else:
                return []

        if self._exists_dir(full_path):
            z = self._ls_dir(full_path, recursive)
            if z:
                z = [os.path.join(path, v) for v in z]
            return z

        return []

    @abstractmethod
    def _rm(self, full_path: str) -> None:
        '''
        Remove single file `full_path` that is known to exist.
        '''
        raise NotImplementedError

    def rm(self, path: str, forced: bool=False) -> None:
        full_path = self.abspath(path)
        self._assert_is_file(full_path)
        if self._exists_file(full_path):
            self._rm(full_path)
        else:
            if not forced:
                raise ZpzError(f"can not remove file '{full_path}', which does not exist")

    def cp(self, source: str, dest: str, forced: bool=False) -> None:
        '''
        Copy within the store.
        '''
        raise NotImplementedError

    def mv(self, source: str, dest: str, forced: bool=False) -> None:
        '''
        Move within the store.
        '''
        raise NotImplementedError

    def put(self, local_abs_file: str, path: str='./') -> None:
        raise NotImplementedError

    def get(self, file: str, local_abs_path: str) -> None:
        raise NotImplementedError

    def open(self, file: str):
        raie NotImplementedError

    @abstractmethod
    def _put_text(self, text: str, full_path: str) -> None:
        '''
        Write `text` to file `full_path`, which is know to be non-existent.
        Not only the file is non-existent, its parent directories may be non-existent as well.
        '''
        raise NotImplementedError

    def put_text(self, text: str, path: str, forced: bool=False) -> None:
        full_path = self.abspath(path)
        self._assert_is_file(full_path)
        if self._exists_file(full_path):
            if forced:
                self._rm(full_path)
            else:
                raise ZpzError(f"file '{full_path}' already exists")
        self._put_text(text, full_path)

    @abstractmethod
    def _get_text(self, full_path: str) -> str:
        '''
        Read the content of text file `full_path`, which is known to exist.
        '''
        raise NotImplementedError

    def get_text(self, path: str) -> str:
        full_path = self.abspath(path)
        self._assert_is_file(full_path)
        if not self._exists_file(full_path):
            raise ZpzError(f"file '{full_path}' does not exist")
        return self._get_text(full_path)

