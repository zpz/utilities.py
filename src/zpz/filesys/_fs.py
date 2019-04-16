from abc import ABC, abstractmethod
import os.path
from typing import List
from .path import join_path
from ..exceptions import ZpzError


class FS(ABC):
    '''
    This class uses POSIX style of path representations, in particular,

    - root is '/'
    - segment separator is '/'

    In addition, it interprets any path ending with '/' as a *directory*,
    and otherwise a *file*.
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

    @property
    def HOME(self) -> str:
        # Do not define setter and deleter for this property.
        # This is a read-only attribute.
        #
        # If a subclass overrides this method, make sure
        # the return value ends with `/`.
        return '/'

    @property
    def pwd(self) -> str:
        # Get the 'present working directory'.
        #
        # Do not define setter and deleter for this property.
        # This is a read-only attribute.
        #
        # To set `pwd`, use the `cd` method.
        #
        # Before `cd` is called, an object's `pwd` is `HOME`.
        z = getattr(self, '_pwd', self.HOME)
        if not z.endswith('/'):
            z += '/'
        return z

    def cd(self, path: str=None) -> 'FS':
        if path is None:
            z = self.HOME
        else:
            z = join_path(self.pwd, path)
        if not z.endswith('/'):
            z += '/'
        self._pwd = z
        return self

    def abspath(self, path: str) -> 'FS':
        '''
        This gives the full path of `path`,
        analogous to the Linux command `realpath`.
        `path` may be given as relative to `pwd`,
        or as an absolute path.
        '''
        return join_path(self.pwd, path)

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
        raise NotImplementedError

    def mv(self, source: str, dest: str, forced: bool=False) -> None:
        raise NotImplementedError

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

