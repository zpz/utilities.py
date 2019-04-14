from abc import ABC, abstractmethod
import os.path
from typing import List
from .path import join_path
from ..exceptions import ZpzError


def _get_cp_dest_path(source_full_path: str, dest_full_path: str) -> str:
    if dest_full_path.endswith('/'):
        assert not source_full_path.endswith('/')
        z = os.path.basename(source_full_path)
        assert z != '.' and z != '..'
        return dest_full_path + z
    return dest_full_path


class FS(ABC):

    @property
    def HOME(self) -> str:
        # Do not define setter and deleter for this property.
        # This is a read-only attribute.
        #
        # If a subclass over-rides this method, make sure
        # the return value ends with `/`.
        return '/'

    @property
    def pwd(self) -> str:
        # Do not define setter and deleter for this property.
        # This is a read-only attribute.
        #
        # To set `pwd`, use the `cd` method.
        z = getattr(self, '_pwd', self.HOME)
        if not z.endswith('/'):
            z += '/'
        return z

    def cd(self,path: str=None) -> 'self':
        if path is None:
            z = self.HOME
        else:
            z = join_path(self.pwd, path)
        if not z.endswith('/'):
            z += '/'
        self._pwd = z
        return self

    def fullpath(self, path: str) -> 'self':
        '''
        This gives the full path of `path`,
        analogous to the Linux command `realpath`.
        '''
        return join_path(self.pwd, path)

    def _isfile(self, full_path: str) -> bool:
        return not full_path.endswith('/')

    def _isdir(self, full_path: str) -> bool:
        return full_path.endswith('/')

    def _assert_isfile(self, full_path: str) -> None:
        if not self._isfile(full_path):
            raise ZpzError(f"Expecting a file; got '{full_path}'")

    def _assert_isdir(self, full_path: str) -> None:
        if not self._isdir(full_path):
            raise ZpzError(f"Expecting a directory; got '{full_path}'")

    @abstractmethod
    def _exists(self, full_path: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _ls(self, full_path: str, recursive: bool=False) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def _rm(self, full_path: str) -> None:
        pass

    def exists(self, path: str) -> bool:
        return self._exists(self.fullpath(path))

    def ls(self, path: str='.', recursive: bool=False) -> List[str]:
        return self._ls(self.fullpath(path), recursive)

    def rm(self, path: str, forced: bool=False) -> None:
        full_path = self.fullpath(path)
        self._assert_isfile(full_path)
        if self._exists(full_path):
            self._rm(full_path)
        else:
            if not forced:
                raise ZpzError(f"file '{full_path}' does not exist")

    def _cp(self, source_full_path: str, dest_full_path: str) -> None:
        raise NotImplementedError

    def cp(self, source: str, dest: str, forced: bool=False) -> None:
        source_full = self.fullpath(source)
        dest_full = _get_cp_dest_path(source_full, self.fullpath(dest))
        self._cp(source_full, dest_full)

    def mv(self, source: str, dest: str, forced: bool=False) -> None:
        raise NotImplementedError

    def _put_text(self, text: str, full_path: str) -> None:
        raise NotImplementedError

    def put_text(self, text: str, path: str, forced: bool=False) -> None:
        full_path = self.fullpath(path)
        self._assert_isfile(full_path)
        if self._exists(full_path):
            if forced:
                self._rm(full_path)
            else:
                raise ZpzError(f"file '{full_path}' already exists")
        self._put(text, full_path)

    @abstractmethod
    def _get_text(self, full_path: str) -> str:
        raise NotImplementedError

    def get_text(self, path: str) -> str:
        full_path = self.fullpath(path)
        self._assert_isfile(full_path)
        if not self._exists(full_path):
            raise ZpzError(f"file '{full_path}' does not exist")
        return self._get_text(full_path)

