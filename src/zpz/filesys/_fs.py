from abc import ABC, abstractmethod
from typing import List
from .path import join_path
from ..exceptions import ZpzError

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

    def _exists(self, full_path: str) -> bool:
        # A subclass may choose to re-implement this
        # in a more efficient way.
        z = self._ls(full_path, recursive=False)
        return len(z) > 0

    @abstractmethod
    def _ls(self, full_path: str, recursive: bool=False) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def _rm(self, full_path: str) -> None:
        pass

    def isfile(self, path: str) -> bool:
        return self._isfile(self.fullpath(path))

    def isdir(self, path: str) -> bool:
        return self._isdir(self.fullpath(path))

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

    def cp(self, source: str, dest: str, forced: bool=False) -> None:
        raise NotImplementedError

    def mv(self, source: str, dest: str, forced: bool=False) -> None:
        raise NotImplementedError

    def put_text(self, text, file_path: str) -> None:
        raise NotImplementedError

    def get_text(self, file_path: str) -> str:
        raise NotImplementedError

