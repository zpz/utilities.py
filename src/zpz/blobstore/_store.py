from abc import ABC, abstractmethod
import os.path
import os
from typing import List
from ..path import join_path
from ..exceptions import ZpzError


def _is_abs(path: str) -> bool:
    return path.startswith('/')


def _assert_is_abs(path: str) -> None:
    if not _is_abs(path):
        raise ZpzError(f"Expecting an absolute path; got '{path}'")


def _get_cp_dest(abs_source_file: str, abs_dest_path: str) -> str:
    # Get the destination file path as if we do
    #    cp abs_source_file abs_dest_path
    if abs_dest_path.endswith('/'):
        assert not abs_source_file.endswith('/')
        return os.path.join(abs_dest_path, os.path.basename(abs_source_file))
    return abs_dest_path


class Store(ABC):
    '''
    This class creates/operates a space in a file storage system for
    file upload, download, deletion, duplication, movement, renaming,
    reading, etc.

    For convenience, this space will be referred to as a 'store' in this class.
    Think of it as a 'directory', or 'box', or 'container'.
    The basic unit of storage in a store is a 'file', or 'blob'.

    Within the store, we use POSIX style of path representations to locate blobs.
    In particular,

    - root is '/'
    - segment separator is '/'

    Note 'root' is the root **inside** the store.
    It does not have to be located at the root of the file system;
    rather, it can be a 'directory', then '/' within this class refers to
    this directory.

    The location of this root in the file system (outside of the store)
    is specified by the `home` parameter of `__init__`, and can be queried
    by the property `home`.

    Operations within the store can not go beyond this store root.
    For example, if `home` is '/home/user/writings/`, then we ar free
    to navigate through the subdirectories of `/home/user/writtings/`,
    but can not access `/home/user/`.

    In the store, 'directories' are *virtual*, meaning we do not need to think about
    directories as concrete things and 'create' or 'delete' them.
    They are transparent to the user.
    If there is a blob with path `/ab/cd/ef.txt`, then we say 'directory'
    `/ab/cd/` exists. If there is no blob with path like `/ab/cd/*`, then
    directory `/ab/cd/` does not exist.

    We use this naming convention:

        *file or *file_path:  a file (i.e. blob)
        *dir or *dir_path: a (virtual) directory
        *path:  either file or directory

    Any in-store path ending with '/' is considered a *directory*,
    and otherwise a *file*.

    In-store paths can always be written as either relative or absolute.
    A relative path is relative to the 'current working directory', which is
    returned by `self.pwd` (which is always absolute).
    '''

    @classmethod
    def _is_file(cls, abs_path: str) -> bool:
        return not abs_path.endswith('/')

    @classmethod
    def _assert_is_file(cls, abs_path: str) -> None:
        if not cls._is_file(abs_path):
            raise ZpzError(f"Expecting a file; got '{abs_path}'")

    @classmethod
    def _is_dir(cls, abs_path: str) -> bool:
        return abs_path.endswith('/')

    @classmethod
    def _assert_is_dir(cls, abs_path: str) -> None:
        if not cls._is_dir(abs_path):
            raise ZpzError(f"Expecting a directory; got '{abs_path}'")

    def __init__(self, home: str='/'):
        '''
        `home` is the location of the store in the file system.
        Usually this is like a 'directory'.
        '''
        if not home.endswith('/'):
            home += '/'
        self._home = home
        self._pwd = '/'

    @property
    def home(self) -> str:
        '''
        Do not define setter and deleter for this property.
        This is a read-only attribute.
        '''
        return self._home

    @property
    def pwd(self) -> str:
        '''
        Get the 'present working directory'.
        This is an 'absolute' path within the store, hence the leading '/' refers to `self.home`.

        Do not define setter and deleter for this property.
        This is a read-only attribute.

        To set `pwd`, use the `cd` method.
        '''
        return self._pwd

    def abspath(self, path: str) -> str:
        '''
        This gives the 'absolute' path within the store,
        in other words, the return value starts with '/'
        and that refers to the root within the store,
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
        '''
        Change the 'present working directory'.
        '''
        if path is None:
            self._pwd = '/'
        else:
            z = self.abspath(path)
            if not z.endswith('/'):
                z += '/'
            self._pwd = z
        return self

    @abstractmethod
    def _exists_file(self, abs_path: str) -> bool:
        raise NotImplementedError

    def exists(self, path: str) -> bool:
        abs_path = self.abspath(path)
        if self._is_file(abs_path):
            return self._exists_file(abs_path)
        else:
            return len(self._ls_dir(abs_path)) > 0

    @abstractmethod
    def _ls_dir(self, abs_path: str, recursive: bool=False) -> List[str]:
        '''
        List items below the directory `abs_path`.
        The returned paths are relative to `abs_path`.
        Subdirectories have a trailing `/`.
        '''
        raise NotImplementedError

    def ls(self, path: str='.', recursive: bool=False) -> List[str]:
        abs_path = self.abspath(path)
        if self._is_file(abs_path):
            if self._exists_file(abs_path):
                return [path]
            else:
                return []

        z = self._ls_dir(abs_path, recursive)
        if z:
            z = [os.path.join(path, v) for v in z]

        return z

    @abstractmethod
    def _rm(self, abs_path: str) -> None:
        '''
        Remove single file `abs_path` that is known to exist.
        '''
        raise NotImplementedError

    def rm(self, path: str, forced: bool=False) -> None:
        abs_path = self.abspath(path)
        self._assert_is_file(abs_path)
        if self._exists_file(abs_path):
            self._rm(abs_path)
        else:
            if not forced:
                raise ZpzError(f"file '{self.realpath(abs_path)}' does not exist")

    def _cp(self, abs_source_file: str, abs_dest_file: str) -> None:
        '''
        Duplicate `abs_source_file` as `abs_dest_file`, which is known to be non-existent.

        This is not a `abstractmethod`. It's not needed by other methods of this class.
        If a subclass does not find this functionality needed, it does not need to implement it.
        '''
        raise NotImplementedError

    def cp(self, source_file: str, dest_path: str, forced: bool=False) -> None:
        '''
        Copy within the store.
        '''
        abs_source_file = self.abspath(source_file)
        self._assert_is_file(abs_source_file)
        abs_dest_file = _get_cp_dest(abs_source_file, self.abspath(dest_path))
        if self._exists_file(abs_dest_file):
            if forced:
                self._rm(abs_dest_file)
            else:
                raise ZpzError(f"file '{self.realpath(abs_dest_file)}' already exists")
        self._cp(abs_source_file, abs_dest_file)

    def _mv(self, abs_source_file: str, abs_dest_file: str) -> None:
        '''
        Rename `abs_source_file` to `abs_dest_file`, which is known to be non-existent.

        This is not a `abstractmethod`. It's not needed by other methods of this class.
        If a subclass does not find this functionality needed, it does not need to implement it.
        '''
        raise NotImplementedError

    def mv(self, source_file: str, dest_path: str, forced: bool=False) -> None:
        '''
        Move within the store.
        '''
        abs_source_file = self.abspath(source_file)
        self._assert_is_file(abs_source_file)
        abs_dest_file = _get_cp_dest(abs_source_file, self.abspath(dest_path))
        if self._exists_file(abs_dest_file):
            if forced:
                self._rm(abs_dest_file)
            else:
                raise ZpzError(f"file '{self.realpath(abs_dest_file)}' already exists")
        self._mv(abs_source_file, abs_dest_file)

    @abstractmethod
    def _put(self, local_abs_file: str, abs_file: str) -> None:
        '''
        Copy `local_abs_file`, which is known to be existent,
        into the store as `abs_file`, which is known to be non-existent.
        '''
        raise NotImplementedError

    def put(self, local_abs_file: str, path: str='./', forced: bool=False) -> None:
        abs_dest_file = _get_cp_dest(local_abs_file, self.abspath(path))
        if self._exists_file(dest):
            if forced:
                self._rm(dest)
            else:
                raise ZpzError(f"file '{self.realpath(abs_dest_file)}' already exists")
        if not os.path.isfile(local_abs_file):
            raise ZpzError(f"file '{local_abs_file}' does not exist")
        self._put(local_abs_file, abs_dest_file)

    @abstractmethod
    def _get(self, abs_file: str, local_abs_file: str) -> None:
        '''
        Download blob `abs_file`, which is known to be existent,
        as `local_abs_file`, which is know to be non-existent.
        '''
        raise NotImplementedError

    def get(self, file_path: str, local_abs_path: str, forced: bool=False) -> None:
        abs_file = self.abspath(file_path)
        self._assert_is_file(abs_file)
        if not self._exists_file(abs_file):
            raise ZpzError(f"file '{self.realpath(abs_file)}' does not exist")
        local_abs_dest_file = _get_cp_dest(abs_file, local_abs_path)
        if os.path.isfile(local_abs_path):
            if forced:
                os.remove(local_abs_dest_file)
            else:
                raise ZpzError(f"file '{local_abs_dest_file}' already exists")
        self._get(abs_file, local_abs_dest_file)

    @abstractmethod
    def _put_text(self, text: str, abs_file_path: str) -> None:
        '''
        Write `text` to file `abs_file_path`, which is know to be non-existent.
        Not only the file is non-existent, its parent directories may be non-existent as well.
        '''
        raise NotImplementedError

    def put_text(self, text: str, file_path: str, forced: bool=False) -> None:
        abs_path = self.abspath(file_path)
        self._assert_is_file(abs_path)
        if self._exists_file(abs_path):
            if forced:
                self._rm(abs_path)
            else:
                raise ZpzError(f"file '{file_path}' already exists")
        self._put_text(text, abs_path)

    @abstractmethod
    def _get_text(self, abs_file_path: str) -> str:
        '''
        Read the content of text file `abs_file_path`, which is known to exist.
        '''
        raise NotImplementedError

    def get_text(self, file_path: str) -> str:
        abs_path = self.abspath(file_path)
        self._assert_is_file(abs_path)
        if not self._exists_file(abs_path):
            raise ZpzError(f"file '{file_path}' does not exist")
        return self._get_text(abs_path)

    def open(self, file_path: str, mode: str='rt'):
        raie NotImplementedError

