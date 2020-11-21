import os
import os.path
import shutil
import tempfile
import time
import threading
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager
from copy import deepcopy
from typing import Union, List, Callable, Type, Iterable

from .serde import (
    json_load, json_dump, orjson_load, orjson_dump,
    orjson_z_load, orjson_z_dump,
    pickle_load, pickle_dump, pickle_z_load, pickle_z_dump,
    marshal_load, marshal_dump,
)


LOADERS = {
    'pickle': pickle_load,
    'marshal': marshal_load,
    'orjson': orjson_load,
    'pickle_z': pickle_z_load,
    'orjson_z': orjson_z_load,
}
DUMPERS = {
    'pickle': pickle_dump,
    'marshal': marshal_dump,
    'orjson': orjson_dump,
    'pickle_z': pickle_z_dump,
    'orjson_z': orjson_z_dump,
}


def _get_file_extension(path, *paths):
    if paths:
        ext = paths[-1]
    else:
        ext = path
    return ext[ext.rfind('.') + 1:]


def _load_file(path):
    loader = LOADERS[_get_file_extension(path)]
    return loader(path)


def _dump_file(x, path):
    dumper = DUMPERS[_get_file_extension(path)]
    return dumper(x, path)


class Dumper:
    def __init__(self, max_workers: int = 5):
        self._max_workers = max_workers
        self._executor = None
        self._sem = None
        # Do not instantiate these now.
        # They would cause trouble when `Biglist`
        # file-views are sent to other processes.

        self._files = {}
        self._tasks = {}

    def _callback(self, t):
        self._sem.release()
        tid = id(t)
        del self._files[tid]
        del self._tasks[tid]
        if t.exception():
            raise t.exception()

    def dump_file(self, data: List, path: str):
        if self._executor is None:
            self._executor = ThreadPoolExecutor(self._max_workers)
            self._sem = threading.Semaphore(self._max_workers)
        self._sem.acquire()
        task = self._executor.submit(_dump_file, data, path)
        tid = id(task)
        self._tasks[tid] = task
        self._files[tid] = path
        task.add_done_callback(self._callback)

    def has_file(self, path: str):
        # Check whether the file `path` is in queue to be
        # dumped or beining dumped (and not done yet).
        return path in self._files.values()

    def wait(self):
        if self._executor is not None:
            self._executor.shutdown(wait=True)
        assert not self._files
        assert not self._tasks

    def cancel(self):
        self.wait()


class Loader:
    def __init__(self, max_workers: int = 5):
        self._max_workers = max_workers
        self._executor = None
        self._sem = None

        self._tasks = {}
        self._data = []

    def _callback(self, t):
        self._sem.release()
        del self._tasks[id(t)]
        if t.cancelled():
            return
        if t.exception():
            raise t.exception()
        self._data.append((t._path_, t.result()))

    def load_file(self, path: str, *paths):
        if self._executor is None:
            self._executor = ThreadPoolExecutor(self._max_workers)
            self._sem = threading.Semaphore(self._max_workers)

        nothing = object()
        result = nothing
        for i, (p, v) in enumerate(self._data):
            if p == path:
                self._data.pop(i)
                self._data.insert(0, (p, v))
                result = v
                break

        if result is nothing:
            result_task = None
            for t in self._tasks.values():
                # TODO:
                # corner case: as we do this loop,
                # the task of interest has finished and
                # disappeared from `self._tasks`.
                if t._path_ == path:
                    result_task = t
                    break

            if result_task is None:
                self._sem.acquire()
                result_task = self._executor.submit(_load_file, path)
                result_task._path_ = path
                self._tasks[id(result_task)] = result_task
                result_task.add_done_callback(self._callback)

        for p in paths:
            found = False
            for v in self._data:
                if v[0] == p:
                    found = True
                    break
            if found:
                continue
            for t in self._tasks.values():
                if t._path_ == p:
                    found = True
                    break
            if found:
                continue

            self._sem.acquire()
            task = self._executor.submit(_load_file, p)
            task._path_ = p
            self._tasks[id(task)] = task
            task.add_done_callback(self._callback)

        if result is nothing:
            _ = result_task.result()
            for i, (p, v) in enumerate(self._data):
                if p == path:
                    self._data.pop(i)
                    self._data.insert(0, (p, v))
                    result = v
                    break

        if len(self._data) > self._max_workers:
            self._data = self._data[: self._max_workers]

        return result

    def cancel(self):
        for t in self._tasks.values():
            t.cancel()
        self._data = []


class Biglist(Sequence, AbstractContextManager):
    '''
    `Biglist` implements a single-machine, out-of-memory list, that is,
    the list may exceed the capacity of the memory, but can be stored on the hard drive
    of the single machine.

    The list is mutable by appending only, via `append` and `extend`.
    Existing elements can not be modified.

    Single elements can be accessed via the standard `[index]` syntax.
    The object can be iterated over to walk through the elements one by one.

    Accessing a range of elements by the slicing syntax `[start:stop:step]` is not supported.
    This is because that, by convention, slicing should return an object of the same type,
    but that is not possible with `Biglist`.

    However, the property `view` returns a `BiglistView`, which supports indexing,
    slicing, iterating.

    The data are stored on disk as a series of files.
    There are methods that provide a view into the elements in a single file.
    This is intended to facilitate parallel processing of files by
    different worker processes.
    '''

    STORAGE_FORMATS = ['pickle', 'orjson', 'pickle_z', 'orjson_z', 'marshal']
    DEFAULT_STORAGE_FORMAT = 'pickle'

    def __init__(
            self,
            path: str = None,
            *,
            batch_size: int = None,
            keep_files: bool = None,
            storage_format: str = None,
    ):
        '''
        `path`: absolute path to a directory where data for the list
            are or will be stored.

        if `path` is `None`:

            A new Biglist is created and a temporary directory is created for it
            to store its data.

            The storage will be kept or deleted after use, according to `keep_files`.
            If `keep_files` is `None` (the default), it will be changed to `False`.

            `storage_format` specifies format of the persistent files.

        If `path` is not `None`:

            If the path is empty, a new Biglist object is created and will use that directory
            for storage.

                The storage will be kept or deleted after use, according to `keep_files`.
                If `keep_files` is `None` (the default), it will be changed to `True`.

            If the path is NOT empty, then the directory must be a directory
            that is the storage space for an existing Biglist.
            This `__init__` points to this path, ready to read the existing data,
            and append to it.

                `storage_format` is ignored. The format of existing data
                is detected and used.

            If `keep_files` is not specified (i.e. at the default value `None`),
            then files will not be deleted. Otherwise, they are kept or deleted
            as requested.

        `batch_size`: number of list elements contained in the in-memory buffer as well as in
            one on-disk file (multiple files will be created as needed).

            If `path` points to an existing `Biglist`,
            `batch_size` should not be specified. The batch size
            of the existing Biglist will be used.
        '''
        self.path = path
        self._read_buffer = None
        self._read_buffer_file_idx = None
        # `self._read_buffer` contains the content of the file
        # indicated by `self._read_buffer_file_idx`.

        self.file_lengths = []
        self.cum_file_lengths = [0]
        self._append_buffer = []
        self._storage_format = storage_format

        if not self.path:
            self.path = tempfile.mkdtemp()
            if keep_files is None:
                keep_files = False
        else:
            if keep_files is None:
                keep_files = True
        self._keep_files = keep_files

        assert self.path.startswith('/')

        if os.path.isdir(self.path):
            z = os.listdir(self.path)
            if z:
                err_msg = f"path '{self.path}' is not empty but is not a valid {self.__class__.__name__} folder"
                if not os.path.isdir(self.data_dir):
                    raise Exception(err_msg)
                if os.path.isfile(self.info_file):
                    info = json_load(self.info_file)
                else:
                    raise Exception(err_msg)

                self.file_lengths = info['file_lengths']
                if batch_size is None:
                    batch_size = info['batch_size']
                else:
                    if batch_size != info['batch_size']:
                        raise ValueError(
                            f"`batch_size` does not agree with the existing value")
                for n in self.file_lengths:
                    self.cum_file_lengths.append(self.cum_file_lengths[-1] + n)
                self._storage_format = info['storage_format']
            else:
                os.makedirs(self.data_dir)

                if self._storage_format:
                    assert self._storage_format in self.STORAGE_FORMATS
                else:
                    self._storage_format = self.DEFAULT_STORAGE_FORMAT
        else:
            os.makedirs(self.data_dir)

            if self._storage_format:
                assert self._storage_format in self.STORAGE_FORMATS
            else:
                self._storage_format = self.DEFAULT_STORAGE_FORMAT

        if batch_size is None:
            batch_size = 10000
        else:
            assert batch_size > 0
        self.batch_size = batch_size

        self._file_dumper = Dumper()
        self._file_loader = Loader()

    @classmethod
    def convert_to_disk(self, x):
        # Transforms an incoming element (i.e. arguments to `append`).
        # Convention is that this is a `to_dict` type of function
        # that converts an object of a custom class to Python
        # native types, so that data persisted in files do not
        # contain custom types.
        #
        # This should be a class method.
        # Do not override it by an instance method or simple attribute.
        # The reason has to do with multiprocessing, i.e.
        # when sending `self.file_views` to other processes.
        return x

    @classmethod
    def convert_from_disk(self, x):
        return x

    @property
    def info_file(self) -> str:
        return os.path.join(self.path, 'info.json')

    @property
    def data_dir(self) -> str:
        return os.path.join(self.path, 'store')

    def data_file(self, file_idx: int) -> str:
        return os.path.join(
            self.data_dir,
            str(file_idx) + '.' + self._storage_format,
        )

    def __len__(self) -> int:
        n = self.cum_file_lengths[-1]
        if self._append_buffer:
            return n + len(self._append_buffer)
        return n

    def __bool__(self) -> bool:
        return len(self) > 0

    def _clear(self) -> None:
        '''
        Clears all the disk files and releases all in-memory data held by this object,
        so that the object is as if upon `__init__` with an empty directory pointed to
        by `self.path`.
        '''
        if os.path.isdir(self.path):
            shutil.rmtree(self.path)
        os.makedirs(self.data_dir)
        self.file_lengths = []
        self.cum_file_lengths = [0]
        self._read_buffer = None
        self._read_buffer_file_idx = None
        self._append_buffer = []
        self._file_dumper.cancel()
        self._file_loader.cancel()

    def destroy(self) -> None:
        '''
        After this method is called, this object is no longer usable.
        '''
        self._clear()
        if os.path.isdir(self.path):
            shutil.rmtree(self.path)

    def __del__(self) -> None:
        self._file_loader.cancel()
        if self._keep_files:
            self.flush()
            self._file_dumper.wait()
        else:
            self._file_dumper.cancel()
            self.destroy()

    def append(self, x) -> None:
        '''
        Append a single element to the in-memory buffer.
        Once the buffer size reaches `self.batch_size`, the buffer's content
        will be written to a file, and the buffer will re-start empty.

        In other words, whenever `self._append_buffer` is non-empty,
        its content is not written to disk yet.
        However, at any time, the content of this buffer is included in
        `self.__len__` and in element accesses, including iterations.
        '''
        x = self.convert_to_disk(x)

        if not self._append_buffer:
            # Three possibilities:
            # (1) The object is new, just starting; or the buffer has just
            #     been dumped by prev call.
            # (2) The object has been open for reading, hence the buffer
            #     has not been used yet.
            # (3) `self.flush` has just been called. In this case, the last
            #     saved file may be "partial".
            #
            # In cases (2)-(3), we need to check whether the final data file
            # is not as big as `batch_size`. If so, we read it in to append,
            # so that all the saved files have the same number of elements
            # except for the very last file.
            if self.file_lengths:
                if self.file_lengths[-1] < self.batch_size:
                    self._append_buffer = self._load_file(self.file_count - 1)
                    # Note:
                    # do not delete the last file.
                    # In `flush`, overwrite this file if it exists.
                    # If the current object is created only to read existing data,
                    # then the user will not call `flush` after use.
                    # By keeping the original file, data is not lost.

                    self.file_lengths.pop()
                    self.cum_file_lengths.pop()

        self._append_buffer.append(x)
        if len(self._append_buffer) >= self.batch_size:
            self.flush()

    def extend(self, x: Iterable) -> None:
        for v in x:
            self.append(v)

    def _load_file(self, idx: int, read_ahead=0) -> List:
        path = self.data_file(idx)
        while self._file_dumper.has_file(path):
            time.sleep(0.07)
        paths = [path]
        if read_ahead:
            for k in range(read_ahead):
                idx += 1
                if idx < self.file_count:
                    paths.append(self.data_file(idx))
                else:
                    break
        return self._file_loader.load_file(*paths)

    def _get_file_idx_for_item(self, idx: int) -> int:
        if idx >= self.cum_file_lengths[-1]:
            return len(self.file_lengths)
            # This suggests the requested element at index `idx`
            # resides in `self._append_buffer`.
        if self._read_buffer_file_idx is None:
            for k in range(len(self.cum_file_lengths)):
                if idx < self.cum_file_lengths[k]:
                    return k - 1
        elif idx < self.cum_file_lengths[self._read_buffer_file_idx]:
            for k in range(self._read_buffer_file_idx - 1, -1, -1):
                if idx >= self.cum_file_lengths[k]:
                    return k
        elif idx >= self.cum_file_lengths[self._read_buffer_file_idx + 1]:
            for k in range(self._read_buffer_file_idx + 2, len(self.cum_file_lengths)):
                if idx < self.cum_file_lengths[k]:
                    return k - 1
        else:
            return self._read_buffer_file_idx

    def flush(self) -> None:
        '''
        Persist the content of the in-memory buffer to a disk file,
        reset the buffer, and update relevant book-keeping variables.

        This method is called any time the size of the in-memory buffer 
        reaches `self.batch_size`. This happens w/o the user's intervention.

        When the user is done adding elements to the list, the buffer size
        may not happen to be `self.batch_size`, hence this method is not called
        automatically,
        and the last chunk of elements are not persisted on disk.
        This is when the *user* should call this method.
        (If user does not call, it is called when this object is going out of scope,
        as appropriate.)

        In summary, call this method once the user is done with adding elements
        to the list *in this session*, meaning in this run of the program.
        '''
        if not self._append_buffer:
            return

        buffer_len = len(self._append_buffer)

        self._file_dumper.dump_file(
            self._append_buffer,
            self.data_file(len(self.file_lengths))
        )
        self.file_lengths.append(buffer_len)
        self.cum_file_lengths.append(self.cum_file_lengths[-1] + buffer_len)
        json_dump(
            {'file_lengths': self.file_lengths,
             'batch_size': self.batch_size,
             'storage_format': self._storage_format,
             },
            self.info_file,
        )

        self._append_buffer = []
        if self._read_buffer_file_idx == len(self.file_lengths) - 1:
            # This indicates the read buffer was pointing to the append
            # buffer. Now that the append buffer has been dumped,
            # the next read op will need to re-determine how to
            # populate the read buffer.
            self._read_buffer = None
            self._read_buffer_file_idx = None

    def __getitem__(self, idx: int):
        '''
        Element access by single index; negative index works as expected.
        '''
        if not isinstance(idx, int):
            raise TypeError(
                f'{self.__class__.__name__} indices must be integers, not {type(idx).__name__}')

        idx = range(len(self))[idx]
        file_idx = self._get_file_idx_for_item(idx)

        if file_idx >= len(self.file_lengths):
            elem = self._append_buffer[idx - self.cum_file_lengths[-1]]
        else:
            if file_idx != self._read_buffer_file_idx:
                self._read_buffer = self._load_file(file_idx)
                self._read_buffer_file_idx = file_idx

            n1 = self.cum_file_lengths[file_idx]
            n2 = self.cum_file_lengths[file_idx + 1]
            assert n1 <= idx < n2
            elem = self._read_buffer[idx - n1]

        return self.convert_from_disk(elem)

    def __iter__(self):
        '''
        Iterate over single elements.
        '''
        for file_idx in range(len(self.file_lengths)):
            if file_idx != self._read_buffer_file_idx:
                self._read_buffer = self._load_file(file_idx, read_ahead=2)
                self._read_buffer_file_idx = file_idx

            yield from (self.convert_from_disk(v) for v in self._read_buffer)
        if self._append_buffer:
            yield from (self.convert_from_disk(v) for v in self._append_buffer)

    def view(self) -> 'ListView':
        assert not self._append_buffer
        return ListView(self.__class__(self.path))

    def file_view(self, file_idx) -> 'ListView':
        assert 0 <= file_idx < self.file_count
        return ListView(
            self.__class__(self.path),
            range(self.cum_file_lengths[file_idx],
                  self.cum_file_lengths[file_idx+1]),
        )

    def file_views(self) -> List['ListView']:
        assert not self._append_buffer
        return [
            self.file_view(idx)
            for idx in range(self.file_count)
        ]

    @property
    def file_ranges(self) -> List:
        # Index ranges of items stored in the files, in order.
        return [
            [a, b]
            for a, b in zip(self.cum_file_lengths[:-1],
                            self.cum_file_lengths[1:])
        ]

    @property
    def file_count(self):
        return len(self.file_lengths)

    def move(self, path: str, overwrite: bool = False, keep_files: bool = True) -> 'self':
        '''
        Note that after this operation, existing files of this object are moved
        to the new path, but if there are data in buffer that is not persisted yet,
        they are not flushed. One still needs to call `flush` to conclude the object's
        data population at the end of the session.

        After `move`, this object continues its life---only underlying storage
        location has changed---in particular, all attributes in memory remain valid.
        '''
        assert path.startswith('/')
        if os.path.exists(path):
            if path == self.path:
                return self
            if os.listdir(path) and not overwrite:
                raise FileExistsError(
                    f"destination directory `{path}` already exists")
            else:
                shutil.rmtree(path)
        shutil.move(self.path, path)
        self.path = path
        self._keep_files = keep_files
        return self

    def deepcopy(self, path: str = None, *, overwrite: bool = False, keep_files: bool = None) -> 'Biglist':
        '''
        This op copies over all the contents of the current object
        into a new object of the same class and returns the new object.
        This is much faster than instantiating a new, empty object,
        and populating it via `append` or `extend` with elements of the
        current object.
        '''
        if path:
            assert path.startswith('/')
            if os.path.exists(path):
                if os.listdir(path) and not overwrite:
                    raise FileExistsError(
                        f'destination directory "{path}" is not empty')
                else:
                    shutil.rmtree(path)

            newlist = self.__class__(
                path=path, batch_size=self.batch_size,
                keep_files=keep_files, storage_format=self._storage_format)
        else:
            newlist = self.__class__(
                batch_size=self.batch_size,
                keep_files=keep_files, storage_format=self._storage_format)

        # additional call to `rmtree` here because `copytree` fails if dest dir exists
        # (even if empty). Python 3.8 has additional arg `dirs_exist_ok`.
        shutil.rmtree(newlist.path)
        shutil.copytree(self.path, newlist.path)
        newlist.file_lengths = self.file_lengths[:]
        newlist.cum_file_lengths = self.cum_file_lengths[:]
        newlist._append_buffer = deepcopy(self._append_buffer)
        return newlist

    def enter(self):
        # There's no need to use context manager
        # if opening an existing Biglist for read only.
        return self

    def __exit__(self, *args, **kwargs):
        if self._keep_files:
            self.flush()
        else:
            self.destroy()


class ListView(Sequence):
    def __init__(self, list_: Sequence, range_: range = None):
        '''
        This provides a "window" into the sequence `list_`,
        which is often a `Biglist` or another `ListView`.

        An object of `ListView` is created by `Biglist.view` or
        by slicing a `ListView`.
        User should not attempt to create an object of this class directly.

        The main purpose of this class is to provide slicing over `Biglist`.

        During the use of this object, it is assumed that the underlying
        `list_` is not changing. Otherwise the results may be incorrect.
        '''
        if range_ is None:
            range_ = range(len(list_))
        self._list = list_
        self._range = range_

    def __len__(self) -> int:
        return len(self._range)

    def __bool__(self) -> bool:
        return len(self) > 0

    def __getitem__(self, idx: Union[int, slice]):
        '''
        Element access by a single index or by slice.
        Negative index and standard slice syntax both work as expected.

        Sliced access returns a new `ListView` object.
        '''
        if isinstance(idx, int):
            return self._list[self._range[idx]]

        if not isinstance(idx, slice):
            range_ = self._range[idx]
            return self.__class__.__name__(self._list, range_)

        raise TypeError(
            f'{self.__class__.__name__} indices must be integers or slices, not {type(idx).__name__}')
