import os
import os.path
import shutil
import tempfile
from copy import deepcopy
from typing import Union, List, Callable, Type, Iterable

from .serde import (
    json_load, json_dump,
    pickle_z_load, pickle_z_dump,
)


class Biglist:
    '''
    `Biglist` implements a single-machine, out-of-memory list, that is,
    the list may exceed the capacity of the memory, but can be stored on the hard drive
    of the single machine.

    The list can be appended to, but existing elements can not be modified.

    Single elements can be accessed via the standard `[index]` syntax.
    The object can be iterated over to walk through the elements one by one.

    Accessing a range of elements by the slicing syntax `[start:stop:step]` is not supported.
    This is because that, by convention, slicing should return an object of the same type,
    but that is not possible with `Biglist`.

    However, the property `view` returns a `BiglistView`, which supports indexing,
    slicing, iterating.
    '''

    element_write_converter: Callable = None
    element_read_converter: Callable = None

    def __init__(
            self,
            path: str = None,
            batch_size: int = None,
            *,
            keep_files: bool = None,
    ):
        '''
        `path`: absolute path to a directory where data for the list will be stored.
            If the directory is not empty, it must be a directory that was created previously by
            `Biglist`.

        if `path` is `None`:

            A new Biglist is created and a temporary directory is created for it
            to store its data.

            The storage will be kept or deleted after use, according to `keep_files`.
            If `keep_files` is `None` (the default), it will changed to `False`.

        If `path` is not `None`:

            If the path is empty, a new Biglist object is created and will use that directory
            for storage.

                The storage will be kept or deleted after use, according to `keep_files`.
                If `keep_files` is `None` (the default), it will changed to `True`.

            If the path is NOT empty, then the directory must be a directory
            that is the storage space for an existing Biglist.
            `__init__` gets read to read this Biglist.

                `batch_size` should not be specified in this case. The batch size
                of the existing Biglist will be used. If `batch_size` is specified,
                it must match the existing value, i.e. it is redundant, but a wrong value
                is an error.

        `batch_size`: number of list elements contained in the in-memory buffer as well as in
            one on-disk file (multiple files will be created as needed).
        '''
        self.path = path
        self.file_lengths = []
        self.cum_file_lengths = [0]

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
                if not os.path.isfile(self.info_file) or not os.path.isdir(self.data_dir):
                    raise RuntimeError(f"path '{self._path}' is not empty "
                                       f"but is not a valid {self.__class__.__name__} folder")
                info = json_load(self.info_file)

                self.file_lengths = info['file_lengths']
                if batch_size is None:
                    batch_size = info['batch_size']
                else:
                    if batch_size != info['batch_size']:
                        raise ValueError(
                            f"`batch_size` does not agree with the existing value")
                for n in self.file_lengths:
                    self.cum_file_lengths.append(self.cum_file_lengths[-1] + n)
            else:
                os.makedirs(self.data_dir)
        else:
            os.makedirs(self.data_dir)

        if batch_size is None:
            batch_size = 10000
        else:
            assert batch_size > 0
        self.batch_size = batch_size
        self._read_buffer = None
        self._read_buffer_file_idx = None
        # `self._read_buffer` contains the content of the file
        # indicated by `self._read_buffer_file_idx`.
        self._append_buffer = None

    @property
    def info_file(self) -> str:
        return os.path.join(self.path, 'info.json')

    @property
    def data_dir(self) -> str:
        return os.path.join(self.path, 'store')

    def data_file(self, file_idx: int) -> str:
        return os.path.join(self.data_dir, str(file_idx) + '.pickle_z')

    def __len__(self) -> int:
        n = self.cum_file_lengths[-1]
        if self._append_buffer:
            return n + len(self._append_buffer)
        return n

    def __bool__(self) -> bool:
        return len(self) > 0

    def clear(self) -> None:
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
        self._append_buffer = None

    def destroy(self) -> None:
        '''
        After this method is called, this object is no longer usable.
        '''
        self.clear()
        if os.path.isdir(self.path):
            shutil.rmtree(self.path)

    def __del__(self) -> None:
        if self._keep_files:
            self.flush()
        else:
            self.destroy()

    def _init_append_buffer(self) -> None:
        # This method is called only when `self._append_buffer` is `None`.
        # This value has been set either in `flush` or in `__init__`.

        if self.file_lengths and self.file_lengths[-1] < self.batch_size:
            self._append_buffer = pickle_z_load(
                self.data_file(len(self.file_lengths) - 1))

            if self._read_buffer_file_idx == len(self.file_lengths) - 1:
                # When there is an active `self._append_buffer`,
                # the in-memory file list includes the "full-length" files only.
                # If the final partial data file has been loaded into `self._read_buffer`
                # (when `self._append_buffer` is `None`), we need to null it.
                self._read_buffer_file_idx = None
                self._read_buffer = None

            # Note:
            # do not delete the last file.
            # In `flush`, overwrite this file if it exists.
            # If the current object is created only to read existing data,
            # then the user will not call `flush` after use.
            # By keeping the original file, data is not lost.
            self.file_lengths.pop()
            self.cum_file_lengths.pop()
        else:
            self._append_buffer = []

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
        if self.element_write_converter is not None:
            x = self.element_write_converter(x)

        if self._append_buffer is None:
            self._init_append_buffer()

        self._append_buffer.append(x)
        if len(self._append_buffer) >= self.batch_size:
            self.flush()

    def extend(self, x: Iterable) -> None:
        for v in x:
            self.append(v)

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
        pickle_z_dump(self._append_buffer,
                      self.data_file(len(self.file_lengths)))
        self.file_lengths.append(buffer_len)
        self.cum_file_lengths.append(self.cum_file_lengths[-1] + buffer_len)
        json_dump(
            {'file_lengths': self.file_lengths,
             'batch_size': self.batch_size,
             },
            self.info_file)

        self._append_buffer = [] if buffer_len == self.batch_size else None

    def _get_file_idx_for_item(self, idx: int) -> int:
        if idx >= self.cum_file_lengths[-1]:
            return len(self.file_lengths)
            # This suggests the requested element at index `idx`
            # resides in `self._append_buffer`.
        if self._read_buffer_file_idx is None:
            for k, n in enumerate(self.cum_file_lengths):
                if idx < n:
                    return k-1
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

    def __getitem__(self, idx: int):
        '''
        Element access by single index; negative index works as expected.
        '''
        if not isinstance(idx, int):
            raise TypeError(
                'A single integer index is expected. To use slice, use `view`.')

        idx = range(len(self))[idx]
        file_idx = self._get_file_idx_for_item(idx)

        if file_idx >= len(self.file_lengths):
            elem = self._append_buffer[idx - self.cum_file_lengths[-1]]
        else:
            if file_idx != self._read_buffer_file_idx:
                self._read_buffer = pickle_z_load(self.data_file(file_idx))
                self._read_buffer_file_idx = file_idx

            n1 = self.cum_file_lengths[file_idx]
            n2 = self.cum_file_lengths[file_idx + 1]
            assert n1 <= idx < n2
            elem = self._read_buffer[idx - n1]

        if self.element_read_converter is not None:
            elem = self.element_read_converter(elem)
        return elem

    def _iter_buffer(self, buffer):
        if self.element_read_converter is None:
            yield from buffer
        else:
            for v in buffer:
                yield self.element_read_converter(v)

    def __iter__(self):
        '''
        Iterate over single elements.
        '''
        for file_idx in range(len(self.file_lengths)):
            if file_idx == self._read_buffer_file_idx:
                buffer = self._read_buffer
            else:
                buffer = pickle_z_load(self.data_file(file_idx))
            yield from self._iter_buffer(buffer)

        if self._append_buffer is not None:
            yield from self._iter_buffer(self._append_buffer)

    def iterfile(self, file_idx):
        assert 0 <= file_idx < len(self.file_lengths)
        if file_idx == self._read_buffer_file_idx:
            buffer = self._read_buffer
        else:
            buffer = pickle_z_load(self.data_file(file_idx))
        yield from self._iter_buffer(buffer)

    def view(self) -> 'BiglistView':
        # During the use of this view, the underlying Biglist should not change.
        # Multiple frozenview's may be used to view
        # different parts of the underlying Biglist--they open
        # and read files independent of other frozenview's.
        assert not self._append_buffer
        return BiglistView(self.path, self.__class__)

    def fileview(self, file_idx: int) -> 'BiglistView':
        assert not self._append_buffer
        assert 0 <= file_idx < len(self.file_lengths)
        return BiglistView(
            self.path, self.__class__,
            range(self.cum_file_lengths[file_idx],
                  self.cum_file_lengths[file_idx+1]),
        )

    def fileviews(self) -> List['BiglistView']:
        return [
            self.fileview(i)
            for i in range(len(self.file_lengths))
        ]

    def move(self, path: str, keep_files: bool = True) -> None:
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
            if os.listdir(path):
                raise FileExistsError(
                    f"destination directory `{path}` already exists")
            else:
                os.rmdir(path)
        shutil.move(self.path, path)
        self.path = path
        self._keep_files = keep_files

    def deepcopy(self, path: str = None, *, keep_files: bool = None) -> 'Biglist':
        if path:
            assert path.startswith('/')
            if os.path.exists(path) and os.listdir(path):
                raise FileExistsError(
                    f'destination directory "{path}" is not empty')
        newlist = self.__class__(
            path=path, batch_size=self.batch_size, keep_files=keep_files)

        # additional call to `rmtree` here because `copytree` fails if dest dir exists
        # (even if empty). Python 3.8 has additional arg `dirs_exist_ok`.
        shutil.rmtree(newlist.path)
        shutil.copytree(self.path, newlist.path)
        newlist.file_lengths = self.file_lengths[:]
        newlist.cum_file_lengths = self.cum_file_lengths[:]
        newlist._append_buffer = deepcopy(self._append_buffer)
        return newlist


class BiglistView:
    def __init__(self, path: str, biglist_cls: Type[Biglist] = None, range_: range = None):
        '''
        An object of `ListView` is created by `Biglist` or `BiglistView`.
        User should not attempt to create an object of this class directly.
        '''
        if biglist_cls is None:
            biglist_cls = Biglist
        assert path
        self._biglist_cls = biglist_cls
        self._path = path
        self._list = None
        self._range = range_

    def _open_list(self):
        if self._list is None:
            self._list = self._biglist_cls(self._path)
            if self._range is None:
                self._range = range(len(self._list))

    def __len__(self) -> int:
        if self._list is None:
            self._open_list()
        return len(self._range)

    def __bool__(self) -> bool:
        return len(self) > 0

    def __getitem__(self, idx: Union[int, slice]):
        '''
        Element access by a single index or by slice.
        Negative index and standard slice syntax both work as expected.

        Sliced access returns a new `BiglistView` object.
        '''
        if self._list is None:
            self._open_list()

        if isinstance(idx, int):
            return self._list[self._range[idx]]

        if not isinstance(idx, slice):
            raise TypeError(f"an integer or slice is expected")

        return self.__class__(
            path=self._path,
            biglist_cls=self._biglist_cls,
            range_=self._range[idx],
        )

    def _fileview_idx(self):
        if self._range.step != 1:
            return None
        try:
            idx = self._list.cum_file_lengths.index(self._range.start)
        except ValueError:
            return None
        if idx >= len(self._list.cum_file_lengths) - 1:
            return None
        if self._range.stop != self._list.cum_file_lengths[idx + 1]:
            return None
        return idx

    def __iter__(self):
        if self._list is None:
            self._open_list()

        file_idx = self._fileview_idx()
        if file_idx is None:
            for idx in self._range:
                yield self._list[idx]
        else:
            yield from self._list.iterfile(file_idx)
