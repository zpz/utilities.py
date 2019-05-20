import os
import os.path
import pickle
from shutil import rmtree
from typing import Union, Iterable
import warnings

from pympler import asizeof
from .exceptions import ZpzError


class Biglist:
    '''
    `Biglist` implements a single-machine, out-of-memory list, that is,
    the list can exceed the capacity of the memory, but can be stored on the hard drive
    of the single machine.

    The list can be appended to via `append` (discouraged) and `extend` (recommended).
    Elements can be accessed by single index, slice, and a single-element iterator or batch iterator.

    Existing elements can not be modified.

    `batch_size`, in terms of number of elements, is both the in-memory buffer size
    and file size. When accessing elements by slice or by batch iterator, this is the upper limit
    of the number of elements obtained in one operation.

    The most efficient usage pattern is: specify `batch_size` at initiation to be the batch size
    of intended future use, and iterate by batches without specifying `batch_size` (hence
    the same `batch_size` specified to `__init__` will be used).

        mylist = Biglist(path=mypath, batch_size=1000)
        ...
        mylist.extend([...])
        mylist.extend([...])
        ...
        mylist.flush()

        for batch in mylist.batches():
            ... # process `batch`
    '''
    def __init__(self, path: str = None, batch_size: int = None, append: bool = False):
        '''
        `path`: absolute path to a directory where data for the list will be stored.
            If the directory is not empty, it must be a directory that was created previously by
            `Biglist`.

            If not specified, the whole list stays in memory and is never written to disk.
            It's not unlike a regular `list`.
            This usecase defeats the purpose of this class, although it should work.

        `batch_size`: number of list elements contained in the in-memory buffer and
            one on-disk file (multiple files will be created as needed).

        `append`: if `path` is not empty, it must be a directory that was previously operated
            by `Biglist`; moreover, `append` must be `True`, indicating the caller intentionally
            opens an existing list for reading and extending.
        '''
        self._path = path
        self._buffer_cap = batch_size
        self._file_lengths = []
        self._cum_file_lengths = [0]
        self._read_buffer = None
        self._read_buffer_file_idx = None
        # `self._read_buffer` contains the content of the file indicated by `self._read_buffer_file_idx`.
        self._append_buffer = []
        self._len = 0

        if not self._path:
            if batch_size is None:
                self._buffer_cap = -1
            else:
                raise ValueError('`batch_size` should not be specified when `path` is missing')
            return

        assert self._path.startswith('/')
        self._info_file = os.path.join(self._path, 'info.pickle')
        self._store_dir = os.path.join(self._path, 'store')

        if os.path.isdir(self._path):
            z = os.listdir(self._path)
            if z:
                if not append:
                    raise ZpzError(f"path '{self._path}' is not empty")
                if (not os.path.isfile(self._info_file)
                    or not os.path.isdir(self._store_dir)):
                    raise ZpzError(f"path '{self._path}' is not empty and is not a valid {self.__class__.__name__} folder")
                info = pickle.load(open(self._info_file, 'rb'))
                self._file_lengths = info['file_lengths']
                if self._buffer_cap is None:
                    self._buffer_cap = info['buffer_cap']
                else:
                    if self._buffer_cap != info['buffer_cap']:
                        raise ZpzError(f"`batch_size` does not agree with the existing value")
                if self._file_lengths:
                    if self._file_lengths[-1] < self._buffer_cap:
                        fname = os.path.join(self._store_dir, str(len(self._file_lengths) - 1) + '.pickle')
                        self._append_buffer = pickle.load(open(fname, 'rb'))
                        os.remove(fname)
                        self._file_lengths.pop()

                self._len = sum(self._file_lengths) + len(self._append_buffer)
                self._cum_file_lengths = [0]
                for n in self._file_lengths:
                    self._cum_file_lengths.append(self._cum_file_lengths[-1] + n)
            else:
                os.makedirs(self._store_dir)
        else:
            os.makedirs(self._store_dir)

    def __len__(self) -> int:
        return self._len

    def __bool__(self) -> bool:
        return self._len > 0

    def clear(self) -> None:
        '''
        Clears all the disk files and releases all in-memory data held by this object,
        so that the object is as if upon `__init__` with an empty directory pointed to
        by `path`.
        '''
        if self._path:
            rmtree(self._path)
            os.makedirs(self._store_dir)
        self._file_lengths = []
        self._cum_file_lengths = [0]
        self._read_buffer = None
        self._read_buffer_file_idx = None
        self._append_buffer = []
        self._len = 0

    @property
    def batch_size(self) -> int:
        return self._buffer_cap

    @classmethod
    def max_batch_size(cls, x) -> int:
        '''
        `x` is an element of the list.
        Assuming all elements are like `x` with similar size,
        this function calculates the number of such elements
        that occupy 64MB of memory.

        This is a guideline of the largest batch size to use.
        '''
        size = asizeof.asizeof(x)  # in bytes
        return (((1024 * 1024 * 64) // size) // 100) * 100

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
        if self._buffer_cap is None:
            self._buffer_cap = self.max_batch_size(x)

        self._append_buffer.append(x)
        self._len += 1
        if self._buffer_cap > 0 and len(self._append_buffer) >= self._buffer_cap:
            self.flush()

    def extend(self, x: Iterable) -> None:
        for v in x:
            self.append(v)

    def _load_file_to_buffer(self, idx: int):
        self._read_buffer = pickle.load(open(
            os.path.join(self._store_dir, str(idx) + '.pickle'), 'rb'))
        self._read_buffer_file_idx = idx

    def _get_file_idx_for_item(self, idx: int) -> int:
        if idx >= self._cum_file_lengths[-1]:
            return len(self._file_lengths)
        if self._read_buffer_file_idx is None:
            for k in range(len(self._cum_file_lengths)):
                if idx < self._cum_file_lengths[k]:
                    return k-1
        elif idx < self._cum_file_lengths[self._read_buffer_file_idx]:
            for k in range(self._read_buffer_file_idx - 1, -1, -1):
                if idx >= self._cum_file_lengths[k]:
                    return k
        elif idx >= self._cum_file_lengths[self._read_buffer_file_idx + 1]:
            for k in range(self._read_buffer_file_idx + 2, len(self._cum_file_lengths)):
                if idx < self._cum_file_lengths[k]:
                    return k - 1
        else:
            return self._read_buffer_file_idx

    def _getone(self, idx: int):
        if idx >= self._len or idx < -self._len:
            raise IndexError(f"index '{idx}' out of range'")
        if idx < 0:
            idx = self._len + idx
        file_idx = self._get_file_idx_for_item(idx)
        if file_idx >= len(self._file_lengths):
            return self._append_buffer[idx - self._cum_file_lengths[-1]]
        if file_idx != self._read_buffer_file_idx:
            self._load_file_to_buffer(file_idx)
        assert self._cum_file_lengths[file_idx] <= idx < self._cum_file_lengths[file_idx + 1]
        return self._read_buffer[idx - self._cum_file_lengths[file_idx]]

    def _getslice(self, idx: slice):
        start = idx.start or 0
        stop = idx.stop or self._len
        step = idx.step or 1

        if start < 0:
            start = self._len + start
        if stop < 0:
            step = self._len + stop

        n = (stop - start) // step
        if n > self._buffer_cap:
            raise ValueError(f"Requested number of elements, {n}, exceeds buffer capacity.")

        first = start
        last = start + step * (n - 1)
        if first < 0 or first >= self._len or last < 0 or last >= self._len:
            raise IndexError("index out of range")

        fidx_first = self._get_file_idx_for_item(first)
        fidx_last = self._get_file_idx_for_item(last)

        if fidx_first == fidx_last:
            if fidx_first == len(self._file_lengths):
                k0 = self._cum_file_lengths[-1]
                return self._append_buffer[(start - k0) : (stop - k0) : step]
            if fidx_first != self._read_buffer_file_idx:
                self._load_file_to_buffer(fidx_first)
            k0 = self._cum_file_lengths[fidx_first]
            return self._read_buffer[(start - k0) : (stop - k0) : step]

        # Now the requested slice straddles multiple files.
        # Since we only read in a single file at any point in time,
        # the result of this request will be hosted by a separate list.
        result = []
        for fidx in range(fidx_first, fidx_last + (1 if step > 0 else -1), step):
            if fidx == len(self._file_lengths):
                k = self._cum_file_lengths[-1]
                m = self._len
                buffer = self._append_buffer
            else:
                if fidx != self._read_buffer_file_idx:
                    self._load_file_to_buffer(fidx)
                k = self._cum_file_lengths[fidx]
                m = self._cum_file_lengths[fidx + 1]
                buffer = self._read_buffer
            n_result_old = len(result)
            if step > 0:
                result.extend(buffer[(start - k) : min(m - k, stop - k) : step])
            else:
                result.extend(buffer[(start - k) : max(-1, stop - k) : step])
            dn = len(result) - n_result_old
            start = start + step * dn

        return result

    def __getitem__(self, idx: Union[int, slice]):
        '''
        Element access by single index or slice.
        Negative index and all forms of slice work as expected.
        '''
        if isinstance(idx, slice):
            return self._getslice(idx)
        return self._getone(idx)

    def __iter__(self):
        '''
        Iterate over single elements.
        '''
        for i in range(self._len):
            yield self._getone(i)

    def batches(self, batch_size: int = None):
        '''
        Iterate over batches of specified size.
        In general, every batch has the same number of elements
        except for the final batch, which contains however many elements
        remain.

        Suppose `obj` is an object of this class, then

            for batch in obj.batches(100):
                # `batch` is a list of up to 100 items
                ...

        `batch_size`: if missing, an internal value (which is equal to the size of disk files)
        is used. This is the most efficient case, because any single batch will not straddle
        multiple files.
        '''
        if self._path:
            assert self._buffer_cap > 0
            if batch_size is None:
                batch_size = self._buffer_cap
            elif batch_size <= 0:
                batch_size = self._buffer_cap
            elif batch_size > self._buffer_cap:
                raise ZpzError(f"requested `batch_size` ({batch_size}) is larger than the `batch_size` ({self._buffer_cap}) in object")
            else:
                pass
        else:
            assert self._buffer_cap > 0
            if batch_size is None:
                batch_size = self._len
            elif batch_size <= 0:
                batch_size = self._len
            else:
                pass

        n_done = 0
        while n_done < self._len:
            n_todo = min(self._len - n_done, batch_size)
            yield self._getslice(slice(n_done, n_done + n_todo))
            n_done += n_todo

    def flush(self) -> None:
        '''
        Persist the content of the in-memory buffer to a disk file,
        reset and the buffer, and update relevant book-keeping variables.

        This method is called any time the size of the in-memory buffer 
        reaches `self.batch_size`. This happens w/o the user's intervention.

        When the user is done adding elements to the list, the buffer size
        may not happen to be `self.batch_size`, hence this method is not called,
        and the last chunk of elements are not persisted on disk.
        This is when the user should call this method.

        In summary, call this method once the user is done with adding elements
        to the list *in this session*, meaning in this run of the program.
        '''
        if not self._append_buffer:
            return
        if not self._path:
            return
        file_name = os.path.join(self._store_dir, str(len(self._file_lengths)) + '.pickle')
        pickle.dump(self._append_buffer, open(file_name, 'wb'))
        self._file_lengths.append(len(self._append_buffer))
        self._cum_file_lengths.append(self._cum_file_lengths[-1] + len(self._append_buffer))
        pickle.dump({'file_lengths': self._file_lengths, 'buffer_cap': self._buffer_cap},
            open(self._info_file, 'wb'))
        self._append_buffer = []
