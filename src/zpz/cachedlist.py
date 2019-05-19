import os
import os.path
import pickle
from shutil import rmtree
from typing import Union, Iterable

from pympler import asizeof

from .exceptions import ZpzError


class CachedAppendOnlyList:
    def __init__(self, path: str = None, batch_size: int = None, append: bool = False):
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
            self._buffer_cap = -1
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
                info = pickle.load(self._info_file)
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
        if self._path:
            rmtree(self._path)
            os.makedires(self._store_dir)
        self._file_lengths = []
        self._cum_file_lengths = [0]
        self._read_buffer = None
        self._read_buffer_file_idx = None
        self._append_buffer = []
        self._len = 0

    def purge(self) -> None:
        self.clear()
        if self._path:
            rmtree(self._path)

    @property
    def batch_size(self) -> int:
        return self._buffer_cap

    def append(self, x) -> None:
        if self._buffer_cap is None:
            size = asizeof.asizeof(x)  # in bytes
            self._buffer_cap = 1024 * 1024 * 32 // size

        self._append_buffer.append(x)
        self._len += 1
        if self._buffer_cap > 0 and len(self._append_buffer) >= self._buffer_cap:
            self._flush()

    def extend(self, x: Iterable) -> None:
        for v in x:
            self.append(v)

    def _load_file_to_buffer(self, idx: int):
        self._read_buffer = pickle.load(open(
            os.path.join(self._store_dir, str(idx) + '.pickle'), 'rb'))
        self._read_buffer_file_idx = idx

    def _get_file_idx_for_item(self, idx: int) -> int:
        if idx >= self._cum_file_lengths[-1]:
            return len(self._cum_file_lengths)
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
        if idx < 0 or idx >= self._len:
            raise IndexError(f"index '{idx}' out of range'")
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
        if isinstance(idx, slice):
            return self._getslice(idx)
        return self._getone(idx)

    def __iter__(self):
        for i in range(self._len):
            yield self._getone(i)

    def items(self):
        '''
        Suppose `obj` is an object of this class, then

            for x in obj:
                ...

        is equivalent to

            for x in obj.items():
                ...
        '''
        return self.__iter__()

    def batches(self, batch_size: int = None):
        '''
        Suppose `obj` is an object of this class, then

            for batch in obj.batches(100):
                # `batch` is a list of up to 100 items
                ...
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

    def _flush(self) -> None:
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

    def __del__(self):
        self._flush()
