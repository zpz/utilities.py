from collections import defaultdict
import os
import os.path
import pickle
import shutil
import tempfile
from typing import Union, Iterable, Sequence, List, Dict, TypeVar, Callable
import warnings

from .exceptions import ZpzError


def regulate_index(idx, length) -> int:
    if idx >= length or idx < -length:
        raise IndexError(f"index '{idx}' out of range'")
    if idx < 0:
        idx = length + idx
    return idx


class Biglist:
    '''
    `Biglist` implements a single-machine, out-of-memory list, that is,
    the list may exceed the capacity of the memory, but can be stored on the hard drive
    of the single machine.

    The list can be appended to via `append` and `extend`.
    Existing elements can not be modified.

    Single elements can be accessed via the standard `[index]` syntax.
    The object can be iterated over to walk through the elements one by one.

    Accessing a range of elements by the slicing syntax `[start:stop:step]` is not supported.
    This is because that, by convention, slicing should return an object of the same type,
    but that is not possible with `Biglist`.

    However, the property `view` returns a `BiglistView`, which supports indexing,
    slicing, iterating, and iterating by batches.
    '''
    def __init__(self, path: str = None, batch_size: int = None):
        '''
        `path`: absolute path to a directory where data for the list will be stored.
            If the directory is not empty, it must be a directory that was created previously by
            `Biglist`.

            If not specified, a temporary directory is used and deleted when the program terminates.

        `batch_size`: number of list elements contained in the in-memory buffer and
            one on-disk file (multiple files will be created as needed).
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

        if self._buffer_cap is not None:
            assert self._buffer_cap > 0

        if not self._path:
            self._path = tempfile.mkdtemp()
            self._use_temp_path = True
        else:
            self._use_temp_path = False

        assert self._path.startswith('/')

        if os.path.isdir(self._path):
            z = os.listdir(self._path)
            if z:
                if (not os.path.isfile(self._info_file) or not os.path.isdir(self._store_dir)):
                    raise ZpzError(f"path '{self._path}' is not empty but is not a valid {self.__class__.__name__} folder")
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
                        # Note: do not delete the file named `fname`.
                        # In `flush`, overwrite this file if it exists.
                        # If this object is created only to read existing data,
                        # then the user will not call `flush` after use.
                        # By keeping the original file named `fname`,
                        # data is not lost.
                        self._file_lengths.pop()

                self._len = sum(self._file_lengths) + len(self._append_buffer)
                for n in self._file_lengths:
                    self._cum_file_lengths.append(self._cum_file_lengths[-1] + n)
            else:
                os.makedirs(self._store_dir)
        else:
            os.makedirs(self._store_dir)

    @property
    def path(self) -> str:
        return self._path

    @property
    def _info_file(self) -> str:
        return os.path.join(self._path, 'info.pickle')

    @property
    def _store_dir(self) -> str:
        return os.path.join(self._path, 'store')

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
        shutil.rmtree(self._path)
        os.makedirs(self._store_dir)
        self._file_lengths = []
        self._cum_file_lengths = [0]
        self._read_buffer = None
        self._read_buffer_file_idx = None
        self._append_buffer = []
        self._len = 0

    def destroy(self) -> None:
        '''
        After this method is called, this object is no longer usable.
        '''
        self.clear()
        shutil.rmtree(self._path)

    def __del__(self) -> None:
        if self._use_temp_path:
            self.destroy()

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
        from pympler import asizeof
        # Import here because `max_batch_size` is expected to be used
        # rarely, and on some systems this import issues a warning.

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
        if len(self._append_buffer) >= self._buffer_cap:
            self.flush()

    def extend(self, x: Iterable) -> None:
        for v in x:
            self.append(v)

    def _load_file_to_buffer(self, idx: int):
        fname = os.path.join(self._store_dir, str(idx) + '.pickle')
        self._read_buffer = pickle.load(open(fname, 'rb'))
        self._read_buffer_file_idx = idx

    def _get_file_idx_for_item(self, idx: int) -> int:
        if idx >= self._cum_file_lengths[-1]:
            return len(self._file_lengths)
            # This suggests the requested element at index `idx`
            # resides in `self._append_buffer`.
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

    def __getitem__(self, idx: int):
        '''
        Element access by single index; negative index works as expected.
        '''
        if not isinstance(idx, int):
            raise ZpzError('A single integer index is expected. To use slice, check out `view`.')

        idx = regulate_index(idx, self._len)
        file_idx = self._get_file_idx_for_item(idx)

        if file_idx >= len(self._file_lengths):
            return self._append_buffer[idx - self._cum_file_lengths[-1]]

        if file_idx != self._read_buffer_file_idx:
            self._load_file_to_buffer(file_idx)

        n1 = self._cum_file_lengths[file_idx]
        n2 = self._cum_file_lengths[file_idx + 1]
        assert n1 <= idx < n2
        return self._read_buffer[idx - n1]

    def __iter__(self):
        '''
        Iterate over single elements.
        '''
        for i in range(self._len):
            yield self.__getitem__(i)

    @property
    def view(self) -> 'ListView':
        return ListView(self)

    def flush(self) -> None:
        '''
        Persist the content of the in-memory buffer to a disk file,
        reset the buffer, and update relevant book-keeping variables.

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
        file_name = os.path.join(self._store_dir, str(len(self._file_lengths)) + '.pickle')
        pickle.dump(self._append_buffer, open(file_name, 'wb'))
        self._file_lengths.append(len(self._append_buffer))
        self._cum_file_lengths.append(self._cum_file_lengths[-1] + len(self._append_buffer))
        pickle.dump({'file_lengths': self._file_lengths, 'buffer_cap': self._buffer_cap},
            open(self._info_file, 'wb'))
        self._append_buffer = []

    def move(self, path: str, overwrite: bool=False) -> None:
        '''
        Note that after this operation, existing files of this object are moved
        to the new path, but if there are data in buffer that is not persisted yet,
        they are not flushed. One still needs to call `flush` to conclude the object's
        data population.
        '''
        assert path.startswith('/')
        if os.path.exists(path):
            if not overwrite:
                raise ZpzError(f"destination directory `{path}` already exists")
            else:
                shutil.rmtree(path)
        shutil.move(self.path, path)
        self._path = path
        self._use_temp_path = False


def slice_to_range(idx: slice, n: int) -> range:
    '''
    This functions takes a `slice`, combined with the length of the sequence,
    to determine an explicit value for each of `start`, `stop`, and `step`,
    and returns a `range` object.

    `n`: length of sequence
    '''
    if n < 1:
        return range(0)

    start, stop, step = idx.start, idx.stop, idx.step
    if step is None:
        step = 1

    if step == 0:
        raise ValueError('slice step cannot be zero')
    elif step > 0:
        if start is None:
            start = 0
        elif start < 0:
            start = max(0, n + start)
        if stop is None:
            stop = n
        elif stop < 0:
            stop = n + stop
    else:
        if start is None:
            start = n - 1
        elif start < 0:
            start = n + start
        if stop is None:
            stop = -1
        elif stop < 0:
            stop = max(-1, n + stop)

    return range(start, stop, step)


def regulate_range(idx: range, n: int) -> range:
    '''
    This functions takes a `range` (such as one returned by `slice_to_range`),
    combined with the length of the sequence,
    to refine the values of `start`, `stop`, and `step`,
    and returns a new `range` object.

    `n`: length of sequence
    '''
    start, stop, step = idx.start, idx.stop, idx.step
    if step > 0:
        if start >= n:
            start, stop, step = 0, 0, 1
        else:
            start = max(start, 0)
            stop = min(stop, n)
            if stop <= start:
                start, stop, step = 0, 0, 1
    else:
        if start < 0:
            start, stop, step = 0, 0, 1
        else:
            start = min(start, n -1)
            stop = max(-1, stop)
            if stop >= start:
                start, stop, step = 0, 0, 1

    return range(start, stop, step)


class ListView:
    def __init__(self, list_: Sequence, range_: range=None):
        '''
        An object of `ListView` is created by `Biglist` or `ListView`.
        User should not attempt to create an object of this class directly.
        '''
        if range_ is None:
            len_ = len(list_)
            range_ = range(len_)
        else:
            n = len(list_)
            range_ = regulate_range(range_, n)
            len_ = len(range_)
        self._list = list_
        self._range = range_
        self._len = len_

    def __len__(self) -> int:
        return len(self._range)

    def __bool__(self) -> bool:
        return len(self) > 0

    def __getitem__(self, idx: Union[int, slice]):
        '''
        Element access by a single index or by slice.
        Negative index and standard slice syntax both work as expected.

        Sliced access returns a `BiglistView` object, which is iterable.
        '''
        if isinstance(idx, int):
            return self._list[self._range[idx]]
        elif isinstance(idx, slice):
            range_ = regulate_range(slice_to_range(idx, self._len), self._len)
            if len(range_) == 0:
                return ListView([])

            start, stop, step = range_.start, range_.stop, range_.step

            start = self._range[start]
            step = self._range.step * step

            if stop >= 0:
                stop = self._range.start + self._range.step * stop
            else:
                assert stop == -1 and range_.step < 0
                if self._range.step > 0:
                    stop = self._range.start - 1
                else:
                    stop = self._range.start + 1

            return ListView(self._list, range(start, stop, step))
        else:
            raise TypeError(f"an integer or slice is expected")

    def __iter__(self):
        for idx in self._range:
            yield self._list[idx]

    @property
    def view(self) -> 'self':
        return self


class ChainListView:
    def __init__(self, *views: Iterable[Sequence]):
        self._views = views
        self._views_len = [len(v) for v in views]
        self._len = sum(self._views_len)

    def __len__(self) -> int:
        return self._len

    def __bool__(self) -> bool:
        return self._len > 0

    def __getitem__(self, idx):
        if isinstance(idx, int):
            idx = regulate_index(idx, self._len)
            for v, k in zip(self._views, self._views_len):
                if idx >= k:
                    idx -= k
                else:
                    return v[idx]
        elif isinstance(idx, slice):
            range_ = regulate_range(slice_to_range(idx, self._len), self._len)
            if len(range_) == 0:
                return ChainListView([])

            start, stop, step = range_.start, range_.stop, range_.step

            views = []
            if step > 0:
                for v, k in zip(self._views, self._views_len):
                    if start >= k:
                        start -= k
                        stop -= k
                    else:
                        if stop <= k:
                            views.append(v[start:stop:step])
                            break
                        else:
                            views.append(v[start::step])
                            newstart = start + len(views[-1]) * step
                            assert newstart >= k
                            if newstart >= stop:
                                break
                            start = newstart - k
                            stop = stop - k
            else:
                start = start - self._len
                stop = stop - self._len

                for v, k in zip(reversed(self._views), reversed(self._views_len)):
                    if -start > k:
                        start += k
                        stop += k
                    else:
                        if -stop <= k + 1:
                            views.append(v[start:stop:step])
                            break
                        else:
                            views.append(v[start::step])
                            newstart = start + len(views[-1]) * step
                            assert -newstart > k
                            if newstart <= stop:
                                break
                            start = newstart + k
                            stop = stop + k

            newview = ChainListView(*views)
            assert len(newview) == len(range_)
            return newview

        else:
            raise TypeError(f"an integer or slice is expected")

    def __iter__(self):
        for i in range(self._len):
            yield self.__getitem__(i)

    @property
    def view(self) -> 'self':
        return self


Element = TypeVar('Element')
Category = TypeVar('Category')
SplitOut = TypeVar('SplitOut', list, Biglist)

def stratified_split(
        x: Sequence[Element], 
        split_frac: Union[float, List[float]], 
        key: Callable[[Element], Category],
        *, 
        min_split_size: int=1, 
        out_cls: SplitOut=Biglist, 
        batch_size: int=None,
        category_sizes: Dict[Category, int]=None) -> List[SplitOut]:
    '''
    `x`: input sequence. If `category_sizes` is given, the `x` will be walked through only once,
        hence it can be any iterable. Otherwise it will be walked through twice,
        hence can't be a generator. Usually a `list` or `Biglist`.
    `split_frac`: fractions of output sequences. If a single value, then split into two parts with
        the first part having this fraction of the original. If a list, then split into
        `len(split_frac)` or `len(split_frac) + 1` parts, depending on whether the values add up to 1.
    `key`: key function by which to split. It takes a single element in `x` and returns
        a hashable value, usually a string. This function determines the *category* of an element.
        The elements are split per category, that is, elements of each category are split
        into multiple parts according to the specified fractions.
        This is what *stratified* refers to.
    `min_split_size`: minimum number of elements in each split of each category.
        For example, suppose `x` contains category `AA` (which is the output of `key` applied
        to elements of this category) that has 7 elements, and `split_frac` is 0.2.
        Then for this category, first split has 1 element and second has 6.
        If `min_split_size` is 2, then category `AA` is dropped entirely.
    `out_cls`: class of the output sequences.
    `batch_size`: batch size if `out_cls` is `Biglist`.

    If the split is required to be random, then the input `x` should have already
    been randomly shuffled.
    '''
    if isinstance(split_frac, list):
        assert all(0.01 <= v < 0.99 for v in split_frac)
        assert sum(split_frac) < 1.0001
        assert split_frac[0] <= 0.99
    else:
        assert 0.0 <= split_frac <= 0.99
        split_frac = [split_frac]

    fractions = [split_frac[0]]
    total = fractions[0]
    for f in split_frac[1:]:
        if total <= 0.99:
            fractions.append(f)
            total += fractions[-1]
    if total <= 0.99:
        fractions.append(1.0 - total)
    assert len(fractions) > 1
    fractions = fractions[:-1]

    n_splits = len(fractions) + 1

    if out_cls is Biglist:
        if batch_size is None:
            if isinstance(x, Biglist):
                batch_size = x.batch_size
        splits = [Biglist(batch_size=batch_size) for _ in range(n_splits)]
    else:
        assert out_cls is list
        splits = [out_cls() for _ in range(n_splits)]

    if category_sizes is None:
        category_sizes = defaultdict(int)
        for xx in x:
            category_sizes[key(xx)] += 1

    if min_split_size < 1:
        min_category_size = 1
    else:
        min_category_size = int(min_split_size / min(min(fractions), 1. - sum(fractions))) + 1

    category_split_sizes = {}
    for cat, n in category_sizes.items():
        if n >= min_category_size:
            category_split_sizes[cat] = [int(n*v) for v in fractions]

    for xx in x:
        k = key(xx)
        sizes = category_split_sizes.get(k)
        if sizes is None:
            continue
        done = False
        for i, n in enumerate(sizes):
            if n > 0:
                splits[i].append(xx)
                sizes[i] -= 1
                done = True
                break
        if not done:
            splits[n_splits - 1].append(xx)

    return splits

