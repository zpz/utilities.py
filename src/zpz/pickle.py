import os
import os.path
import pickle


def pickle_load(path, *path_elements):
    return pickle.load(open(os.path.join(path, *path_elements), 'rb'))


def pickle_dump(x, path, *path_elements):
    ff = os.path.join(path, *path_elements)
    dirname = os.path.dirname(os.path.abspath(ff))
    if not os.path.isdir(dirname):
        os.makedirs(dirname)
    return pickle.dump(x, open(ff, 'wb'))