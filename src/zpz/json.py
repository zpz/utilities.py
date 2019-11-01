import json
import os
import os.path

def json_load(path, *path_elements):
    return json.load(open(os.path.join(path, *path_elements), 'r'))

def json_dump(x, path, *path_elements):
    ff = os.path.join(path, *path_elements)
    dirname = os.path.dirname(os.path.abspath(ff))
    if not os.path.isdir(dirname):
        os.makedirs(dirname)
    return json.dump(x, open(ff, 'w'))