import io

from sklearn.externals import joblib


def dump_bytes(x, compress: int = 9) -> bytes:
    """
    Serialize Python object (e.g. fitted model) `x` into a binary blob.
    """
    o = io.BytesIO()
    joblib.dump(x, o, compress=compress)
    return o.getvalue()


def load_bytes(b: bytes):
    """
    Inverse of `dump_bytes`.
    """
    return joblib.load(io.BytesIO(b))


def dump_file(x, filename: str, overwrite: bool = True,
              compress: int = 9) -> None:
    """
    Persist Python object (e.g. fitted model) `x` into disk file `filename`.
    """
    with open(filename, 'wb' if overwrite else 'xb') as f:
        f.write(dump_bytes(x, compress=compress))


def load_file(filename: str):
    """
    Inverse of `dump`.
    """
    with open(filename, 'rb') as f:
        z = f.read()
    return load_bytes(z)
