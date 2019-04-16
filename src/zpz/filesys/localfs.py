import os
import os.path
from pathlib import Path
from typing import List
from ._fs import FS
from ..exceptions import ZpzError

class LocalFS(FS):
    def __init__(self):
        if os.name != 'posix':
            import sys
            raise ZpzError(f"`{self.__class__.__name__}` is not supported on your platform: `{sys.platform}`")

    @property
    def HOME(self):
        return str(Path.home()) + '/'

    def _exists_file(self, full_path: str) -> bool:
        return Path(full_path).is_file()

    def _exists_dir(self, full_path: str) -> bool:
        return Path(full_path).is_dir()

    def _ls_dir(self, full_path: str, recursive: bool=False) -> List[str]:
        dd = Path(full_path)
        if recursive:
            z = dd.glob('*')
        else:
            z = dd.glob('**')
        zz = [str(v.relative_to(dd)) + '/' if v.is_dir() 
                else str(v.relative_to(dd)) 
                for v in z]
        return sorted(zz)

    def _rm(self, full_path: str) -> None:
        Path(full_path).unlink()

    def _put_text(self, text: str, full_path: str) -> None:
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        open(full_path, 'w').write(text)

    def _get_text(self, full_path: str) -> str:
        return open(full_path).read()
