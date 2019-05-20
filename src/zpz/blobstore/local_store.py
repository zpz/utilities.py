import os
import os.path
from pathlib import Path
import shutil
from typing import List
from ._store import Store
from ..exceptions import ZpzError

class LocalStore(Store):
    def __init__(self, home: str=None):
        if os.name != 'posix':
            import sys
            raise ZpzError(f"`{self.__class__.__name__}` is not supported on your platform: `{sys.platform}`")
        if not home:
            home = str(Path.home())
        super().__init__(home)

    def _exists_file(self, abs_path: str) -> bool:
        return Path(self.realpath(abs_path)).is_file()

    def _ls_dir(self, abs_path: str, recursive: bool=False) -> List[str]:
        dd = Path(self.realpath(abs_path))
        if not dd.is_dir():
            return []
        if recursive:
            zz = [str(f.relative_to(abs_path)) for f in dd.glob('**') if f.is_file]
        else:
            zz = [str(f.relative_to(abs_path)) + ('/' if f.is_dir() else '') for f in dd.iterdir()]
        return zz

    def _rm(self, abs_path: str) -> None:
        Path(self.realpath(abs_path)).unlink()

    def _stat(self, abs_path: str):
        return Path(self.realpath(abs_path)).stat()

    def _cp(self, abs_source_file: str, abs_dest_file: str) -> None:
        shutil.copyfile(self.realpath(abs_source_file), self.realpath(abs_dest_file))

    def _mv(self, abs_source_file: str, abs_dest_file: str) -> None:
        shutil.move(self.realpath(abs_source_file), self.realpath(abs_dest_file))

    def _put(self, local_abs_file: str, abs_file: str) -> None:
        shutil.copyfile(local_abs_file, self.realpath(abs_file))

    def _get(self, abs_file: str, local_abs_file: str) -> None:
        shutil.copyfile(self.realpath(abs_file), local_abs_file)

    def open(self, file_path: str, mode: str='rt'):
        return open(self.realpath(file_path), mode)
        
