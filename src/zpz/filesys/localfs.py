from pathlib import Path
from ._fs import FS


class LocalFS(FS):
    def _exists_file(self, full_path: str) -> bool:
        return Path(full_path).is_file()

    def _exists_dir(self, full_path: str) -> bool:
        return Path(full_path).is_dir()

    def _ls_dir(self, full_path: str, recursive: bool=False) -> List[str]:
        dd = Path(full_path)
        if not dd.is_dir():
            return []
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
        open(full_path, 'w').write(text)

    def _get_text(self, full_path: str) -> str:
        return open(full_path).read()
