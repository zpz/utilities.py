import logging
import os
import shutil
import string
from datetime import datetime
from os.path import join as path_join
from typing import List, Type

from ..functools import classproperty

from ._dropbox import Dropbox
from ._dropbox import write_timestamp, read_timestamp, has_timestamp, TIMESTAMP_FILE

logger = logging.getLogger(__name__)


def make_version() -> str:
    return datetime.utcnow().strftime('%Y%m%d-%H%M%S')


ALNUM = string.ascii_letters + string.digits


def is_version(version: str) -> bool:
    if not version:
        return False
    return (version[0] in ALNUM) and all(v in ALNUM + '._-' for v in version)


class VersionedUploadableMixin:
    def __init__(self, version: str = None, version_tag: str = None):
        if version is None:
            self.version = make_version()
            if version_tag:
                version_tag = version_tag.strip(' ').strip('-').strip('_')
                if version_tag:
                    assert is_version(version_tag)
                    self.version = self.version + '-' + version_tag
            return
        assert version_tag is None

    def load(self):
        if self.is_local_version():
            self.load_local()
        else:
            if self.is_remote_version():
                self.load_remote()

    def load_local(self):
        pass

    def load_remote(self):
        pass

    @classproperty
    def local_cls_dir(cls):
        z = os.environ.get('VERSIONED_UPLOADABLE_PATH')
        if not z:
            z = path_join(
                os.environ.get('HOME', '/tmp'),
                'versioned_uploadables'
            )
        return path_join(z, cls.__name__)

    @classproperty
    def remote_cls_dir(cls):
        return f'/{cls.__name__}/'

    @property
    def local_dir(self):
        return path_join(self.local_cls_dir, 'versions', self.version)

    @property
    def remote_dir(self):
        return path_join(self.remote_cls_dir, 'versions', self.version)

    @classmethod
    def has_local_version(cls, version):
        return has_timestamp(
            path_join(cls.local_cls_dir, 'versions', version))

    @classmethod
    def has_remote_version(cls, version):
        raise NotImplementedError

    def is_local_version(self):
        return self.has_local_version(self.version)

    def is_remote_version(self):
        return self.has_remote_version(self.version)

    @classmethod
    def get_local_versions(cls) -> List[str]:
        path = path_join(cls.local_cls_dir, 'versions')
        if not os.path.isdir(path):
            return []
        dd = [
            d
            for d in os.listdir(path)
            if (
                os.path.isdir(path_join(path, d))
                and has_timestamp(path_join(path, d))
            )
        ]
        return dd

    @classmethod
    def get_remote_versions(cls) -> List[str]:
        raise NotImplementedError

    @classmethod
    def rm_local_version(cls, version, ignore_errors=True):
        logger.info("deleting local version %s of %s",
                    version, cls.__name__)
        path = path_join(cls.local_cls_dir, 'versions', version)
        shutil.rmtree(path, ignore_errors=ignore_errors)

    @classmethod
    def rm_remote_version(cls, version):
        logger.info("deleting remote version %s of %s",
                    version, cls.__name__)

    def destroy(self):
        self.rm_local_version(self.version)

    def destroy_remote(self):
        self.rm_remote_version(self.version)

    def upload_dir(self, rel_dir: str, **kwargs):
        raise NotImplementedError

    def download_dir(self, rel_dir: str, **kwargs):
        raise NotImplementedError

    def upload(self, verbose=True):
        if not self.is_local_version():
            logger.warning(
                "Local object %s of version %s is either empty or in-complete; uploading is aborted.",
                self.__class__.__name__,
                self.version,
            )
            return
        self.upload_dir('', verbose=verbose, clear_remote_dir=True)

    def download(self, verbose=True):
        if not self.is_remote_version():
            logger.warning(
                "Remote object %s of version %s is either empty or in-complete; udownloading is aborted.",
                self.__class__.__name__,
                self.version,
            )
            return
        n = self.download_dir('', verbose=verbose)
        if n:
            self.load_local()

    def __repr__(self):
        return f'{self.__class__.__name__}(version="{self.version}")'

    def __str__(self):
        return self.__repr__()
