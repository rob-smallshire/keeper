import contextlib
import logging
import os
import shutil
import uuid
from pathlib import Path

import atomicwrites
from atomicwrites import move_atomic

from keeper.storage.storage import Storage
from keeper.storage.streams import WriteOnlyStream, ReadOnlyStream

META_EXTENSION = '.pickle'

logger = logging.getLogger(__name__)


class FileStorage(Storage):

    def __init__(self, root_dirpath, levels=None):
        """
        Args:
            root_dirpath: The path to a directory in which data will be stored. If dirpath does
                not exist it will be created.

            levels: The number of levels of key to split into directories.

        Raises:
            FileExistsError: If parts of the structure within dirpath already exist, but
                are not directories as expected.
        """
        # TODO: This class is very much based around using strings as paths rather than
        #       path objects. It needs refactoring to Paths.
        self._root_dirpath = Path(root_dirpath)

        self._levels = levels if levels is not None else 4

        logger.debug(
            "Creating %s with dirpath %s and %r levels",
            type(self).__name__,
            self._root_dirpath,
            self._levels
        )

        self._root_dirpath.mkdir(parents=True, exist_ok=True)

        self._temp_root_path = self._root_dirpath / 'temp'
        self._meta_root_path = self._root_dirpath / 'meta'
        self._data_root_path = self._root_dirpath / 'data'

        shutil.rmtree(self._temp_root_path, ignore_errors=True)

        self._temp_root_path.mkdir(parents=True, exist_ok=True)
        self._meta_root_path.mkdir(parents=True, exist_ok=True)
        self._data_root_path.mkdir(parents=True, exist_ok=True)

    @property
    def root_path(self):
        return self._root_dirpath

    def _relative_key_path(self, key):
        if len(key) < self._levels:
            raise ValueError("Key is too short")

        path_components = list(key[:self._levels])
        path_components.append(key[self._levels:])
        return Path(*path_components)

    def keys(self):
        for dirpath, dirnames, filenames in os.walk(self._meta_root_path):
            prefix_dirpath = dirpath[len(str(self._meta_root_path)):]
            prefix = ''.join(prefix_dirpath.split(os.sep))
            for filename in filenames:
                suffix = filename[:-len(META_EXTENSION)]
                key = prefix + suffix
                yield key

    def _meta_path(self, key) -> Path:
        return self._meta_root_path / self._relative_key_path(key).with_suffix(META_EXTENSION)

    @contextlib.contextmanager
    def openout_temp(self):
        handle = str(uuid.uuid4())
        temp_path = self._temp_path(handle)
        logger.debug(
            "%s creating temporary file %r",
            type(self).__name__,
            temp_path,
        )
        with open(temp_path, mode="wb") as temp_file:
            logger.debug(
                "%s opened temporary file with path %r",
                type(self).__name__,
                temp_file.name
            )
            with WriteOnlyStream(temp_file, name=handle) as stream:
                yield stream
            if not temp_file.closed:
                temp_file.flush()
                logger.debug(
                    "%s fsynced temporary file with path %r",
                    type(self).__name__,
                    temp_file.name
                )
                atomicwrites._proper_fsync(temp_file.fileno())
        logger.debug(
            "%s closed temporary file with path %r",
            type(self).__name__,
            temp_file.name
        )
        self._sync_parent_directory(temp_path)

    @contextlib.contextmanager
    def openin_temp(self, handle):
        temp_path = self._temp_path(handle)
        logger.debug(
            "%s opening temporary file %r for read",
            type(self).__name__,
            temp_path,
        )
        with open(temp_path, mode="rb") as temp_file:
            with ReadOnlyStream(temp_file, name=handle) as stream:
                yield stream

        logger.debug(
            "%s closed temporary file with path %r",
            type(self).__name__,
            temp_file.name
        )

    def _temp_path(self, handle):
        return self._temp_root_path / handle

    def promote_temp(self, handle, key):
        """
        Args:
            handle: The name of the temporary.
            key: The key under which the contents of the temporary file
                should be stored.

        Raises:
            ValueError: If name does not exist.
        """
        logger.debug(
            "%s promoting temporary file with name %s and key %r",
            type(self).__name__,
            handle,
            key,
        )
        temp_path = self._temp_path(handle)
        data_path = self._data_path(key)
        data_path.parent.mkdir(parents=True, exist_ok=True)
        move_atomic(temp_path, data_path)
        logger.debug(
            "%s promoted temporary file %s to permanent by moving %s",
            type(self).__name__,
            temp_path,
            data_path
        )

    def remove_temp(self, handle):
        temp_path = self._temp_path(handle)
        logger.debug(
            "%s removing temporary file %s",
            type(self).__name__,
            handle
        )
        try:
            temp_path.unlink()
        except FileNotFoundError:
            logger.debug(
                "%s could not find %r to remove it",
                type(self).__name__,
                handle
            )
            raise ValueError(f"Could not remove temp with handle {handle!r}")

    @contextlib.contextmanager
    def openout_meta(self, key):
        meta_filepath = self._meta_path(key)
        meta_filepath.parent.mkdir(parents=True, exist_ok=True)
        with atomicwrites.atomic_write(meta_filepath, mode="wb") as meta_file:
            with WriteOnlyStream(meta_file, name=key) as stream:
                yield stream

    @contextlib.contextmanager
    def openin_meta(self, key):
        meta_filepath = self._meta_path(key)
        with open(meta_filepath, "rb") as meta_file:
            with ReadOnlyStream(meta_file, name=key) as stream:
                yield stream

    def _data_path(self, key) -> Path:
        return self._data_root_path / self._relative_key_path(key)

    @contextlib.contextmanager
    def openout_data(self, key):
        logger.debug(
            "%s opening write-only data file for key %r",
            type(self).__name__,
            key,
        )
        data_filepath = self._data_path(key)
        data_filepath.parent.mkdir(parents=True, exist_ok=True)
        with atomicwrites.atomic_write(data_filepath, mode="wb") as datafile:
            yield datafile

    @contextlib.contextmanager
    def openin_data(self, key):
        logger.debug(
            "%s opening read-only data file for key %r",
            type(self).__name__,
            key,
        )
        data_filepath = self._data_path(key)
        with open(data_filepath, mode="rb") as datafile:
            yield datafile

    def remove(self, key):
        logger.debug("%s removing key %r", type(self).__name__, key)
        meta_filepath = self._meta_path(key)
        meta_filepath.unlink(missing_ok=True)
        self._sync_parent_directory(meta_filepath)

        data_filepath = self._data_path(key)
        data_filepath.unlink(missing_ok=True)
        self._sync_parent_directory(data_filepath)
        logger.debug("%s removed %r", type(self).__name__, data_filepath)

    def _sync_parent_directory(self, path: Path):
        logger.debug("Syncing parent directory of %s", path)
        atomicwrites._sync_directory(path.parent)

    def close(self):
        self._root_dirpath = None
        logger.debug("%s closed", type(self).__name__)

    @property
    def closed(self):
        return self._root_dirpath is None

    @property
    def root_dirpath(self):
        return self._root_dirpath

    def __repr__(self):
        return f"{type(self).__name__}(root_dirpath={self.root_dirpath}, levels={self._levels})"
