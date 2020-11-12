import contextlib
import logging
import os
import shutil
import uuid

import atomicwrites
from atomicwrites import move_atomic

META_EXTENSION = '.pickle'

logger = logging.getLogger(__name__)


class FileStorage:

    def __init__(self, dirpath, levels=None):
        """
        Args:
            dirpath: The path to a directory in which data will be stored. If dirpath does
                not exist it will be created.

        Raises:
            FileExistsError: If parts of the structure within dirpath already exist, but
                are not directories as expected.
        """
        # TODO: This class is very much based around using strings as paths rather than
        #       path objects. It needs refactoring to Paths.
        dirpath = str(dirpath)

        self._levels = levels if levels is not None else 4

        logger.debug(
            "Creating %s with dirpath %s and %r levels",
            type(self).__name__,
            dirpath,
            self._levels
        )

        self._ensure_directory_exists(dirpath)

        self._temp_root_path = os.path.join(dirpath, 'temp')
        self._meta_root_path = os.path.join(dirpath, 'meta')
        self._data_root_path = os.path.join(dirpath, 'data')

        shutil.rmtree(self._temp_root_path, ignore_errors=True)

        self._ensure_directory_exists(self._temp_root_path)
        self._ensure_directory_exists(self._meta_root_path)
        self._ensure_directory_exists(self._data_root_path)

        self._directory_path = dirpath

    @staticmethod
    def _ensure_directory_exists(dirpath):
        """Ensure that a directory exists.

        Args:
            dirpath: The path to the directory.

        Raises:
            FileExistsError: If the path already exists, but is not a directory.
        """
        logger.debug("Ensuring directory %s exists", dirpath)
        try:
            os.mkdir(dirpath)
        except FileExistsError:
            if not os.path.isdir(dirpath):
                raise FileExistsError(f"{dirpath} already exists but is a file, not a directory")
            logger.debug("Directory %s already exists", dirpath)
        else:
            logger.debug("Directory %s created", dirpath)

    @property
    def root_path(self):
        return self._directory_path

    def _relative_key_path(self, key):
        if len(key) < self._levels:
            raise ValueError("Key is too short")

        path_components = list(key[:self._levels])
        path_components.append(key[self._levels:])
        return  os.path.join(*path_components)

    #noinspection PyTypeChecker
    def __iter__(self):
        for dirpath, dirnames, filenames in os.walk(self._meta_root_path):
            prefix_dirpath = dirpath[len(self._meta_root_path):]
            prefix = ''.join(prefix_dirpath.split(os.sep))
            for filename in filenames:
                suffix = filename[:-len(META_EXTENSION)]
                key = prefix + suffix
                yield key

    def _meta_path(self, key):
        return os.path.join(
            self._meta_root_path,
            self._relative_key_path(key) + META_EXTENSION
        )

    @contextlib.contextmanager
    def open_temp(self, mode="w", encoding=None):
        temp_filename = str(uuid.uuid4())
        temp_path = os.path.join(self._temp_root_path, temp_filename)
        if encoding is None and 'b' not in mode:
            mode += 'b'
        logger.debug(
            "%s opening temporary file %r with mode %r and encoding %r",
            type(self).__name__,
            temp_path,
            mode,
            encoding
        )
        with open(temp_path, mode=mode, encoding=encoding) as temp_file:
            logger.debug(
                "%s opened temporary file with path %r",
                type(self).__name__,
                temp_file.name
            )
            yield temp_file
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

    def promote_temp(self, temporary_filepath, key):
        """
        Args:
            temporary_filepath: The path to the temporary file.
            key: The key under which the contents of the temporary file
                should be stored.

        Raises:
            FileNotFoundError: If temporary_filepath does not exist.
        """
        logger.debug(
            "%s promoting temporary file with %s and key %r",
            type(self).__name__,
            temporary_filepath,
            key,
        )
        data_path = self.path(key)
        dir_path = os.path.dirname(data_path)
        os.makedirs(dir_path, exist_ok=True)
        move_atomic(temporary_filepath, data_path)
        logger.debug(
            "%s promoted temporary file %s to permanent by moving %s",
            type(self).__name__,
            temporary_filepath,
            data_path
        )

    def remove_temp(self, temporary_filepath):
        logger.debug(
            "%s removing temporary file %s",
            type(self).__name__,
            temporary_filepath
        )
        try:
            os.remove(temporary_filepath)
        except FileNotFoundError:
            logger.debug(
                "%s could not find %r to remove it",
                type(self).__name__,
                temporary_filepath
            )

    @contextlib.contextmanager
    def open_meta(self, key, mode='r'):
        meta_filepath = self._meta_path(key)
        dir_path = os.path.dirname(meta_filepath)
        os.makedirs(dir_path, exist_ok=True)
        if 'b' not in mode:
            mode += 'b'
        with open(meta_filepath, mode) as meta_file:
            yield meta_file
            if not meta_file.closed and ('w' in mode):
                meta_file.flush()
                atomicwrites._proper_fsync(meta_file.fileno())
                self._sync_parent_directory(meta_filepath)

    def path(self, key):
        return os.path.join(
            self._data_root_path,
            self._relative_key_path(key)
        )

    @contextlib.contextmanager
    def open_data(self, key, mode='r', encoding=None):
        logger.debug(
            "%s opening data file for key %r with mode %r and encoding %r",
            type(self).__name__,
            key,
            mode,
            encoding,
        )
        # TODO: We should write the file to a temporary path then atomically move it into place
        data_filepath = self.path(key)
        dir_path = os.path.dirname(data_filepath)
        os.makedirs(dir_path, exist_ok=True)
        if encoding is None and 'b' not in mode:
            mode += 'b'
        with open(data_filepath, mode=mode, encoding=encoding) as datafile:
            logger.debug(
                "%s opened data file with path %r",
                type(self).__name__,
                data_filepath
            )
            yield datafile
            if not datafile.closed and ('w' in mode):
                datafile.flush()
                atomicwrites._proper_fsync(datafile.fileno())
                self._sync_parent_directory(data_filepath)

    # TODO: At some point we should make the files read only

    def remove(self, key):
        logger.debug("%s removing key %r", type(self).__name__, key)
        meta_filepath = self._meta_path(key)
        os.remove(meta_filepath)
        meta_parent_dirpath = os.path.abspath(os.path.join(meta_filepath, os.pardir))
        atomicwrites._sync_directory(meta_parent_dirpath)
        logger.debug("%s removed %r", type(self).__name__, meta_filepath)
        data_filepath = self.path(key)
        os.remove(data_filepath)
        self._sync_parent_directory(data_filepath)
        logger.debug("%s removed %r", type(self).__name__, data_filepath)

    def _sync_parent_directory(self, path):
        logger.debug("Syncing parent directory of %s", path)
        data_parent_dirpath = os.path.abspath(os.path.join(path, os.pardir))
        atomicwrites._sync_directory(data_parent_dirpath)

    def close(self):
        self._directory_path = None
        logger.debug("%s closed", type(self).__name__)

