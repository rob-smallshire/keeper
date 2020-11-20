import contextlib
import logging
import threading
import uuid
from concurrent.futures.thread import ThreadPoolExecutor
from io import BytesIO

from keeper.storage.storage import Storage
from keeper.storage.streams import WriteOnlyStream, ReadOnlyStream

logger = logging.getLogger(__name__)


class WriteCacheStorage(Storage):

    def __init__(self, storage: Storage):
        if storage.closed:
            raise ValueError(f"Underlying storage is {storage} is closed")
        self._storage = storage
        self._temp_buffer_lock = threading.RLock()
        self._temp_data = {}
        self._data_lock = threading.RLock()
        self._data = {}
        self._executor = ThreadPoolExecutor()

    def close(self):
        self._executor.shutdown()
        self._storage = None

    @property
    def closed(self):
        return self._storage is None

    @property
    def storage(self):
        if self.closed:
            raise ValueError(f"Operation on closed storage {self}")
        return self._storage

    def keys(self):
        """An iterator over all keys."""
        with self._data_lock:
            pending_keys = set(self._data.keys())
        yield from pending_keys

        for key in self.storage.keys():
            if key not in pending_keys:
                yield key

    @contextlib.contextmanager
    def openin_meta(self, key):
        # Metadata is not cached
        with self._storage.openin_meta(key) as meta_file:
            yield meta_file

    @contextlib.contextmanager
    def openout_meta(self, key):
        # Metadata is not cached
        with self._storage.openout_meta(key) as meta_file:
            yield meta_file

    @contextlib.contextmanager
    def openout_data(self, key):
        """
        Raises:
            KeyError: If a buffer with the requested key could not be found.
        """
        logger.debug(
            "%s opening write-only data file for key %r",
            type(self).__name__,
            key,
        )
        with BytesIO() as buffer:
            with WriteOnlyStream(buffer, name=key) as stream:
                yield stream
            with self._data_lock:
                self._data[key] = buffer.getvalue()
        self._executor.submit(self._write_buffer, key)

    def _write_buffer(self, key):
        with self._data_lock:
            buffer = self._data[key]
        with self.storage.openout_data(key) as out:
            out.write(buffer)
        with self._data_lock:
            del self._data[key]

    @contextlib.contextmanager
    def openin_data(self, key):
        try:
            with self._data_lock:
                data = self._data[key]
            with BytesIO(data) as buffer:
                with ReadOnlyStream(buffer, name=key) as stream:
                    yield stream
        except KeyError:
            with self.storage.openin_data(key) as stream:
                yield stream

    @contextlib.contextmanager
    def openout_temp(self):
        handle = str(uuid.uuid4())
        with BytesIO() as buffer:
            with WriteOnlyStream(buffer, name=handle) as stream:
                yield stream
            data = buffer.getvalue()
        with self._temp_buffer_lock:
            self._temp_data[handle] = data

    @contextlib.contextmanager
    def openin_temp(self, handle):
        with self._temp_buffer_lock:
            data = self._temp_data[handle]
        with BytesIO(data) as buffer:
            with ReadOnlyStream(buffer, name=handle) as stream:
                yield stream

    def promote_temp(self, name, key):
        self._executor.submit(self._promote_temp_buffer, name, key)

    def _promote_temp_buffer(self, name, key):
        with self._temp_buffer_lock:
            data = self._temp_data[name]
        with self._data_lock:
            self._data[key] = data
        with self.storage.openout_temp() as underlying_temp:
            underlying_temp.write(data)
        self.storage.promote_temp(underlying_temp.name, key)
        with self._temp_buffer_lock:
            del self._temp_data[name]
        with self._data_lock:
            del self._data[key]

    def remove_temp(self, name):
        with self._temp_buffer_lock:
            del self._temp_data[name]

    def discard(self, key):
        with self._data_lock:
            try:
                del self._data[key]
            except KeyError:
                pass
        self.storage.discard(key)

    def __repr__(self):
        return f"{type(self).__name__}(storage={self.storage})"


