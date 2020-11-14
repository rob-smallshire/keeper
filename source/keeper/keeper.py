import hashlib
import pickle
import sys
import time
from concurrent.futures.thread import ThreadPoolExecutor
from pathlib import Path
import logging

from keeper.streams import WriteableBufferedStream, WriteableStream, StreamMap
from keeper.values import ValueMeta, Value, PendingValue
from .storage import filestorage


logger = logging.getLogger(__name__)


class KeeperClosed(Exception):

    def __init__(self):
        super().__init__(f"{type(Keeper).__name__} has been closed")


class Keeper(object):

    def __init__(self, path, storage_factory=None):
        """Instantiate a Keeper store with a directory path.

        Args:
            path: A location for the keeper store. This will be passes as an
                argument to the storage_factory. The type of the path argument
                can be anything accepted by the storage factory.

            storage_factory: A callable which returns a storage backet. It should
                accept a single argument, which will be the path argument. If
                unspecified or None, the default storage factory FileStorage will
                be used.
        """

        logger.debug("Creating %s with pagh %s", type(self).__name__, path)
        if storage_factory is None:
            storage_factory = filestorage.FileStorage
        self._storage = storage_factory(path)
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._pending_streams = StreamMap()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def add_stream(self, mime=None, encoding=None, **kwargs):
        """Returns an open, writable file-like-object and context manager
        which when closed, commits the data to this keeper. Only then is the
        key accessible through the key property of the returned object.

        mime: The optional MIME type of the data.

        encoding: If encoding is None (the default) the returned file-like-
            object will only accept bytes objects. If the encoding is not None
            only strings will be accepted.
        """
        logger.debug(
            "%s adding stream with MIME type %r and encoding %r",
            type(self).__name__,
            mime,
            encoding
        )
        if not self._storage:
            raise KeeperClosed()
        return WriteableStream(self, mime, encoding, **kwargs)

    def add_buffered_stream(self, mime=None, encoding=None, **kwargs):
        logger.debug(
            "%s adding stream with MIME type %r and encoding %r",
            type(self).__name__,
            mime,
            encoding
        )
        if not self._storage:
            raise KeeperClosed()
        return WriteableBufferedStream(self, mime, encoding, **kwargs)

    def _enqueue_pending(self, stream):
        self._pending_streams.add(stream)
        self._executor.submit(self._persist_pending_stream, stream.key)

    def _persist_pending_stream(self, key):
        try:
            stream = self._pending_streams[key]
            with self._storage.open_temp(encoding=stream.encoding) as tmp:
                tmp.write(stream._file.getvalue())
            self._storage.promote_temp(tmp.name, stream.key)
        except Exception as e:
            logger.error("Exception in _persist_pending_stream %s", e)
        finally:
            self._pending_streams.discard(key)

    def add(self, data, mime=None, encoding=None, **kwargs):
        """Adds data into the store.

        Args:
            data (bytes): The data to be added.
            mime: The MIME type of the data
            encoding: The encoding of the data
        Returns:
            A key for the data
        """
        logger.debug(
            "%s adding data of length %r with MIME type %r and encoding %r",
            type(self).__name__,
            len(data),
            mime,
            encoding
        )
        if not self._storage:
            raise KeeperClosed()

        if isinstance(data, str):
            encoding = encoding or sys.getdefaultencoding()
            if encoding != sys.getdefaultencoding():
                raise ValueError("Strings must use default encoding.")
            data = data.encode()

        if not isinstance(data, bytes):
            raise TypeError("data type must be bytes or str")

        meta = ValueMeta(length=len(data), mime=mime, encoding=encoding, **kwargs)
        serialised_meta = pickle.dumps(meta)

        digester = hashlib.sha1()
        digester.update(data)
        digester.update(serialised_meta)
        key = digester.hexdigest()

        if key in self:
            return key

        with self._storage.open_meta(key, 'w') as meta_file:
            meta_file.write(serialised_meta)

        with self._storage.open_data(key, 'w') as data_file:
            data_file.write(data)

        logger.debug(
            "%s added data of length %r with key %r",
            type(self).__name__,
            len(data),
            key
        )
        return key

    def __contains__(self, key):
        logging.debug("%s checking for membership of key %r", type(self).__name__, key)
        if key in self._pending_streams:
            return True
        if not self._storage:
            raise KeeperClosed()
        try:
            with self._storage.open_meta(key):
                contained = True
        except (ValueError, FileNotFoundError):
            contained = False
        logging.debug(
            "%s %s key %r",
            type(self).__name__,
            "contains" if contained else "does not contain",
            key
        )
        return contained

    def __iter__(self):
        if not self._storage:
            raise KeeperClosed()

        pending_keys = self._pending_streams.keys()

        yield from pending_keys

        for key in self._storage:
            if key not in pending_keys:
                yield key

    def __getitem__(self, key):
        """Retrieve data by its key.

        Args:
            key: A key obtained from add().

        Returns:
            A Value object representing the data associated with key.

        Raises:
            KeyError: If the key is unknown.
        """
        logger.debug("%s getting item with key %r", type(self).__name__, key)
        if key in self._pending_streams:
            return PendingValue(self, key)
        if not self._storage:
            raise KeeperClosed()
        try:
            return Value(self, key)
        except KeyError:
            logger.debug("%s has not item with key %r", type(self).__name__, key)
            raise

    def __delitem__(self, key):
        """Remove an item by its key"""
        logger.debug("%s removing item with key %r", type(self).__name__, key)
        found = False
        if key in self._pending_streams:
            self._pending_streams.discard(key)
            found = True
        if not self._storage:
            raise KeeperClosed()
        if key in self:
            self._storage.remove(key)
            found = True
        if not found:
            raise KeyError(key)

    def __len__(self):
        if not self._storage:
            raise KeeperClosed()
        return sum(1 for _ in self)

    def close(self):
        logger.debug("%s closing", type(self).__name__)
        logger.debug("%s draining %d pending streams", type(self).__name__, len(self._pending_streams))
        while len(self._pending_streams) > 0:
            time.sleep(0.001)
        logger.debug("%s drained pending streams", type(self).__name__)
        self._executor.shutdown()
        self._storage.close()
        self._storage = None

