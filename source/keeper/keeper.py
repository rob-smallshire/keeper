import threading
from collections import Mapping
import logging

from keeper.streams import WriteableBinaryStream
from keeper.values import Value

DEFAULT_ENCODING = "utf-8"

logger = logging.getLogger(__name__)


class KeeperClosed(ValueError):

    def __init__(self):
        super().__init__(f"{type(Keeper).__name__} has been closed")


class Keeper(Mapping):

    def __init__(self, storage):
        self._lock = threading.RLock()
        self._storage = storage

    @property
    def storage(self):
        if self.closed:
            raise KeeperClosed()
        with self._lock:
            return self._storage

    @property
    def closed(self):
        with self._lock:
            return self._storage is None

    def close(self):
        with self._lock:
            self._storage = None
        logger.debug("%s closing", type(self).__name__)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def add_stream(self, mime=None, encoding=None, **meta):
        """Returns an open, writable file-like-object and context manager
        which when closed, commits the data to this keeper. Only then is the
        key accessible through the key property of the returned object.

        mime: The optional MIME type of the data.

        encoding: If encoding is None (the default) the returned file-like-
            object will only accept bytes objects. If the encoding is not None
            only strings will be accepted.

        **meta: Meta data about the stream which will be stored along with the
            stream.
        """
        logger.debug(
            "%s adding stream with MIME type %r and encoding %r",
            type(self).__name__,
            mime,
            encoding
        )
        if self.closed:
            raise KeeperClosed()
        return WriteableBinaryStream(self, mime, encoding, **meta)

    def add(self, data, mime=None, encoding=None, **meta):
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

        if isinstance(data, str):
            encoding = encoding or DEFAULT_ENCODING
            data = data.encode(encoding)

        if not isinstance(data, bytes):
            raise TypeError("data type must be bytes or str")

        with self.add_stream(mime, encoding=encoding, **meta) as stream:
            stream.write(data)
        return stream.key

    def __contains__(self, key):
        logging.debug("%s checking for membership of key %r", type(self).__name__, key)
        try:
            with self.storage.openin_meta(key):
                contained = True
        except KeyError:
            contained = False
        logging.debug(
            "%s %s key %r",
            type(self).__name__,
            "contains" if contained else "does not contain",
            key
        )
        return contained

    def __iter__(self):
        """Obtain an iterator over all keys
        """
        yield from self.storage.keys()

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
        if self.closed:
            raise KeeperClosed()
        try:
            return Value(self, key)
        except KeyError:
            logger.debug("%s has not item with key %r", type(self).__name__, key)
            raise

    def __delitem__(self, key):
        """Remove an item by its key"""
        logger.debug("%s removing item with key %r", type(self).__name__, key)
        if key in self:
            return self.storage.discard(key)
        raise KeyError(key)

    def __len__(self):
        return sum(1 for _ in self)

    def __repr__(self):
        return f"{type(self).__name__}(storage={self._storage})"

