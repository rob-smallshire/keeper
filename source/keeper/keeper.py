import hashlib
import pickle
import sys
from collections import Mapping
import logging

from keeper.streams import WriteableBinaryStream
from keeper.values import ValueMeta, Value


logger = logging.getLogger(__name__)


class KeeperClosed(Exception):

    def __init__(self):
        super().__init__(f"{type(Keeper).__name__} has been closed")


class Keeper(Mapping):

    def __init__(self, storage):
        self._storage = storage

    @property
    def storage(self):
        return self._storage

    @property
    def closed(self):
        return self._storage is None

    def close(self):
        self._storage = None
        logger.debug("%s closing", type(self).__name__)

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
        if self.closed:
            raise KeeperClosed()
        return WriteableBinaryStream(self, mime, encoding, **kwargs)

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
        if self.closed:
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

        with self._storage.openout_data(key) as data_file:
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

        yield from self._storage

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
        if not self._storage:
            raise KeeperClosed()
        if key in self:
            return self._storage.remove(key)
        raise KeyError(key)

    def __len__(self):
        if self.closed:
            raise KeeperClosed()
        return sum(1 for _ in self)


