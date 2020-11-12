import contextlib
import hashlib
import os
import pickle
import sys
import threading
import time
from concurrent.futures.thread import ThreadPoolExecutor
from io import BytesIO, StringIO
from pathlib import Path
import logging

from .storage import filestorage


logger = logging.getLogger(__name__)


class ValueMeta:
    """
    Immutable meta data for a value.
    """
    def __init__(self, length, mime=None, encoding=None, **kwargs):
        self._keywords = {'length': length,
                          'mime': mime,
                          'encoding': encoding,
                          }
        self._keywords.update(kwargs)

    def __getattr__(self, item):
        if item == "_keywords":
            raise AttributeError
        try:
            return self._keywords[item]
        except KeyError:
            raise AttributeError

    def __iter__(self):
        yield from self._keywords.keys()

    def __contains__(self, item):
        return item in self._keywords


class Value:
    """Access to a value and its metadata.
    """

    def __init__(self, keeper, key):
        self._keeper = keeper
        self._key = key

        try:
            with self._keeper._storage.open_meta(self._key, 'rb') as meta_file:
                self._meta = pickle.load(meta_file)
        except FileNotFoundError:
            raise KeyError(key)

    @property
    def meta(self) -> ValueMeta:
        """The metadata associated with this value.

        Returns:
            The ValueMeta object corresponding to this value.
        """
        return self._meta

    def as_bytes(self):
        """Access the value as a bytes object.
        """
        with self._keeper._storage.open_data(self._key, 'rb') as data_file:
            data = data_file.read()

        return data

    def as_file(self):
        """Access the data as a read-only file-like object.
        """
        mode = 'rb' if self.meta.encoding is None else 'r'
        return self._keeper._storage.open_data(self._key, mode=mode,
                                               encoding=self.meta.encoding)

    def as_string(self):
        """Return the data as a string.

        The string is constructed from the underlying bytes data using the
        encoding in self.meta.encoding or the default string encoding if the
        former is None.
        """
        with self._keeper._storage.open_data(self._key, mode='r',
                                    encoding=self.meta.encoding) as data_file:
            s = data_file.read()
        return s

    def __str__(self):
        """Return the data as a string.

       The string is constructed from the underlying bytes data using the
       encoding in self.meta.encoding or the default string encoding if the
       former is None.
       """
        return self.as_string()

    def __len__(self):
        """The length of the data in bytes (NOT characters).
        """
        return self._meta.length

    @property
    def path(self):
        """Obtains a filesystem path to the resource.

        Returns:
            A path to the resource as a string or None.

        Warning:
            The file MUST NOT be modified through this path.
        """
        return self._keeper._storage.path(self._key)


class PendingValue:
    """Access to a value and its metadata.
    """

    def __init__(self, keeper, key):
        self._keeper = keeper
        self._key = key

        try:
            with self._keeper._storage.open_meta(self._key, 'rb') as meta_file:
                self._meta = pickle.load(meta_file)
        except FileNotFoundError:
            raise KeyError(key)
        try:
            with self._keeper._pending_streams_lock:
                self._buffer = keeper._pending_streams[key]
        except KeyError:
            raise KeyError(key)

    @property
    def meta(self) -> ValueMeta:
        """The metadata associated with this value.

        Returns:
            The ValueMeta object corresponding to this value.
        """
        return self._meta

    def as_bytes(self):
        """Access the value as a bytes object.
        """
        return self._buffer.getvalue()

    def as_file(self):
        """Access the data as a read-only file-like object.
        """
        mode = 'rb' if self.meta.encoding is None else 'r'
        # TODO: Read-only wrapper for BytesIO
        return self._buffer

    def as_string(self):
        """Return the data as a string.

        The string is constructed from the underlying bytes data using the
        encoding in self.meta.encoding or the default string encoding if the
        former is None.
        """
        return self._buffer.decode(encoding=self.meta.encoding)

    def __str__(self):
        """Return the data as a string.

       The string is constructed from the underlying bytes data using the
       encoding in self.meta.encoding or the default string encoding if the
       former is None.
       """
        return self.as_string()

    def __len__(self):
        """The length of the data in bytes (NOT characters).
        """
        return self._meta.length

    @property
    def path(self):
        """Obtains a filesystem path to the resource.

        Returns:
            A path to the resource as a string or None.

        Warning:
            The file MUST NOT be modified through this path.
        """
        return "<memory>"


class WriteableBufferedStream:

    def __init__(self, keeper, mime, encoding, **kwargs):
        logger.debug(
            "Creating %s with MIME type %r and encoding %r",
            type(self).__name__,
            mime,
            encoding
        )
        self._keeper = keeper
        self._encoding = encoding
        self._file = self._keeper._storage.open_temp('w+', encoding)
        self._mime = mime
        self._keywords = kwargs
        self._key = None
        self._file = BytesIO() if encoding is None else StringIO()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
        return False

    @property
    def mime(self):
        return self._mime

    @property
    def encoding(self):
        return self._encoding

    @property
    def key(self):
        return self._key

    def write(self, data):
        self._file.write(data)

    @property
    def closed(self):
        return self._key is not None

    def __getattr__(self, item):
        # Forward all other operations to the underlying file
        if item == "_file":
            raise AttributeError
        try:
            return getattr(self._file, item)
        except KeyError:
            raise AttributeError(item)

    def close(self):
        logger.debug("%s closing", type(self).__name__)
        if self.closed:
            logger.debug("%s already closed, returning key %r", type(self).__name__, self._key)
            return self._key

        digester = hashlib.sha1()
        self._file.seek(0)
        while True:
            data = self._file.read(16 * 1024 * 1024)
            if not data:
                break
            digester.update(data if isinstance(data, bytes) else data.encode(self.encoding))
        length = self._file.tell()
        self._file.seek(0)

        meta = ValueMeta(length=length, mime=self._mime, encoding=self._encoding,
                         **self._keywords)

        serialised_meta = pickle.dumps(meta)
        digester.update(serialised_meta)

        self._key = digester.hexdigest()
        logger.debug("%s key computed as %r", type(self).__name__, self._key)

        if self._key not in self._keeper:
            logging.debug(
                "%s persisting buffered data with length %d bytes",
                type(self).__name__,
                length
            )
            self._keeper._enqueue_pending(self)
            with self._keeper._storage.open_meta(self._key, 'w') as meta_file:
                meta_file.write(serialised_meta)
        else:
            self._file.close()

        logger.debug("%s closed, returning key %r", type(self).__name__, self._key)
        return self._key


class WriteableStream:

    def __init__(self, keeper, mime, encoding, **kwargs):
        logger.debug(
            "Creating %s with MIME type %r and encoding %r",
            type(self).__name__,
            mime,
            encoding
        )
        self._keeper = keeper
        self._encoding = encoding
        self._stack = contextlib.ExitStack()
        self._file = self._stack.enter_context(self._keeper._storage.open_temp('w+', encoding))
        self._mime = mime
        self._keywords = kwargs
        self._key = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
        return False

    @property
    def mime(self):
        return self._mime

    @property
    def encoding(self):
        return self._encoding

    @property
    def key(self):
        return self._key

    def write(self, data):
        self._file.write(data)

    @property
    def closed(self):
        return self._key is not None

    def __getattr__(self, item):
        # Forward all other operations to the underlying file
        if item == "_file":
            raise AttributeError
        try:
            return getattr(self._file, item)
        except KeyError:
            raise AttributeError(item)

    def close(self):
        logger.debug("%s closing", type(self).__name__)
        if self.closed:
            logger.debug("%s already closed, returning key %r", type(self).__name__, self._key)
            return self._key

        self._stack.close()
        assert self._file.closed
        logger.debug("%s reopening %r", type(self).__name__, self._file.name)
        logger.debug("%s computing key...", type(self).__name__)
        with open(self._file.name, mode='rb', encoding=None) as self._file:
            digester = hashlib.sha1()
            while True:
                data = self._file.read(16 * 1024 * 1024)
                if not data:
                    break
                digester.update(data)
            length = self._file.tell()

        meta = ValueMeta(length=length, mime=self._mime, encoding=self._encoding,
                         **self._keywords)
        serialised_meta = pickle.dumps(meta)
        digester.update(serialised_meta)
        key = digester.hexdigest()
        logger.debug("%s key computed as %r", type(self).__name__, key)

        if key not in self._keeper:
            logging.debug(
                "%s promoting temporary file %s to permanent",
                type(self).__name__,
                self._file.name
            )
            self._keeper._storage.promote_temp(self._file.name, key)
            with self._keeper._storage.open_meta(key, 'w') as meta_file:
                meta_file.write(serialised_meta)
        else:
            self._keeper._storage.remove_temp(self._file.name)
        logger.debug("%s closed, returning key %r", type(self).__name__, key)
        self._key = key
        return self._key


class KeeperClosed(Exception):

    def __init__(self):
        super().__init__("Keeper has been closed")


class StreamMap:

    def __init__(self):
        self._lock = threading.RLock()
        self._streams = {}

    def __contains__(self, key):
        with self._lock:
            return key in self._streams

    def __getitem__(self, key):
        with self._lock:
            return self._streams[key]

    def add(self, stream):
        with self._lock:
            self._streams[stream.key] = stream

    def discard(self, key):
        with self._lock:
            with contextlib.suppress(KeyError):
                del self._streams[key]

    def keys(self):
        with self._lock:
            return set(self._streams.keys())

    def __len__(self):
        with self._lock:
            return len(self._streams)


class Keeper(object):

    def __init__(self, dirpath):
        """Instantiate a Keeper store with a directory path.
        """
        logger.debug("Creating %s with dirpath %s", type(self).__name__, dirpath)
        dirpath = Path(dirpath)
        self._storage = filestorage.FileStorage(dirpath)
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
        self._executor.shutdown()
        self._storage.close()
        self._storage = None

