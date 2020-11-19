import contextlib
import hashlib
import logging
import pickle
import threading
from io import BytesIO, StringIO

from keeper.values import ValueMeta


logger = logging.getLogger(__name__)


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


class WriteableBinaryStream:

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
        self._file = self._stack.enter_context(self._keeper._storage.openout_temp())
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

    # def __getattr__(self, item):
    #     # Forward all other operations to the underlying file
    #     if item == "_file":
    #         raise AttributeError
    #     try:
    #         return getattr(self._file, item)
    #     except KeyError:
    #         raise AttributeError(item)

    def close(self):
        logger.debug("%s closing", type(self).__name__)
        if self.closed:
            logger.debug("%s already closed, returning key %r", type(self).__name__, self._key)
            return self._key

        self._stack.close()
        assert self._file.closed
        logger.debug("%s reopening %r", type(self).__name__, self._file.name)
        logger.debug("%s computing key...", type(self).__name__)

        handle = self._file.name

        with self._keeper._storage.openin_temp(handle) as self._file:
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