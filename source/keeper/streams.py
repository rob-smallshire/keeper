import contextlib
import hashlib
import logging
import pickle

from keeper.values import ValueMeta


logger = logging.getLogger(__name__)


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

        with self._keeper._storage.openin_temp(handle) as temp_file:
            digester = hashlib.sha1()
            while True:
                data = temp_file.read(16 * 1024 * 1024)
                if not data:
                    break
                digester.update(data)
            length = temp_file.tell()

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
            self._keeper._storage.promote_temp(handle, key)
            with self._keeper._storage.openout_meta(key) as meta_file:
                meta_file.write(serialised_meta)
        else:
            self._keeper._storage.remove_temp(handle)
        logger.debug("%s closed, returning key %r", type(self).__name__, key)
        self._key = key
        return self._key
