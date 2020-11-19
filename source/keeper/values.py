import io
import pickle
import logging


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
        with self._keeper._storage.openin_data(self._key) as data_file:
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
        with self._keeper._storage.openin_data(self._key) as data_file:
            s = data_file.read().decode(encoding=self.meta.encoding)
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
        return self._keeper._storage._data_path(self._key)


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
        # TODO: Read-only wrapper for BytesIO. io.BufferedReader has a write
        #       method, so isn't much help
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