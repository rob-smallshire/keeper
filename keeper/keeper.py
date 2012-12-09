import hashlib
import pickle
import sys

from .storage import filestorage

__author__ = 'rjs'

class Value:
    """
    value.data
    value.meta
    value.meta.mime
    value.meta.path
    value.meta.length
    value.meta.encoding
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
    def meta(self):
        return self._meta

    def as_bytes(self):
        """
        Return the value as a bytes object.
        """
        with self._keeper._storage.open_data(self._key, 'rb') as data_file:
            data = data_file.read()

        return data

    def as_file(self):
        """
        Return the data as a read-only file-like object.
        """
        mode = 'rb' if self.meta.encoding is None else 'r'
        return self._keeper._storage.open_data(self._key, mode=mode,
                                               encoding=self.meta.encoding)


    def as_string(self):
        """
        Return a string according the the supplied encoding
        """
        with self._keeper._storage.open_data(self._key, mode='r',
                                    encoding=self.meta.encoding) as data_file:
            s = data_file.read()
        return s


    def __str__(self):
        """
        Decoded into a string.
        """
        return self.as_string()

    def __len__(self):
        """
        The length of the data in bytes (NOT characters).
        """
        return self._meta.length


    @property
    def path(self):
        """
        A filesystem path to the resource
        """
        return self._keeper._storage.path


class ValueMeta:
    """
    Immutable meta data for a value.
    """
    def __init__(self, length, mime=None, encoding=None, **kwargs):
        self._length = length
        self._mime = mime
        self._encoding = encoding
        self._keywords = kwargs


    @property
    def mime(self):
        """
        The MIME type as a string.
        """
        return self._mime


    @property
    def encoding(self):
        """
        An optional encoding used for string data.
        """
        return self._encoding


    def __getattr__(self, item):
        if item == "_keywords":
            raise AttributeError
        try:
            return self._keywords[item]
        except KeyError:
            raise AttributeError


class Keeper(object):

    def __init__(self, dirname):
        self._storage = filestorage.FileStorage(dirname)

    def add(self, data, mime=None, encoding=None, **kwargs):
        """Adds data into the store.

        Args:
            data (bytes): The data to be added.
            mime: The MIME type of the data
            encoding: The encoding of the data
        Returns:
            A key for the data
        """

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
        digest = digester.hexdigest()

        # TODO: Check for the value already being there and return early

        with self._storage.open_meta(digest, 'w') as meta_file:
            meta_file.write(serialised_meta)

        with self._storage.open_data(digest, 'w') as data_file:
            data_file.write(data)

        return digest

    def __contains__(self, key):
        try:
            with self._storage.open_meta(key):
                return True
        except (ValueError, FileNotFoundError):
            return False


    def __iter__(self):
        yield from self._storage

    def __getitem__(self, key):
        """Retrieve data by its key.

        Args:
            key: A key obtained from add().

        Returns:
            An object representing the data associated with key.

        Raises:
            KeyError: If the key is unknown.
        """
        return Value(self, key)

    def __delitem__(self, key):
        """Remove an item by it's key"""
        if key not in self:
            raise KeyError(key)
        self._storage.remove(key)

    def __len__(self):
            return sum(1 for _ in self)
