import hashlib
import pickle
import sys

from .storage import filestorage

__author__ = 'rjs'

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
    def meta(self):
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

class WriteableStream:

    def __init__(self, keeper, mime, encoding, **kwargs):
        self._keeper = keeper
        self._encoding = encoding
        self._file = self._keeper._storage.open_temp('w+', encoding)
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


    def __getattr__(self, item):
        # Forward all other operations to the underlying file
        if item == "_file":
            raise AttributeError
        try:
            return getattr(self._file, item)
        except KeyError:
            raise AttributeError(item)


    def close(self):
        if self.closed:
            return self._key

        self._file.flush()
        self._file.seek(0)

        # May need to close the file here and reopen in binary to get
        # bytes data for the digest

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
        fileno = self._file.fileno()
        self._file.close()
        self._key = digester.hexdigest()
        if self._key not in self._keeper:
            self._keeper._storage.promote_temp(fileno, self._key)
            with self._keeper._storage.open_meta(self._key, 'w') as meta_file:
                meta_file.write(serialised_meta)
        else:
            self._keeper._storage.remove_temp(fileno)
        return self._key



class Keeper(object):

    def __init__(self, dirname):
        self._storage = filestorage.FileStorage(dirname)

    def add_stream(self, mime=None, encoding=None, **kwargs):
        """Returns an open, writable file-like-object and context manager
        which when closed, commits the data to this keeper. Only then is the
        key accessible through the key property of the returned object
        """
        return WriteableStream(self, mime, encoding, **kwargs)

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
        key = digester.hexdigest()

        if key in self:
            return key

        with self._storage.open_meta(key, 'w') as meta_file:
            meta_file.write(serialised_meta)

        with self._storage.open_data(key, 'w') as data_file:
            data_file.write(data)

        return key

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
            A Value object representing the data associated with key.

        Raises:
            KeyError: If the key is unknown.
        """
        return Value(self, key)

    def __delitem__(self, key):
        """Remove an item by its key"""
        if key not in self:
            raise KeyError(key)
        self._storage.remove(key)

    def __len__(self):
        return sum(1 for _ in self)
