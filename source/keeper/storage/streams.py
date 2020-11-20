import io


class WriteOnlyStream:
    """A write-only wrapper around a stream.

    Closing this stream does not close the underlying stream.
    """

    def __init__(self, raw, name=None):
        self._raw = raw
        self._name = name

    def close(self):
        self._raw = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def _f(self):
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        return self._raw

    @property
    def closed(self):
        return self._raw is None

    def fileno(self):
        raise io.UnsupportedOperation(f"{type(self).__name__} has no fileno")

    def flush(self):
        return self._f.flush()

    def isatty(self):
        return self._f.isatty()

    @property
    def mode(self):
        return "wb"

    def readable(self):
        return False

    def read(self, n=None):
        raise io.UnsupportedOperation(f"{type(self).__name__} is not readable")

    def readall(self):
        raise io.UnsupportedOperation(f"{type(self).__name__} is not readable")

    def readinto(self, b):
        raise io.UnsupportedOperation(f"{type(self).__name__} is not readable")

    def readline(self, size: int = -1):
        raise io.UnsupportedOperation(f"{type(self).__name__} is not readable")

    def readlines(self, hint: int = -1):
        raise io.UnsupportedOperation(f"{type(self).__name__} is not readable")

    def seek(self, offset, whence=io.SEEK_SET):
        raise io.UnsupportedOperation(f"{type(self).__name__} is not seekable")

    def tell(self):
        return self._f.tell()

    def truncate(self, size=None):
        raise io.UnsupportedOperation(f"{type(self).__name__} is not truncatable")

    def writable(self) -> bool:
        return True

    def write(self, b) -> int:
        return self._f.write(b)

    def writelines(self, lines):
        return self._f.writelines(lines)

    def __del__(self):
        return self.close()

    @property
    def name(self):
        if self._name is not None:
            return self._name
        try:
            return self._raw.name
        except AttributeError:
            return self._name


class ReadOnlyStream:

    """A write-only wrapper around a stream.

    Closing this stream does not close the underlying stream.
    """

    def __init__(self, raw, name=None):
        self._raw = raw
        self._name = name

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self._raw = None

    @property
    def _f(self):
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        return self._raw

    @property
    def closed(self):
        return self._raw is None

    def fileno(self):
        raise io.UnsupportedOperation(f"{type(self).__name__} has no fileno")

    def flush(self):
        return self._f.flush()

    def isatty(self):
        return self._f.isatty()

    @property
    def mode(self):
        return "rb"

    def readable(self):
        return True

    def read(self, n=None):
        return self._f.read(n)

    def readall(self):
        return self._f.readall()

    def readinto(self, b):
        return self._f.readinto(b)

    def readline(self, size: int = -1):
        return self._f.readline(size)

    def readlines(self, hint: int = -1):
        return self._f.readlines(hint)

    def seek(self, offset, whence=io.SEEK_SET):
        raise io.UnsupportedOperation(f"{type(self).__name__} is not seekable")

    def tell(self):
        return self._f.tell()

    def truncate(self, size=None):
        raise io.UnsupportedOperation(f"{type(self).__name__} is not truncatable")

    def writable(self) -> bool:
        return True

    def write(self, b) -> int:
        raise io.UnsupportedOperation(f"{type(self).__name__} is not writable")

    def writelines(self, lines):
        raise io.UnsupportedOperation(f"{type(self).__name__} is not writable")

    def __del__(self):
        return self.close()

    @property
    def name(self):
        if self._name is not None:
            return self._name
        try:
            return self._raw.name
        except AttributeError:
            return self._name