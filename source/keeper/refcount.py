import threading


class ReferenceCount:

    def __init__(self, on_zero):
        """Manage a thread-safe counter. When it becomes zero invoke a callable.

        Args:
            on_zero: A callable invoked when the count becomes zero
        """
        self._on_zero = on_zero
        self._lock = threading.RLock()
        self._count = 0
        self._complete = False

    def increment(self):
        with self._lock:
            if self._complete:
                raise RuntimeError("{self!r} cannot be reused")
            self._count += 1

    def decrement(self):
        complete = False
        with self._lock:
            if self._complete:
                raise RuntimeError("{self!r} already complete")
            self._count -= 1
            if self._count == 0:
                self._complete = True
                complete = bool(self._complete)
        if complete:
            self._on_zero()