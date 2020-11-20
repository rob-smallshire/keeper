import unittest
from unittest.mock import Mock

from keeper.storage.writecachestorage import WriteCacheStorage


class TestWriteCacheStorage(unittest.TestCase):

    def test_constructing_with_closed_underlying_storage_raises_value_error(self):
        storage = Mock(closed=True)
        with self.assertRaises(ValueError):
           WriteCacheStorage(storage)

    def test_closing_does_not_close_underlying_storage(self):
        storage = Mock(closed=False)
        f = WriteCacheStorage(storage)
        f.close()
        storage.close.assert_not_called()

    def test_is_not_closed_after_construction(self):
        storage = Mock(closed=False)
        f = WriteCacheStorage(storage)
        self.assertFalse(f.closed)

    def test_is_closed_after_closing(self):
        storage = Mock(closed=False)
        f = WriteCacheStorage(storage)
        f.close()
        self.assertTrue(f.closed)

    def test_storage_property_returns_storage_parameter(self):
        storage = Mock(closed=False)
        f = WriteCacheStorage(storage)
        self.assertIs(f.storage, storage)
