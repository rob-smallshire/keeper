import logging
import os
import sys
import shutil
import unittest
import warnings

from keeper import Keeper
from keeper.values import Value

logging.basicConfig(level=logging.DEBUG)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.DEBUG)
logging.getLogger().addHandler(console)


class KeeperTests(unittest.TestCase):

    def setUp(self):
        self.original_stream = console.stream
        console.stream = sys.stdout
        self.keeper_root = 'testkeeper'
        shutil.rmtree(self.keeper_root, ignore_errors=True)

        try:
            os.mkdir(self.keeper_root)
        except FileExistsError:
            pass
        self.keeper = Keeper(self.keeper_root)

    def tearDown(self):
        self.keeper.close()
        try:
            shutil.rmtree(self.keeper_root)
        except FileNotFoundError:
            pass
        console.stream = self.original_stream

    def test_create(self):
        pass

    def test_len_empty(self):
        self.assertEqual(len(self.keeper), 0)

    def test_len_one(self):
        self.keeper.add(b'hsgshsgsha')
        self.assertEqual(len(self.keeper), 1)

    def test_len_two(self):
        self.keeper.add(b'hsgshsgsha')
        self.keeper.add(b'fdfdsffsdf')
        self.assertEqual(len(self.keeper), 2)

    def test_len_identical(self):
        key1 = self.keeper.add(b'gdgdgdggd')
        key2 = self.keeper.add(b'gdgdgdggd')
        self.assertEqual(len(self.keeper), 1)

    def test_distinct_keys(self):
        key1 = self.keeper.add(b'hsgshsgsha')
        key2 = self.keeper.add(b'xsgshsgsha')
        self.assertNotEqual(key1, key2)

    def test_key_identical(self):
        key1 = self.keeper.add(b'gdgdgdggd')
        key2 = self.keeper.add(b'gdgdgdggd')
        self.assertEqual(key1, key2)

    def test_store_strings(self):
        key1 = self.keeper.add("The quick brown fox jumped over the lazy dog")
        pass

    def test_store_unicode_strings(self):
        key1 = self.keeper.add("Søker for kanaler")
        pass

    def test_retrieve_bytes(self):
        string = "The hunting of the snark"
        key1 = self.keeper.add(string)
        value = self.keeper[key1]
        self.assertEqual(value.as_string(), string)

    def test_string_encoding(self):
        string = "It was the best of times. It was the worst of times."
        key1 = self.keeper.add(string)
        value = self.keeper[key1]
        self.assertEqual(value.meta.encoding, sys.getdefaultencoding())

    def test_unknown_key(self):
        with self.assertRaises(KeyError):
            _ = self.keeper['hello']

    def test_encoding(self):
        data = "søker sjåfør".encode('utf-16')
        key = self.keeper.add(data, encoding='utf-16')
        self.assertEqual(self.keeper[key].meta.encoding, 'utf-16')

    def test_add_bytes_retrieve_string(self):
        string = "søker sjåfør"
        data = string.encode('utf-16')
        key = self.keeper.add(data, encoding='utf-16')
        self.assertEqual(self.keeper[key].as_string(), string)

    def test_add_string_retrieve_bytes(self):
        string = "søker sjåfør"
        key = self.keeper.add(string)
        self.assertEqual(self.keeper[key].as_bytes(), string.encode())

    def test_length(self):
        data = b'The quick brown fox jumped over the lazy dog.'
        key = self.keeper.add(data)
        self.assertEqual(len(self.keeper[key].as_bytes()), self.keeper[key].meta.length)

    def test_mime_type(self):
        string = "<!DOCTYPE html><html><body><b>Thunderbirds are go</b></body></html>"
        key = self.keeper.add(string, mime="text/html")
        self.assertEqual(self.keeper[key].meta.mime, "text/html")

    def test_bytes_encoding(self):
        string = "<!DOCTYPE html><html><body><b>Thunderbirds are go</b></body></html>"
        data = string.encode('utf-32')
        key = self.keeper.add(data, encoding='utf-32')
        self.assertEqual(self.keeper[key].meta.encoding, 'utf-32')

    def test_meta_attributes_default(self):
        key = self.keeper.add(b'')
        meta = self.keeper[key].meta
        self.assertIn('length', meta)
        self.assertIn('mime', meta)
        self.assertIn('encoding', meta)

    def test_iterate_default_meta_attributes(self):
        key = self.keeper.add(b'')
        meta = self.keeper[key].meta
        attributes = list(self.keeper[key].meta)
        self.assertEqual(len(attributes), 3)
        self.assertIn('length', attributes)
        self.assertIn('mime', attributes)
        self.assertIn('encoding', attributes)

    def test_arbitrary_metadata(self):
        text = "We'll attach some arbitrary meta data to this"
        key = self.keeper.add(text, mime="text/plain", filename="foo.txt", author="Joe Bloggs")
        self.assertEqual(self.keeper[key].meta.filename, "foo.txt")
        self.assertEqual(self.keeper[key].meta.author, "Joe Bloggs")

    def test_iterate_arbitrary_metadata(self):
        text = "We'll attach some arbitrary meta data to this"
        key = self.keeper.add(text, mime="text/plain", filename="foo.txt", author="Joe Bloggs")
        attributes = list(self.keeper[key].meta)
        self.assertEqual(len(attributes), 5)
        self.assertIn('length', attributes)
        self.assertIn('mime', attributes)
        self.assertIn('encoding', attributes)
        self.assertIn('filename', attributes)
        self.assertIn('author', attributes)

    def test_meta_keys_distinct(self):
        string = "<!DOCTYPE html><html><body><b>Thunderbirds are go</b></body></html>"
        key1 = self.keeper.add(string, mime="text/html")
        key2 = self.keeper.add(string, mime="text/plain")
        self.assertNotEqual(key1, key2)

    def test_encodings_distinct(self):
        key1 = self.keeper.add(b'', encoding='utf-8')
        key2 = self.keeper.add(b'', encoding='utf-16')
        self.assertNotEqual(key1, key2)

    def test_large_data(self):
        data = os.urandom(10 * 1024 * 1024)
        key = self.keeper.add(data)
        data2 = self.keeper[key].as_bytes()
        self.assertEqual(data, data2)

    def test_contains_negative(self):
        self.assertNotIn('2a206783b16f327a53555861331980835a0e059e', self.keeper)

    def test_contains_positive(self):
        key = self.keeper.add("Some data")
        self.assertIn(key, self.keeper)

    def test_remove_item_positive(self):
        text = "We'll attach some arbitrary meta data to this"
        key = self.keeper.add(text, mime="text/plain", filename="foo.txt", author="Joe Bloggs")
        self.assertIn(key, self.keeper)
        del self.keeper[key]
        self.assertNotIn(key, self.keeper)

    def test_remove_item_negative(self):
        with self.assertRaises(KeyError):
            _ = self.keeper['2a206783b16f327a53555861331980835a0e059e']


class StreamTests(unittest.TestCase):

    def setUp(self):
        self.original_stream = console.stream
        console.stream = sys.stdout
        self.keeper_root = 'testkeeper'
        shutil.rmtree(self.keeper_root, ignore_errors=True)

        try:
            os.mkdir(self.keeper_root)
        except FileExistsError:
            pass
        self.keeper = Keeper(self.keeper_root)

    def tearDown(self):
        self.keeper.close()
        try:
            shutil.rmtree(self.keeper_root)
        except FileNotFoundError:
            pass
        console.stream = self.original_stream


    def test_add_stream_in_context(self):
        with self.keeper.add_stream() as stream:
            stream.write(b"The quick brown fox jumped over the lazy dog")
        self.assertIn(stream.key, self.keeper)

    def test_add_stream_with_close(self):
        stream = self.keeper.add_stream()
        stream.write(b"The very hungry caterpillar")
        stream.close()
        self.assertIn(stream.key, self.keeper)

    def test_key_in_none_before_close(self):
        stream = self.keeper.add_stream()
        stream.write(b"The very hungry caterpillar")
        self.assertIs(stream.key, None)
        stream.close()

    def test_key_identical_bytes_add_before_stream(self):
        key1 = self.keeper.add(b'gdgdgdggd')
        with self.keeper.add_stream() as stream:
            stream.write(b'gdgdgdggd')
        key2 = stream.key
        self.assertEqual(key1, key2)
        self.assertEqual(len(self.keeper), 1)

    def test_key_identical_bytes_stream_before_add(self):
        with self.keeper.add_stream() as stream:
            stream.write(b'gdgdgdggd')
        key1 = stream.key
        key2 = self.keeper.add(b'gdgdgdggd')
        self.assertEqual(key1, key2)
        self.assertEqual(len(self.keeper), 1)

    def test_key_identical_strings_add_before_stream(self):
        key1 = self.keeper.add('gdgdgdggd')
        with self.keeper.add_stream(encoding=sys.getdefaultencoding()) as stream:
            stream.write('gdgdgdggd')
        key2 = stream.key
        self.assertEqual(key1, key2)
        self.assertEqual(len(self.keeper), 1)

    def test_key_identical_strings_stream_before_add(self):
        with self.keeper.add_stream(encoding=sys.getdefaultencoding()) as stream:
            stream.write('gdgdgdggd')
        key1 = stream.key
        key2 = self.keeper.add('gdgdgdggd')
        self.assertEqual(key1, key2)
        self.assertEqual(len(self.keeper), 1)

    def test_cleanup_pending_add_stream(self):
        # Temporarily ignore warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            stream = self.keeper.add_stream()
            stream.write(b"The very hungry caterpillar")
            self.assertIs(stream.key, None)
            # deliberately omitted close causes an intentional ResourceWarning
            self.assertEqual(len(self.keeper), 0)

            self.keeper = Keeper(self.keeper_root)
            stream = self.keeper.add_stream()
            stream.write(b"The very hungry caterpillar")
            stream.close()
            self.assertEqual(len(self.keeper), 1)



class BufferedStreamTests(unittest.TestCase):

    def setUp(self):
        self.original_stream = console.stream
        console.stream = sys.stdout
        self.keeper_root = 'testkeeper'
        shutil.rmtree(self.keeper_root, ignore_errors=True)

        try:
            os.mkdir(self.keeper_root)
        except FileExistsError:
            pass
        self.keeper = Keeper(self.keeper_root)

    def tearDown(self):
        self.keeper.close()
        try:
            shutil.rmtree(self.keeper_root)
        except FileNotFoundError:
            pass
        console.stream = self.original_stream


    def test_add_buffered_stream_in_context(self):
        with self.keeper.add_buffered_stream() as stream:
            stream.write(b"The quick brown fox jumped over the lazy dog")
        self.assertIn(stream.key, self.keeper)

    def test_add_buffered_stream_with_close(self):
        stream = self.keeper.add_buffered_stream()
        stream.write(b"The very hungry caterpillar")
        stream.close()
        self.assertIn(stream.key, self.keeper)


    # def test_add_large_stream_with_close(self):
    #     stream = self.keeper.add_buffered_stream()
    #     stream.write(bytes(1000000000))
    #     stream.close()
    #     value = self.keeper[stream.key]
    #     print(type(value))
    #     print(len(value.as_bytes()))
    #     self.assertIn(stream.key, self.keeper)
    #     while True:
    #         value = self.keeper[stream.key]
    #         print(type(value))
    #         print(len(value.as_bytes()))
    #         if isinstance(value, Value):
    #             break
    #     print(type(value))
    #     print(len(value.as_bytes()))
    #     self.assertIn(stream.key, self.keeper)

    def test_key_in_none_before_close(self):
        stream = self.keeper.add_buffered_stream()
        stream.write(b"The very hungry caterpillar")
        self.assertIs(stream.key, None)
        stream.close()

    def test_key_identical_bytes_add_before_stream(self):
        key1 = self.keeper.add(b'gdgdgdggd')
        with self.keeper.add_buffered_stream() as stream:
            stream.write(b'gdgdgdggd')
        key2 = stream.key
        self.assertEqual(key1, key2)
        self.assertEqual(len(self.keeper), 1)

    def test_key_identical_bytes_stream_before_add(self):
        with self.keeper.add_buffered_stream() as stream:
            stream.write(b'gdgdgdggd')
        key1 = stream.key
        key2 = self.keeper.add(b'gdgdgdggd')
        self.assertEqual(key1, key2)
        self.assertEqual(len(self.keeper), 1)

    def test_key_identical_strings_add_before_stream(self):
        key1 = self.keeper.add('gdgdgdggd')
        with self.keeper.add_buffered_stream(encoding=sys.getdefaultencoding()) as stream:
            stream.write('gdgdgdggd')
        key2 = stream.key
        self.assertEqual(key1, key2)
        self.assertEqual(len(self.keeper), 1)

    def test_key_identical_strings_stream_before_add(self):
        with self.keeper.add_buffered_stream(encoding=sys.getdefaultencoding()) as stream:
            stream.write('gdgdgdggd')
        key1 = stream.key
        key2 = self.keeper.add('gdgdgdggd')
        self.assertEqual(key1, key2)
        self.assertEqual(len(self.keeper), 1)

    def test_cleanup_pending_add_buffered_stream(self):
        # Temporarily ignore warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            stream = self.keeper.add_buffered_stream()
            stream.write(b"The very hungry caterpillar")
            self.assertIs(stream.key, None)
            # deliberately omitted close causes an intentional ResourceWarning
            self.assertEqual(len(self.keeper), 0)

            self.keeper = Keeper(self.keeper_root)
            stream = self.keeper.add_buffered_stream()
            stream.write(b"The very hungry caterpillar")
            stream.close()
            self.assertEqual(len(self.keeper), 1)

