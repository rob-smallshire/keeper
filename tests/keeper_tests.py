import os
import sys
import shutil
import unittest
from keeper.keeper import Keeper

class KeeperTests(unittest.TestCase):

    def setUp(self):
        self.keeper_root = 'testkeeper'
        try:
            shutil.rmtree(self.keeper_root)
        except FileNotFoundError:
            pass
        try:
            os.mkdir(self.keeper_root)
        except FileExistsError:
            pass
        self.keeper = Keeper(self.keeper_root)

    def tearDown(self):
        try:
            shutil.rmtree(self.keeper_root)
        except FileNotFoundError:
            pass

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


    def test_len_two(self):
        key1 = self.keeper.add(b'hsgshsgsha')
        key2 = self.keeper.add(b'xsgshsgsha')
        self.assertNotEqual(key1, key2)

    def test_len_identical(self):
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
        self.assertRaises(KeyError, lambda: self.keeper['hello'])

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

    def test_mime_type(self):
        string = "<!DOCTYPE html><html><body><b>Thunderbirds are go</b></body></html>"
        key = self.keeper.add(string, mime="text/html")
        self.assertEqual(self.keeper[key].meta.mime, "text/html")

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

    def test_arbitrary_metadata(self):
        text = "We'll attach some arbitrary meta data to this"
        key = self.keeper.add(text, mime="text/plain", filename="foo.txt", author="Joe Bloggs")
        self.assertEqual(self.keeper[key].meta.filename, "foo.txt")
        self.assertEqual(self.keeper[key].meta.author, "Joe Bloggs")

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
        self.assertRaises(KeyError, lambda: self.keeper['2a206783b16f327a53555861331980835a0e059e'])

