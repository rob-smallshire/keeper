import os
import shutil
import unittest

from keeper.storage.filestorage import FileStorage


class FileStorageTests(unittest.TestCase):

    def setUp(self):
        self.keeper_root = 'testkeeper'
        self._remove_rootdir()

    def tearDown(self):
        self._remove_rootdir()

    def _remove_rootdir(self):
        shutil.rmtree(self.keeper_root, ignore_errors=True)
        if os.path.exists(self.keeper_root):
            if os.path.isdir(self.keeper_root):
                os.rmdir(self.keeper_root)
            if os.path.isfile(self.keeper_root):
                os.unlink(self.keeper_root)

    def test_construction_does_not_raise_exception_when_dirpath_available(self):
        fs = FileStorage(self.keeper_root)

    def test_construction_does_not_raise_exception_when_dirpath_is_existing_dir(self):
        os.mkdir(self.keeper_root)
        FileStorage(self.keeper_root)

    def test_dirpath_already_exists_as_file_raises_file_exists_error(self):
        try:
            os.close(os.open(self.keeper_root, os.O_CREAT))
            with self.assertRaises(FileExistsError):
                FileStorage(self.keeper_root)
        finally:
            shutil.rmtree(self.keeper_root, ignore_errors=True)

    def test_construction_does_not_raise_exception_when_dirpath_temp_is_existing_dir(self):
        os.mkdir(self.keeper_root)
        os.mkdir(os.path.join(self.keeper_root, "temp"))
        FileStorage(self.keeper_root)

    def test_dirpath_temp_already_exists_as_file_raises_file_exists_error(self):
        os.mkdir(self.keeper_root)
        temp_dirpath = os.path.join(self.keeper_root, "temp")
        os.close(os.open(temp_dirpath, os.O_CREAT))
        with self.assertRaises(FileExistsError):
            FileStorage(self.keeper_root)

    def test_dirpath_meta_already_exists_as_file_raises_file_exists_error(self):
        os.mkdir(self.keeper_root)
        meta_dirpath = os.path.join(self.keeper_root, "meta")
        os.close(os.open(meta_dirpath, os.O_CREAT))
        with self.assertRaises(FileExistsError):
            FileStorage(self.keeper_root)

    def test_dirpath_data_already_exists_as_file_raises_file_exists_error(self):
        os.mkdir(self.keeper_root)
        data_dirpath = os.path.join(self.keeper_root, "data")
        os.close(os.open(data_dirpath, os.O_CREAT))
        with self.assertRaises(FileExistsError):
            FileStorage(self.keeper_root)
