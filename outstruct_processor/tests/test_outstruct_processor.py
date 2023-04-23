import os
import shutil
import tempfile
import unittest
from outstruct_processor.outstruct_processor import OutStructProcessor


class TestOutStructProcessor(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.processor = OutStructProcessor(self.test_dir)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_remove_crc_files(self):
        # Create test files with .crc and .json extensions
        file1 = os.path.join(self.test_dir, 'file1.json')
        file2 = os.path.join(self.test_dir, 'file2.crc')
        file3 = os.path.join(self.test_dir, 'file3.json')
        file4 = os.path.join(self.test_dir, 'file4.crc')
        open(file1, 'a').close()
        open(file2, 'a').close()
        open(file3, 'a').close()
        open(file4, 'a').close()

        # Call remove_crc_files method and check if .crc files were deleted
        self.processor.remove_crc_files()
        self.assertFalse(os.path.exists(file2))
        self.assertFalse(os.path.exists(file4))
        self.assertTrue(os.path.exists(file1))
        self.assertTrue(os.path.exists(file3))

    def test_rename_json_files(self):
        # Create test files with .json extensions
        file1 = os.path.join(self.test_dir, 'file1.json')
        file2 = os.path.join(self.test_dir, 'file2.json')
        file3 = os.path.join(self.test_dir, 'file3.json')
        file4 = os.path.join(self.test_dir, 'file4.json')
        open(file1, 'a').close()
        open(file2, 'a').close()
        open(file3, 'a').close()
        open(file4, 'a').close()

        # Call rename_json_files method and check if files were renamed correctly
        self.processor.rename_json_files()
        self.assertTrue(os.path.exists(os.path.join(self.test_dir, 'data_1.json')))
        self.assertTrue(os.path.exists(os.path.join(self.test_dir, 'data_2.json')))
        self.assertTrue(os.path.exists(os.path.join(self.test_dir, 'data_3.json')))
        self.assertTrue(os.path.exists(os.path.join(self.test_dir, 'data_4.json')))
        self.assertFalse(os.path.exists(file1))
        self.assertFalse(os.path.exists(file2))
        self.assertFalse(os.path.exists(file3))
        self.assertFalse(os.path.exists(file4))
