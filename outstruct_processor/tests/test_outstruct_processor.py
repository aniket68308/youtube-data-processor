import os
import tempfile
import unittest
from outstruct_processor.outstruct_processor import OutStructProcessor


class TestOutStructProcessor(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.out_struct_processor = OutStructProcessor(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_run(self):
        # Create the subdirectories and a .crc file for testing
        publish_year_dir = "2023"
        publish_month_dir = "04"
        category_id_dir = "category1"
        dir_path = os.path.join(self.temp_dir.name, publish_year_dir, publish_month_dir, category_id_dir)
        os.makedirs(dir_path, exist_ok=True)
        with open(os.path.join(dir_path, "file.json"), 'w') as f:
            f.write("test")
        with open(os.path.join(dir_path, "file.crc"), 'w') as f:
            f.write("test")

        self.out_struct_processor.run()

        # Assert that the .crc file was removed and the .json file was renamed
        self.assertFalse(os.path.isfile(os.path.join(dir_path, "file.crc")))
        self.assertFalse(os.path.isfile(os.path.join(dir_path, "file.json")))
        self.assertTrue(os.path.isfile(os.path.join(dir_path, "data.json")))

    def test_remove_crc_files(self):
        # Create a .crc file for testing
        dir_path = self.temp_dir.name
        with open(os.path.join(dir_path, "file.crc"), 'w') as f:
            f.write("test")

        self.out_struct_processor.remove_crc_files(dir_path)

        self.assertFalse(os.path.isfile(os.path.join(dir_path, "file.crc")))

    def test_rename_json_files(self):
        # Create a .json file for testing
        dir_path = self.temp_dir.name
        with open(os.path.join(dir_path, "file.json"), 'w') as f:
            f.write("test")

        self.out_struct_processor.rename_json_files(dir_path)

        self.assertFalse(os.path.isfile(os.path.join(dir_path, "file.json")))
        self.assertTrue(os.path.isfile(os.path.join(dir_path, "data.json")))
