import os
from logger.logger import Logger


class OutStructProcessor:
    def __init__(self, output_dir):
        self.output_dir = output_dir
        self.logger = Logger(self.__class__.__name__)

    def run(self):
        # Loop through all subdirectories
        for publish_year_dir in os.listdir(self.output_dir):
            if not os.path.isdir(os.path.join(self.output_dir, publish_year_dir)):
                continue
            for publish_month_dir in os.listdir(os.path.join(self.output_dir, publish_year_dir)):
                if not os.path.isdir(os.path.join(self.output_dir, publish_year_dir, publish_month_dir)):
                    continue
                for category_id_dir in os.listdir(os.path.join(self.output_dir, publish_year_dir, publish_month_dir)):
                    if not os.path.isdir(
                            os.path.join(self.output_dir, publish_year_dir, publish_month_dir, category_id_dir)):
                        continue
                    dir_path = os.path.join(self.output_dir, publish_year_dir, publish_month_dir, category_id_dir)
                    self.remove_crc_files(dir_path)
                    self.rename_json_files(dir_path)

    def remove_crc_files(self, dir_path):
        # Remove .crc files
        for filename in os.listdir(dir_path):
            if filename.endswith(".crc"):
                filename_with_dir = os.path.join(dir_path, filename)
                self.logger.info("Removing {}".format(filename_with_dir))
                os.remove(filename_with_dir)

    def rename_json_files(self, dir_path):
        # Rename json files
        file_index = 0
        file_suffix = ""
        for filename in os.listdir(dir_path):
            if filename.endswith(".json"):
                new_filename = "data{}.json".format(file_suffix)
                old_file_path_with_dir = os.path.join(dir_path, filename)
                new_file_path_with_dir = os.path.join(dir_path, new_filename)
                self.logger.info("Renaming {} to {}".format(old_file_path_with_dir, new_file_path_with_dir))
                os.rename(old_file_path_with_dir, new_file_path_with_dir)
                file_index += 1
                file_suffix = "_{}".format(str(file_index))