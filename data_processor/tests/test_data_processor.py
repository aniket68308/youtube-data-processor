import unittest
from pyspark.sql import SparkSession
from data_processor.data_processor import YouTubeDataProcessor


class TestYouTubeDataProcessor(unittest.TestCase):
    def setUp(self):
        self.raw_data_path = "/path/to/raw_data.csv"
        self.output_folder_path = "/path/to/output_folder"
        self.spark = SparkSession.builder.appName("test").getOrCreate()
        self.processor = YouTubeDataProcessor(self.raw_data_path, self.output_folder_path)

    def test_clean_raw_data(self):
        # create test input data
        raw_data = self.spark.createDataFrame([
            ("1", "2023-04-23T08:15:30.000Z", "10", "20", "30", "40"),
            ("2", None, "10", "20", "30", "40"),
            ("3", "2023-04-23T08:15:30.000Z", "10", "20", "30", None),
            ("4", "invalid_time_format", "10", "20", "30", "40"),
        ], ["video_id", "publish_time", "views", "comment_count", "likes", "dislikes"])

        # create expected output data
        expected_data = self.spark.createDataFrame([
            ("1", 2023, 4),
        ], ["video_id", "publish_year", "publish_month"])

        # run the function being tested
        cleaned_data = self.processor.clean_raw_data(raw_data)

        # check that the output is correct
        self.assertTrue(expected_data.subtract(cleaned_data).rdd.isEmpty())
        self.assertTrue(cleaned_data.subtract(expected_data).rdd.isEmpty())

    def test_read_existing_folder_list(self):
        # create test input data
        file_path_list = [["/path/to/folder/*", "2022", "12", "1"]]

        # create expected output data
        expected_data = self.spark.createDataFrame([
            ("1", 100, 200, 300, 2022, 12, "1"),
        ], ["video_id", "views", "comment_count", "likes", "publish_year", "publish_month", "category_id"])

        # write expected data to disk
        expected_data.write.mode("overwrite").json("/path/to/folder/publish_year=2022/publish_month=12/category_id=1")

        # run the function being tested
        existing_data = self.processor.read_existing_folder_list(file_path_list, self.spark)

        # check that the output is correct
        self.assertTrue(expected_data.subtract(existing_data).rdd.isEmpty())
        self.assertTrue(existing_data.subtract(expected_data).rdd.isEmpty())
