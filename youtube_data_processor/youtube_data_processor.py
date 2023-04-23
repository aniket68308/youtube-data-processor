from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

from logger.logger import Logger


class YouTubeDataProcessor:
    def __init__(self, raw_data_path, output_folder_path):
        self.raw_data_path = raw_data_path
        self.output_folder_path = output_folder_path
        self.logger = Logger(self.__class__.__name__)

    def filter_existing_data(self, raw_data, output_folder_path, spark):
        # Filter the existing data based on the publish_year, publish_month, and category_id values from the raw data
        slash_value_lit = lit("/")
        wild_card_lit = lit("*")
        output_folder_path_lit = lit(output_folder_path)

        unique_folder_combination_df = raw_data.select(col("publish_year"), col("publish_month"),
                                                       col("category_id")).distinct() \
            .withColumn("folder_path",
                        concat(
                            output_folder_path_lit,
                            slash_value_lit,
                            lit("publish_year="),
                            col("publish_year"),
                            slash_value_lit,
                            lit("publish_month="),
                            col("publish_month"),
                            slash_value_lit,
                            lit("category_id="),
                            col("category_id"),
                            slash_value_lit,
                            wild_card_lit
                        )
                        )

        folder_to_read_list = unique_folder_combination_df.select("folder_path",
                                                                  "publish_year",
                                                                  "publish_month",
                                                                  "category_id").collect()
        folder_to_read_list = [[str(row[0]), str(row[1]), str(row[2]), str(row[3])] for row in folder_to_read_list]
        existing_data_df = self.read_existing_folder_list(folder_to_read_list, spark)

        return existing_data_df

    def read_existing_folder_list(self, file_path_list, spark):
        # We have a list of file paths in `file_paths`

        combined_df = None
        for file_path in file_path_list:
            try:
                self.logger.info(f"Reading File : {file_path[0]}")
                df = spark.read.json(file_path[0])
                df = df.withColumn("publish_year", lit(file_path[1]).astype(IntegerType())). \
                    withColumn("publish_month", lit(file_path[2]).astype(IntegerType())). \
                    withColumn("category_id", lit(file_path[3]))
                if combined_df:
                    combined_df = combined_df.union(df.select(combined_df.columns))
                else:
                    combined_df = df
            except:
                self.logger.info(f"File not found: {file_path[0]}")
                pass
        return combined_df

    def clean_raw_data(self, raw_data_df):
        # Add columns for publish_year and publish_month using the publish_time column
        cleaned_data = raw_data_df \
            .filter(col("publish_time").isNotNull()) \
            .filter(to_timestamp(col("publish_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").isNotNull()) \
            .filter(col("video_id").isNotNull()) \
            .filter(col("category_id").isNotNull())
        cleaned_data = cleaned_data.withColumn("publish_year", year("publish_time"))
        cleaned_data = cleaned_data.withColumn("publish_month", month("publish_time"))
        return cleaned_data

    def run(self):
        # Initialize the SparkSession
        spark = SparkSession.builder.appName("YouTubeDataConverter").getOrCreate()
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

        # column list to select whenever required
        columns_list = ["video_id", "views", "comment_count", "likes",
                        "dislikes", "publish_year", "publish_month", "category_id"]

        # Load the raw data into a PySpark DataFrame
        raw_data = spark.read.csv(self.raw_data_path, header=True, inferSchema=True)
        cleaned_data = self.clean_raw_data(raw_data)
        cleaned_data = cleaned_data.select(columns_list)
        # Filter the existing data based on the publish_year, publish_month, and category_id values from the raw data
        self.logger.info("Reading existing data")
        existing_data = self.filter_existing_data(cleaned_data, self.output_folder_path, spark)
        if existing_data:
            existing_data = existing_data.select(columns_list)

        # Combine the existing and new data and write it back to the files
        group_cols = ["video_id", "publish_year", "publish_month", "category_id"]
        aggregated_data_df = cleaned_data.groupBy(group_cols) \
            .agg(sum("views").cast(IntegerType()).alias("views"),
                 sum("comment_count").cast(IntegerType()).alias("comment_count"),
                 sum("likes").cast(IntegerType()).alias("likes"),
                 sum("dislikes").cast(IntegerType()).alias("dislikes"))
        updated_data = aggregated_data_df
        if existing_data is not None:
            # Merge the dataframes
            merged_df = aggregated_data_df.union(existing_data.select(aggregated_data_df.columns))
            # Group by the common columns and sum the values of views, comment_count, likes, and dislikes
            updated_data = merged_df.groupBy(group_cols) \
                .agg(sum(col("views")).cast(IntegerType()).alias("views"),
                     sum(col("comment_count")).cast(IntegerType()).alias("comment_count"),
                     sum(col("likes")).cast(IntegerType()).alias("likes"),
                     sum(col("dislikes")).cast(IntegerType()).alias("dislikes"))

        self.logger.info("Writing updated data")
        partition_by_col = ["publish_year", "publish_month", "category_id"]
        updated_data.write.partitionBy(partition_by_col) \
            .mode("overwrite").json(self.output_folder_path)
