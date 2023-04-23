import argparse
import os
import shutil

from logger.logger import Logger
from data_processor.data_processor import YouTubeDataProcessor
from outstruct_processor.outstruct_processor import OutStructProcessor


def main():
    logger = Logger(__name__)
    # Create the argument parser object
    parser = argparse.ArgumentParser(description='Please provide input file, and output directory')

    # Add the file and directory arguments
    parser.add_argument('-i', '--input_file', type=str, help='path to the input file', required=True)
    parser.add_argument('-o', '--output_dir', type=str, help='path to the output directory', required=True)

    # Parse the arguments
    args = parser.parse_args()

    # Get the file, directory, and output paths from the arguments
    input_file_path = args.input_file
    output_dir_path = args.output_dir

    # Check if the input file exists
    if not os.path.isfile(input_file_path):
        logger.error(f"Error: {input_file_path} does not exist or is not a file")
        return

    # Check if the output directory exists
    if not os.path.isdir(output_dir_path):
        logger.error(f"Error: {output_dir_path} is not a directory")
        return

    # Run YoutubeData processor
    youtube_data_processor = YouTubeDataProcessor(input_file_path, output_dir_path)
    youtube_data_processor.run()
    output_file_structure_processor = OutStructProcessor(output_dir_path)
    output_file_structure_processor.run()


if __name__ == '__main__':
    main()
