# Youtube Data Processor

This Docker application provides a PySpark environment to run the `main.py` script for the `youtube-data-processor` application with local disk mount for input file and output directory.

## Prerequisites

- Docker (version 19.03 or later)

## Installation

1. Clone this repository to your local machine.

   ```
   git clone https://github.com/aniket68308/youtube-data-processor.git
   cd youtube-data-processor
   ```

2. Build the Docker image using the following command:

   ```
   docker build -t data-processor-spark-app .
   ```

   This will build the Docker image with the name `data-processor-spark-app`.

## Usage

To run the `main.py` script with local disk mount, use the following command:

```
docker run -it --rm \
  -v <complete_path_of_input_csv_file>:/app/data/input_file.csv \
  -v <complete_path_of_output_directory>:/app/data/output \
  -p 4040:4040 -p 8080:8080 \
  data-processor-spark-app -i /app/data/input/input_file.csv -o /app/data/output
```

This command mounts the local `path_input_of_csv_file` and `input_path_output_folder` directories to the corresponding directories inside the Docker container. It also maps the container ports 4040 and 8080 to the corresponding host ports 4040 and 8080 respectively, so that you can access Spark UI from your web browser. Finally, it runs the `data-processor-spark-app` Docker image with the `-i` and `-o` options to specify the input file and output directory for the `main.py` script.
