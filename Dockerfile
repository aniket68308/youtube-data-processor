FROM python:3.8-slim-buster

# Install Java
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk-headless procps && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV SPARK_VERSION=3.4.0
ENV HADOOP_VERSION=3

# Set working directory
WORKDIR /app

# Download and install Spark
RUN apt-get update && \
    apt-get install -y wget && \
    rm -rf /var/lib/apt/lists/*


RUN wget https://dlcdn.apache.org/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz && \
    tar -xzf /app/spark-3.4.0-bin-hadoop3.tgz && \
    mv /app/spark-3.4.0-bin-hadoop3 /usr/local/spark && \
    rm /app/spark-3.4.0-bin-hadoop3.tgz


COPY dist/youtube-data-processor-0.1.0.tar.gz /app/youtube-data-processor.tar.gz
COPY main.py /app/main.py

RUN pip install /app/youtube-data-processor.tar.gz

# Expose ports for accessing Spark UI
EXPOSE 4040 8080

# Mount output directory
VOLUME ["/app/data/output"]

# Set command to run the PySpark code
CMD ["/usr/local/spark/bin/spark-submit", "--master", "local[2]", "main.py", "-i", "/app/data/input_file.csv", "-o", "/app/data/output"]