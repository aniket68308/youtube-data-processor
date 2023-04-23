from setuptools import setup, find_packages

setup(
    name="youtube-data-processor",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyspark==3.4.0"
    ],
)
