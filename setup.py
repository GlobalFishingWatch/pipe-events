from setuptools import setup, find_packages

setup(
    name="events",
    version="1.0.0",
    description="Apache Beam pipeline which computes fishing events out of classified positional messages.",
    author="Global Fishing Watch",
    license="Apache 2",
    packages=find_packages(),
    install_requires=[
        "google-cloud-dataflow==2.1.0",
        "statistics==1.0.3.5",
    ]
)
