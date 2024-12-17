#!/usr/bin/env python

"""
Setup script for pipe-orbcomm
"""

from setuptools import find_packages
from setuptools import setup

setup(
    name='pipe-events',
    version='4.2.1',
    author="Global Fishing Watch.",
    description=(
        "Pipeline for publishing summarized event information"
    ),
    url="https://github.com/GlobalFishingWatch/pipe-events",
    packages=find_packages(exclude=['test*.*', 'tests']),
    install_requires=[
        'jinja2-cli<1',
        'google-cloud-bigquery<4',
    ],
    entry_points={
        'console_scripts': [
            'pipe = pipe_events.cli:main',
        ]
    },
)
