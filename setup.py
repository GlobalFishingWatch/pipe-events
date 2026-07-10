#!/usr/bin/env python

"""
Setup script for pipe-orbcomm
"""

from setuptools import find_packages
from setuptools import setup

setup(
    name='pipe-events',
    version='5.0.0',
    author="Global Fishing Watch.",
    description=(
        "Pipeline for publishing summarized event information"
    ),
    url="https://github.com/GlobalFishingWatch/pipe-events",
    packages=find_packages(exclude=['test*.*', 'tests']),
    install_requires=[
        'jinja2~=3.1',
        'google-cloud-bigquery~=3.26',
    ],
    extras_require={
        "dev": [
            "pip-tools",
            "flake8",
        ],
    },
    entry_points={
        'console_scripts': [
            'pipe-events = pipe_events.cli:main',
        ]
    },
)
