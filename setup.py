# -*- coding: utf-8 -*-

# Learn more: https://github.com/kennethreitz/setup.py
from setuptools import setup

with open('README.md') as f:
    readme = f.read()

setup(
    name='dynamodb-api',
    version='1.3.3',
    description='Wrapper to easily access DynamoDB table',
    long_description=readme,
    author='Max Bird',
    author_email='mhb316@ic.ac.uk',
    url='https://github.com/MaxBird300/dynamodb-api',
    packages=['dynamodb-api'],
    install_requires=[
        "pandas>=1.2.4",
        "numpy>=1.21.2",
        "openpyxl>=3.0.7",
        "boto3==1.17.46",
        "matplotlib>=3.4.3"
        ],
)