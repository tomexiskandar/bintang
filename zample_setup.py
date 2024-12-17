# as seen in https://github.com/pypa/sampleproject/blob/master/setup.py

from setuptools import setup, find_packages
from pathlib import Path

BASE_DIR = Path(__file__).parent

# Get the long description from the README file
with open(BASE_DIR / 'README.rst', encoding='utf-8') as f:
    long_description = f.read()

# Arguments marked as "Required" below must be included for upload to PyPI.
# Fields marked as "Optional" may be commented out.

setup(
    name='bintang',  # Required
    description='A tiny and temporary db for quick data cleansing and transformation.',
    keywords='data table tool',  # Optional
    version='0.1.12',
    author='Tomex Iskandar',
    author_email='tomex.iskandar@gmail.com',
    packages=['bintang']
)