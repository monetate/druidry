"""Installation script."""
from setuptools import setup, find_packages

# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

# Get the current version from the VERSION file
with open(path.join(here, "src", "druidry", "VERSION")) as version_file:
    version = version_file.read().strip()

setup(
    name='druidry',
    version=version,
    description='Python bindings for building and executing Druid queries.',
    long_description=long_description,
    url="https://github.com/monetate/druidry",
    author='Monetate, Inc.',
    packages=find_packages("src"),
    package_dir={"": "src"},
    include_package_data=True,
    install_requires=[
        'decorator>=4.1.2',
        'isodate>=0.5.1',
        'pytz>=2013',
        'requests>=1.2.3'
    ],
    extras_require={
        'results': [
            'pandas>=0.12.0,<0.21'  # our code breaks somewhere above 0.20, tests catch the errors
        ],
        'doc': [
            "Sphinx==1.4",
            "boto==2.40.0",
            "livereload==2.3.2",
            "sphinx-argparse==0.1.14",
            "sphinx-autobuild==0.5.0",
            "sphinx-rtd-theme==0.1.9",
        ],
        'test': [
            'mock>=0.8.0'
        ]
    }
)
