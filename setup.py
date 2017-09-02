from setuptools import setup, find_packages
from codecs import open
import os
import re

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, "pysdbd", "__init__.py")) as fh:
    version = re.match(r".*__version__ = \"(.*?)\"", fh.read(),re.S).group(1)

setup(
    name="pysdbd",
    version=version,
    description="database abstraction API",
    url="https://github.com/lschw/pysdbd",
    author="Lukas Schwarz",
    author_email="ls@lukasschwarz.de",
    license="GPLv3",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)"
        "Programming Language :: Python :: 2.7"
        "Programming Language :: Python :: 3.6"
    ],
    packages=["pysdbd"],
)

