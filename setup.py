import os
import setuptools

_here = os.path.abspath(os.path.dirname(__file__))

with open("README.md", "r") as fh:
    long_description = fh.read()

version = {}
with open(os.path.join(_here, 'db_toolkit', 'version.py')) as f:
    exec(f.read(), version)

setuptools.setup(
    name="db_toolkit",
    version=version['__version__'],
    author="Ian Buttimer",
    author_email="author@example.com",
    description="Database utility functions/classes",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ib-da-ncirl/db_toolkit",
    license='MIT',
    packages=setuptools.find_packages(),
    install_requires=[
      'azure-cosmos>=3.1.2',
      'psycopg2>=2.8.4',
      'pymongo>=3.9.0'
    ],
    tests_require=['testfixtures>=6.10.2'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
