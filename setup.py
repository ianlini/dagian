#!/usr/bin/env python
import os
from setuptools import setup


on_rtd = os.environ.get('READTHEDOCS') == 'True'
# read the docs could not compile numpy and c extensions
if on_rtd:
    setup_requires = []
    install_requires = []
    tests_require = []
else:
    setup_requires = [
        'nose',
        'coverage',
    ]
    install_requires = [
        'six',
        'future',
        'mkdir-p',
        'h5py',
        'bistiming>=0.1.1',
        'numpy',
        'scipy',
        'networkx>=2',
        'pyyaml',
        'tables',
        'pandas',
        'h5sparse>=0.0.4',
    ]
    tests_require = []


description = """\
A data-centric DAG framework."""

long_description = """\
Please visit  the `GitHub repository <https://github.com/ianlini/dagian>`_
for more information.\n
"""
with open('README.rst') as fp:
    long_description += fp.read()


setup(
    name='dagian',
    version="0.0.0",
    description=description,
    long_description=long_description,
    author='Ian Lin',
    url='https://github.com/ianlini/dagian',
    setup_requires=setup_requires,
    install_requires=install_requires,
    tests_require=tests_require,
    license="MIT",
    classifiers=[
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Scientific/Engineering :: Image Recognition',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'License :: OSI Approved :: MIT License',
    ],
    test_suite='nose.collector',
    packages=[
        'dagian',
    ],
)
