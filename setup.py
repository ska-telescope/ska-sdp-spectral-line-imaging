#!/usr/bin/env python
# -*- coding: utf-8 -*-

import setuptools
from setuptools import setup

with open('README.md') as readme_file:
    readme = readme_file.read()

setup(
    name='spectral_line_imaging',
    version='0.0.1',
    description="",
    long_description=readme + '\n\n',
    author="DHRUVA",
    author_email='DHRUVA-team@SKA',
    url='https://gitlab.com/ska-telescope/sdp/ska-sdp-spectral-line-imaging',
    packages=setuptools.find_namespace_packages(
        where="src", include=["ska.*"]
    ), package_dir={"spectral_line_imaging": "src"},
    include_package_data=True,
    license="BSD license",
    zip_safe=False,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.10',
    ],
    test_suite='tests',
    install_requires=[
        
    ],  # FIXME: add your package's dependencies to this list
    setup_requires=[
        # dependency for `python setup.py test`
        'pytest-runner',
        # dependencies for `python setup.py build_sphinx`
        'sphinx',
        'recommonmark'
    ],
    tests_require=[
        'pytest',
        'pytest-cov',
        'pytest-json-report',
        'pycodestyle',
    ],
    extras_require={
        'dev':  ['prospector[with_pyroma]', 'yapf', 'isort'],
    }
)
