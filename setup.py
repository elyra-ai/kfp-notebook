#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

requirements = [
    'click>=6.0',
    'bumpversion>=0.5.3',
    'wheel>=0.30.0',
    'watchdog>=0.8.3',
    'flake8>=3.5.0',
    'tox>=2.9.1',
    'coverage>=4.5.1',
    'twine>=1.10.0',
    'kfp'
]

setup_requirements = [ ]

test_requirements = [ ]

setup(
    author="IBM - CODAIT",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    description="Jupyter Notebook operator for Kubeflow Pipeline.",
    install_requires=requirements,
    license='Apache License, Version 2.0',
    long_description=readme,
    include_package_data=True,
    keywords='jupyter, kubeflow, pipeline',
    name='kfp-notebook',
    packages=[
        'notebook.pipeline',
    ],
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.ibm.com/ai-workspace/kfp-notebook',
    version='0.1.0.dev',
    zip_safe=False,
)
