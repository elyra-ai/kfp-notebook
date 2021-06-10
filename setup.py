#!/usr/bin/env python
#
# Copyright 2018-2021 Elyra Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

requirements = [
    'click>=6.0',
    'bumpversion>=0.5.3',
    'wheel>=0.30.0',
    'watchdog>=0.8.3',
    'flake8>=3.5.0,<3.9.0',
    'tox>=2.9.1',
    'coverage>=4.5.1',
    'twine>=1.10.0',
    'kfp==1.4.0',
]

setup_requirements = [ ]

test_requirements = [ ]

setup(
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Jupyter Notebook operator for Kubeflow Pipelines",
    long_description=readme,
    long_description_content_type='text/markdown',
    install_requires=requirements,
    license='Apache License, Version 2.0',
    include_package_data=True,
    keywords='jupyter, kubeflow, pipeline',
    name='kfp-notebook',
    packages=find_packages(),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/elyra-ai/kfp-notebook',
    version='0.24.0',
    zip_safe=False,
)
