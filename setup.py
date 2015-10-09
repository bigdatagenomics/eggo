# Licensed to Big Data Genomics (BDG) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The BDG licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup, find_packages


def readme():
    with open('README.md', 'r') as ip:
        return ip.read()


setup(
    name='eggo',
    version='0.1.0.dev0',
    description='Pre-formatted Hadoop-friendly public genomics datasets',
    long_description=readme(),
    author='Uri Laserson',
    author_email='laserson@cloudera.com',
    url='https://github.com/bigdatagenomics/eggo',
    packages=find_packages(),
    package_data={'eggo.resources': ['*.template', '*.conf']},
    include_package_data=True,
    install_requires=['fabric', 'boto', 'click', 'cm_api'],
    entry_points={'console_scripts': ['eggo-cluster = eggo.cli.cluster:main',
                                      'eggo-data = eggo.cli.datasets:main']},
    keywords=('bdg adam spark eggo genomics omics public data'),
    license='Apache License, Version 2.0',
    classifiers=[
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7'
    ],
    zip_safe=False)
