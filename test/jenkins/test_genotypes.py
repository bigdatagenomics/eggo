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

import os
from contextlib import contextmanager
from tempfile import mkdtemp
from shutil import rmtree

import parquet

from eggo.config import supported_formats
from eggo.dag import ToastConfig, JsonFileParameter


toast_config = ToastConfig(
    config=JsonFileParameter().parse(
        os.path.join(os.environ['EGGO_HOME'],
                     'test/registry/test-genotypes.json')))


@contextmanager
def dfs_parquet_as_local_csv(fs, path):
    tmp_local_dir = mkdtemp(prefix='tmp_eggo_')
    try:
        local_parquet_path = os.path.join(tmp_local_dir, 'data.parquet')
        local_csv_path = os.path.join(tmp_local_dir, 'data.csv')
        fs.get(path, local_parquet_path)
        with open(local_csv_path, 'w') as op:
            parquet.dump(local_parquet_path)
        yield

    finally:
        rmtree(tmp_local_dir)


def listdir(fs, path):
    r = [(os.path.basename(name), size)
         for (name, size) in fs.listdir(path, include_size=True)]
    return set(r)


def test_dfs_dir_structure(fs):
    assert fs.exists(toast_config.raw_data_url())
    assert fs.exists(toast_config.dataset_url())
    for format in supported_formats:
        for edition in toast_config.config['editions']:
            assert fs.exists(toast_config.edition_url(format=format,
                                                      edition=edition))


def test_raw_dir_contents():
    result_files = listdir(toast_config.raw_data_url())
    assert ('6a2df5dfbf260f9a0ac9c5270fe27c97.https___github.com_'
            'bigdatagenomics_eggo_raw_master_test_resources_chr22.small.vcf',
            2180324) in result_files
    assert ('_SUCCESS', 0) in result_files


def test_etl_dir_structure():
    assert len(listdir(toast_config.dataset_url())) == 1
    assert len(listdir(os.path.join(toast_config.dataset_url(), 'bdg'))) == 2
    bdg_basic = listdir(toast_config.edition_url(format='bdg', edition='basic'))
    bdg_flat = listdir(toast_config.edition_url(format='bdg', edition='flat'))
    assert ('_SUCCESS', 0) in bdg_basic
    assert ('_SUCCESS', 0) in bdg_flat


def test_adam_parquet_data():
