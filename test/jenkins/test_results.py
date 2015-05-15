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

from eggo.config import eggo_config, supported_formats
from eggo.dag import ToastConfig, JsonFileParameter


def test_config():
    pass


def test_genotypes(fs):
    def listdir(path):
        r = [(os.path.basename(name), size)
             for (name, size) in fs.listdir(path, include_size=True)]
        return set(r)

    toast_config = ToastConfig(config=JsonFileParameter().parse(
        os.path.join(os.environ['EGGO_HOME'],
                     'test/registry/test-genotypes.json')))

    # TODO: should all these assertions be broken into their own individual tests?

    # check that all expected DFS directories exist
    assert fs.exists(toast_config.raw_data_url())
    assert fs.exists(toast_config.dataset_url())
    for format in supported_formats:
        for edition in toast_config.config['editions']:
            assert fs.exists(toast_config.edition_url(format=format,
                                                      edition=edition))

    # check for expected results in the raw/ subdir
    result_files = listdir(toast_config.raw_data_url())
    assert ('6a2df5dfbf260f9a0ac9c5270fe27c97.https___github.com_'
            'bigdatagenomics_eggo_raw_master_test_resources_chr22.small.vcf',
            2180324) in result_files
    assert ('_SUCCESS', 0) in result_files

    # check for expected ETL'd results
    assert len(listdir(toast_config.dataset_url())) == 1
    assert len(listdir(os.path.join(toast_config.dataset_url(), 'bdg'))) == 2

    bdg_basic = listdir(toast_config.edition_url(format='bdg', edition='basic'))
    assert ('part-r-00000.gz.parquet', 124989) in bdg_basic
    assert ('_SUCCESS', 0) in bdg_basic
    bdg_flat = listdir(toast_config.edition_url(format='bdg', edition='flat'))
    assert ('part-r-00000.gz.parquet', 22035) in bdg_flat
    assert ('_SUCCESS', 0) in bdg_flat
    # TODO: test for _metadata and _common_metadata files?


def test_alignments():
    pass
