#! /usr/bin/env python
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

import json
import os.path as osp
from os.path import join as pjoin

from eggo.datasets.operations import (
    download_dataset_with_hadoop, vcf_to_adam_variants, locus_partition,
    distcp)
from eggo.util import make_hdfs_tmp


hdfs_uri = 'hdfs:///user/ec2-user'
s3a_uri = 's3a://bdg-eggo'


raw_data_path = 'dbsnp_raw'
adam_nested_path = 'dbsnp_adam'
adam_flat_path = 'dbsnp_adam_flat'


with open(pjoin(osp.dirname(__file__), 'datapackage.json')) as ip:
    datapackage = json.load(ip)


download_dataset_with_hadoop(datapackage, pjoin(hdfs_uri, raw_data_path))

with make_hdfs_tmp('tmp_dbsnp') as tmp_hdfs_path:
    tmp_adam_variant_path = pjoin(tmp_hdfs_path, 'tmp_adam_variants')
    vcf_to_adam_variants(pjoin(hdfs_uri, raw_data_path),
                         tmp_adam_variant_path)
    locus_partition(tmp_adam_variant_path, pjoin(hdfs_uri, adam_nested_path))
    distcp(pjoin(hdfs_uri, adam_nested_path), pjoin(s3a_uri, adam_nested_path))
