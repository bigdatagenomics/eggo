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

from eggo.datasets import download_dataset_with_hadoop, vcf_to_adam_variants


raw_data_path = '/user/ec2-user/dbsnp/raw'
adam_variants_path = '/user/ec2-user/dbsnp/variants/adam/basic'

with open(pjoin(osp.dirname(__file__), 'datapackage.json')) as ip:
    datapackage = json.load(ip)

download_dataset_with_hadoop(datapackage, raw_data_path)
vcf_to_adam_variants(raw_data_path, adam_variants_path)
