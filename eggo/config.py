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

from eggo.util import random_id

# path to store raw input data
RAW_DATA_KEY_PREFIX = 'raw'
# each module load/invocation will generate a new temp location in the distributed fs
TMP_DATA_KEY_PREFIX = random_id()

EGGO_BASE_URL = os.environ.get('EGGO_BASE_URL', 's3n://bdg-eggo')
EGGO_TMP_URL = os.path.join(EGGO_BASE_URL, TMP_DATA_KEY_PREFIX)
EGGO_RAW_URL = os.path.join(EGGO_BASE_URL, RAW_DATA_KEY_PREFIX)


def validate_config(d):
    """Validate a JSON config file for an eggo dataset"""
    pass
