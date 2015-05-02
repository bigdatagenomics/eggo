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
import re
import random
import string
from hashlib import md5
from datetime import datetime


def random_id(prefix='tmp', n=4):
    dt_string = datetime.now().strftime('%Y-%m-%dT%H-%M-%S')
    rand_string = ''.join(random.sample(string.ascii_uppercase, n))
    return '{pre}_{dt}_{rand}'.format(pre=prefix, dt=dt_string,
                                      rand=rand_string)


def sanitize_filename(dirty):
    # inspired by datacache
    clean = re.sub(r'/|\\|;|:|\?|=', '_', dirty)
    if len(clean) > 150:
        prefix = md5(filename).hexdigest()
        clean = prefix + clean[-140:]
    return clean


def build_dest_filename(download_url, decompress=False):
    # inspired by datacache
    digest = md5(download_url.encode('utf-8')).hexdigest()
    filename = '{digest}.{sanitized_url}'.format(
        digest=digest, sanitized_url=sanitize_filename(download_url))
    if decompress:
        (base, ext) = os.path.splitext(filename)
        if ext == '.gz':
            filename = base
    return filename


def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)
