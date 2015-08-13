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

# standard lib only makes it easier to run

import re
import os
import sys
import json
from subprocess import check_call
from os.path import join as pjoin
from hashlib import md5


def sanitize(dirty):
    # for sanitizing URIs/filenames
    # inspired by datacache
    clean = re.sub(r'/|\\|;|:|\?|=', '_', dirty)
    if len(clean) > 150:
        prefix = md5(dirty).hexdigest()
        clean = prefix + clean[-114:]
    return clean


def uri_to_sanitized_filename(source_uri, decompress=False):
    # inspired by datacache
    digest = md5(source_uri.encode('utf-8')).hexdigest()
    filename = '{digest}.{sanitized_uri}'.format(
        digest=digest, sanitized_uri=sanitize(source_uri))
    if decompress:
        (base, ext) = os.path.splitext(filename)
        if ext == '.gz':
            filename = base
    return filename


for line in sys.stdin:
    resource = json.loads(line.split('\t', 1)[1])
    
    # compute dest filename
    staging_path = os.environ['STAGING_PATH']
    decompress = resource['compression'] in ['gzip']
    dest_name = uri_to_sanitized_filename(resource['url'],
                                          decompress=decompress)
    dest_path = pjoin(staging_path, dest_name)
    
    # construct dnload cmd (straight into HDFS)
    pipeline = ['curl -L {0}'.format(resource['url'])]
    if resource['compression'] == 'gzip':
        pipeline.append('gunzip')
    pipeline.append('hadoop fs -put - {0}'.format(dest_path))

    # ensure staging path exists
    check_call('hadoop fs -mkdir -p {0}'.format(staging_path), shell=True)

    # execute dnload
    cmd = ' | '.join(pipeline)
    check_call(cmd, shell=True)

    # dummy output
    sys.stdout.write('{0}\t1\n'.format(dest_path))
