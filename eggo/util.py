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


# This module should have generally applicable utilities, and primarily import
# only standard libraries


import os
import re
import time
import random
import string
import os.path as osp
from os.path import join as pjoin
from uuid import uuid4
from getpass import getuser
from shutil import rmtree
from hashlib import md5
from tempfile import mkdtemp
from datetime import datetime
from subprocess import check_call, Popen, CalledProcessError
from contextlib import contextmanager


def uuid():
    return uuid4().hex


def random_id(prefix='tmp_eggo', n=4):
    dt_string = datetime.now().strftime('%Y-%m-%dT%H-%M-%S')
    rand_string = ''.join(random.sample(string.ascii_uppercase, n))
    return '{pre}_{dt}_{rand}'.format(pre=prefix.rstrip('_', 1), dt=dt_string,
                                      rand=rand_string)


def sleep_progressive(start_time):
    elapsed = (datetime.now() - start_time).seconds
    if elapsed < 30:
        time.sleep(5)
    elif elapsed < 60:
        time.sleep(10)
    elif elapsed < 200:
        time.sleep(20)
    else:
        time.sleep(elapsed / 10.)


def resource_dir():
    return pjoin(osp.dirname(__file__), 'resources')


# ==============
# FILE UTILITIES
# ==============


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


@contextmanager
def make_local_tmp(prefix='tmp_eggo_', dir_=None):
    tmpdir = mkdtemp(prefix=prefix, dir=dir_)
    try:
        yield tmpdir
    finally:
        rmtree(tmpdir)


@contextmanager
def make_hdfs_tmp(prefix='tmp_eggo', dir_='/tmp', permissions='755'):
    tmpdir = pjoin(dir_, '_'.join([prefix, uuid()]))
    check_call('hadoop fs -mkdir {0}'.format(tmpdir).split())
    if permissions != '755':
        check_call(
            'hadoop fs -chmod -R {0} {1}'.format(permissions, tmpdir).split())
    try:
        yield tmpdir
    finally:
        check_call('hadoop fs -rm -r {0}'.format(tmpdir).split())


# ====================
# CONNECTION UTILITIES
# ====================


def non_blocking_tunnel(tunnel_host, remote_host, remote_port, local_port=None,
                        user=None, private_key=None):
    if local_port is None:
        local_port = remote_port
    if user is None:
        user = getuser()
    private_key = '-i {0}'.format(private_key) if private_key else ''
    p = Popen('ssh -nNT {private_key} -o UserKnownHostsFile=/dev/null '
              '-o StrictHostKeyChecking=no -L {local}:{remote_host}:{remote} '
              '{user}@{tunnel_host}'.format(
                  private_key=private_key, tunnel_host=tunnel_host,
                  remote_host=remote_host, user=user, local=local_port,
                  remote=remote_port),
              shell=True)
    return p


@contextmanager
def tunnel_ctx(tunnel_host, remote_host, remote_port, local_port=None,
               user=None, private_key=None):
    tunnel_process = non_blocking_tunnel(tunnel_host, remote_host, remote_port,
                                         local_port, user, private_key)
    # ssh may take a bit to open up the connection, so we wait until we get a
    # successful curl command to the local port
    print('Attempting to curl to SSH tunnel; may take a few attempts')
    start_time = datetime.now()
    while True:
        try:
            check_call('curl http://localhost:{0}'.format(local_port),
                       shell=True)
            # only reach this point if the curl cmd succeeded.
            print('...success!')
            break
        except CalledProcessError:
            sleep_progressive(start_time)
    try:
        yield local_port
    finally:
        tunnel_process.terminate()
