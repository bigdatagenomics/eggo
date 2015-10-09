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
import sys
import time
import random
import string
import os.path as osp
from os.path import join as pjoin
from getpass import getuser
from uuid import uuid4
from shutil import rmtree
from hashlib import md5
from tempfile import mkdtemp
from datetime import datetime
from subprocess import check_call, Popen, CalledProcessError
from contextlib import contextmanager

import boto.ec2
import boto.cloudformation
from boto.exception import BotoServerError

from eggo.config import get_ec2_key_pair, get_ec2_private_key_file


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


# =============
# AWS UTILITIES
# =============


def non_blocking_tunnel(instance, remote_port, local_port=None, user='ec2-user',
                        private_key=None):
    if local_port is None:
        local_port = remote_port
    if private_key is None:
        private_key = get_ec2_private_key_file()
    p = Popen('ssh -nNT -i {private_key} -o UserKnownHostsFile=/dev/null '
              '-o StrictHostKeyChecking=no -L {local}:{private_ip}:{remote} '
              '{user}@{public_ip}'.format(
                  private_key=private_key, public_ip=instance.ip_address,
                  private_ip=instance.private_ip_address, user=user,
                  local=local_port, remote=remote_port),
              shell=True)
    return p


@contextmanager
def http_tunnel_ctx(instance, remote_port, local_port=None, user='ec2-user',
                    private_key=None):
    tunnel_process = non_blocking_tunnel(instance, remote_port, local_port,
                                         user, private_key)
    # ssh may take a bit to open up the connection, so we wait until we get a
    # successful curl command to the local port
    print('Attempting to curl to SSH tunnel; may take a few attempts\n')
    start_time = datetime.now()
    while True:
        try:
            check_call('curl http://localhost:{0}'.format(local_port),
                       shell=True)
            # only reach this point if the curl cmd succeeded.
            break
        except CalledProcessError:
            sleep_progressive(start_time)
    try:
        yield
    finally:
        tunnel_process.terminate()


# CLOUDFORMATION UTIL


def wait_for_stack_status(cf_conn, stack_name, stack_status):
    sys.stdout.write(
        "Waiting for stack to enter '{s}' state.".format(s=stack_status))
    sys.stdout.flush()
    start_time = datetime.now()
    while True:
        sleep_progressive(start_time)
        stack = cf_conn.describe_stacks(stack_name)[0]
        if stack.stack_status == stack_status:
            break
        sys.stdout.write(".")
        sys.stdout.flush()
    sys.stdout.write("\n")
    end_time = datetime.now()
    print "Stack is now in '{s}' state. Waited {t} seconds.".format(
        s=stack_status, t=(end_time - start_time).seconds)


def create_cf_connection(region):
    return boto.cloudformation.connect_to_region(region)


def create_cf_stack(cf_conn, stack_name, cf_template_path, availability_zone):
    try:
        if len(cf_conn.describe_stacks(stack_name)) > 0:
            print "Stack '{n}' already exists. Reusing.".format(n=stack_name)
            return
    except BotoServerError:
        # stack does not exist
        pass

    print "Creating stack with name '{n}'.".format(n=stack_name)
    with open(cf_template_path, 'r') as template_file:
        template_body=template_file.read()
    cf_conn.create_stack(stack_name, template_body=template_body,
                         parameters=[('KeyPairName', get_ec2_key_pair()),
                                     ('AZ', availability_zone)],
                         tags={'owner': getuser(),
                               'ec2_key_pair': get_ec2_key_pair()})
    wait_for_stack_status(cf_conn, stack_name, 'CREATE_COMPLETE')


def get_stack_resource_id(cf_conn, stack_name, logical_resource_id):
    for resource in cf_conn.describe_stack_resources(stack_name):
        if resource.logical_resource_id == logical_resource_id:
            return resource.physical_resource_id
    return None


def get_subnet_id(cf_conn, stack_name):
    return get_stack_resource_id(cf_conn, stack_name, 'DMZSubnet')


def get_security_group_id(cf_conn, stack_name):
    return get_stack_resource_id(cf_conn, stack_name, 'ClusterSG')


def delete_stack(cf_conn, stack_name):
    print "Deleting stack with name '{n}'.".format(n=stack_name)
    cf_conn.delete_stack(stack_name)
    wait_for_stack_status(cf_conn, stack_name, 'DELETE_COMPLETE')


# EC2 UTIL


def create_ec2_connection(region):
    return boto.ec2.connect_to_region(region)


def get_tagged_instances(ec2_conn, tags):
    filters = [('tag:' + k, v) for (k, v) in tags.iteritems()]
    instances = ec2_conn.get_only_instances(filters=filters)
    return [i for i in instances
            if i.state not in ["shutting-down", "terminated"]]


def get_launcher_instance(ec2_conn, stack_name):
    return get_tagged_instances(ec2_conn, {'eggo_stack_name': stack_name,
                                           'eggo_node_type': 'launcher'})[0]


def get_manager_instance(ec2_conn, stack_name):
    return get_tagged_instances(ec2_conn, {'eggo_stack_name': stack_name,
                                           'eggo_node_type': 'manager'})[0]


def get_master_instance(ec2_conn, stack_name):
    return get_tagged_instances(ec2_conn, {'eggo_stack_name': stack_name,
                                           'eggo_node_type': 'master'})[0]


def get_worker_instances(ec2_conn, stack_name):
    return get_tagged_instances(ec2_conn, {'eggo_stack_name': stack_name,
                                           'eggo_node_type': 'worker'})


def wait_for_instance_state(ec2_conn, instance, state='running'):
    sys.stdout.write(
        "Waiting for instance to enter '{s}' state.".format(s=state))
    sys.stdout.flush()
    start_time = datetime.now()
    while True:
        sleep_progressive(start_time)
        instance.update()
        statuses = ec2_conn.get_all_instance_status(instance.id,
                                                    include_all_instances=True)
        if len(statuses) > 0:
            status = statuses[0]
            if (instance.state == state and
                    status.system_status.status in ['ok', 'not-applicable'] and
                    status.instance_status.status in ['ok', 'not-applicable']):
                break
        sys.stdout.write(".")
        sys.stdout.flush()
    sys.stdout.write("\n")
    end_time = datetime.now()
    print "Instance is now in '{s}' state. Waited {t} seconds.".format(
        s=state, t=(end_time - start_time).seconds)
