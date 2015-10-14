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


# This module contains custom utilities for working with AWS services,
# including EC2, S3, and CloudFormation.  The functions here tend to use boto,
# or expect boto objects.  They shouldn't know anything about Cloudera
# Director.


import sys
from datetime import datetime
from getpass import getuser

import boto.ec2
import boto.cloudformation
from boto.exception import BotoServerError

from eggo.util import sleep_progressive
from eggo.config import get_ec2_key_pair


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
        template_body = template_file.read()
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
