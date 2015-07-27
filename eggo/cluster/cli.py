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

from click import group, option, pass_context, pass_obj

import eggo.cluster.director as director
from eggo.cluster.config import (
    DEFAULT_DIRECTOR_CONF_PATH, DEFAULT_CF_TEMPLATE_PATH)

@group(context_settings={'help_option_names': ['-h', '--help']})
@option('--region', default='us-east-1', help='AWS Region')
@option('--cf-stack-name', default='bdg-eggo',
        help='AWS Cloudformation Stack Name')
@pass_context
def cli(ctx, region, cf_stack_name):
    """eggo -- provisions Hadoop clusters in AWS using Cloudera Director."""
    ctx.obj = {'region': region, 'cf_stack_name': cf_stack_name}


@cli.command()
@option('--availability-zone', default='us-east-1b',
        help='AWS Availability Zone')
@option('--cf-template-path', default=DEFAULT_CF_TEMPLATE_PATH,
        help='Path to AWS Cloudformation Template')
@option('--launcher-ami', default='ami-00a11e68',
        help='The AMI to use for the launcher node')
@option('--launcher-instance-type', default='m3.medium',
        help='The instance type to use for the launcher node')
@option('--director-conf-path', default=DEFAULT_DIRECTOR_CONF_PATH,
        help='Path to Director conf for AWS cloud')
@option('--cluster-ami', default='ami-00a11e68',
        help='The AMI to use for the worker nodes')
@option('-n', '--num-workers', default=3,
        help='The total number of worker nodes to provision')
@pass_obj
def provision(obj, availability_zone, cf_template_path,
              launcher_ami, launcher_instance_type,
              director_conf_path, cluster_ami, num_workers):
    """provision a new cluster on AWS"""
    director.provision(
        obj['region'], availability_zone, obj['cf_stack_name'],
        cf_template_path, launcher_ami, launcher_instance_type,
        director_conf_path, cluster_ami, num_workers)


@cli.command()
def setup():
    pass


@cli.command()
@pass_obj
def teardown(obj):
    """tear down a cluster on AWS"""
    director.teardown(obj['region'], obj['cf_stack_name'])


@cli.command()
def login(obj):
    """login to gateway node of cluster"""
    director.login(obj['region'])


@cli.command()
def describe():
    pass
