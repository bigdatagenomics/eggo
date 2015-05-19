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

"""Adds "owner" tags to instances with tag:stack_name matching eggo config."""

import sys
from getpass import getuser

from boto.ec2 import connect_to_region

from eggo.config import eggo_config


exec_ctx = eggo_config.get('execution', 'context')
if exec_ctx == 'local':
    sys.exit()
conn = connect_to_region(eggo_config.get(exec_ctx, 'region'))
user = getuser()
instances = conn.get_only_instances(
    filters={'tag:stack_name': [eggo_config.get(exec_ctx, 'stack_name')]})
for instance in instances:
    print instance
    instance.add_tag('owner', user)
