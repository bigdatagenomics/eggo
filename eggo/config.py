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
from ConfigParser import SafeConfigParser

from eggo.util import random_id


# EGGO CONFIGURATION

def _init_eggo_config():
    defaults = {}
    eggo_config = SafeConfigParser(defaults=defaults,
                                   dict_type=dict)

    # Read the local eggo config file
    with open(os.environ['EGGO_CONFIG'], 'r') as ip:
        eggo_config.readfp(ip, os.environ['EGGO_CONFIG'])

    # Generate the random identifier for this module load
    eggo_config.set('paths', 'random_id', random_id())

    # Set local SPARK_HOME from environment if available
    if 'SPARK_HOME' in os.environ:
        eggo_config.set('local_env',
                        'spark_home',
                        os.environ['SPARK_HOME'])

    # Set AWS variables from environment if available
    if 'AWS_ACCESS_KEY_ID' in os.environ:
        eggo_config.set('aws',
                        'aws_access_key_id',
                        os.environ['AWS_ACCESS_KEY_ID'])
    if 'AWS_SECRET_ACCESS_KEY' in os.environ:
        eggo_config.set('aws',
                        'aws_secret_access_key',
                        os.environ['AWS_SECRET_ACCESS_KEY'])
    if 'EC2_KEY_PAIR' in os.environ:
        eggo_config.set('aws',
                        'ec2_key_pair',
                        os.environ['EC2_KEY_PAIR'])
    if 'EC2_PRIVATE_KEY_FILE' in os.environ:
        eggo_config.set('aws',
                        'ec2_private_key_file',
                        os.environ['EC2_PRIVATE_KEY_FILE'])

    return eggo_config


eggo_config = _init_eggo_config()


# TOAST CONFIGURATION

def validate_toast_config(d):
    """Validate a JSON config file for an eggo dataset (a "toast")."""
    pass
