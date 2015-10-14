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


# This module should define the AWS config required by the eggo-director CLI


import os

from eggo.error import ConfigError


def _get_env_var(name):
    if name not in os.environ:
        raise ConfigError('{0} environment variable is not set'.format(name))
    return os.environ[name]


def get_aws_access_key_id():
    return _get_env_var('AWS_ACCESS_KEY_ID')


def get_aws_secret_access_key():
    return _get_env_var('AWS_SECRET_ACCESS_KEY')


def get_ec2_key_pair():
    return _get_env_var('EC2_KEY_PAIR')


def get_ec2_private_key_file():
    return _get_env_var('EC2_PRIVATE_KEY_FILE')
