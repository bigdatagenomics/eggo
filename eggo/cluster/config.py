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
import os.path as osp

from eggo.error import ConfigError


def get_env_var(name):
    if name not in os.environ:
        raise ConfigError('{0} environment variable is not set'.format(name))
    return os.environ[name]


AWS_ACCESS_KEY_ID = get_env_var('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = get_env_var('AWS_SECRET_ACCESS_KEY')
EC2_KEY_PAIR = get_env_var('EC2_KEY_PAIR')
EC2_PRIVATE_KEY_FILE = get_env_var('EC2_PRIVATE_KEY_FILE')


DEFAULT_DIRECTOR_CONF_PATH = osp.join(osp.dirname(__file__), 'aws.conf')
DEFAULT_CF_TEMPLATE_PATH = osp.join(osp.dirname(__file__),
                                    'cloudformation.template')
