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
from eggo.error import ConfigError


# EGGO CONFIGURATION

def _init_eggo_config():
    defaults = {}
    eggo_config = SafeConfigParser(defaults=defaults,
                                   dict_type=dict)

    # Read the local eggo config file
    with open(os.environ['EGGO_CONFIG'], 'r') as ip:
        eggo_config.readfp(ip, os.environ['EGGO_CONFIG'])

    # Generate the random identifier for this module load
    eggo_config.set('execution', 'random_id', random_id())

    # Set local (client) SPARK_HOME from environment if available
    if 'SPARK_HOME' in os.environ:
        eggo_config.set('client_env',
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


def assert_eggo_config_complete(c):
    # read the "master" config file
    ref = SafeConfigParser(dict_type=dict)
    ref_config_file = os.path.join(os.environ['EGGO_HOME'],
                                   'conf/eggo/eggo.cfg')
    with open(ref_config_file, 'r') as ip:
        ref.readfp(ip, ref_config_file)

    def assert_section_complete(section):
        if not set(ref.options(section)) <= set(c.options(section)):
            raise ConfigError('Config pointed to by EGGO_CONFIG missing '
                              'options in section {0}'.format(section))

    assert_section_complete('dfs')
    assert_section_complete('execution')
    assert_section_complete('versions')
    assert_section_complete('client_env')
    assert_section_complete('worker_env')
    exec_ctx = c.get('execution', 'context')
    assert_section_complete(exec_ctx)


def validate_eggo_config(c):
    if (c.get('dfs', 'dfs_root_url').startswith('file:') and
            c.get('execution', 'context') != 'local'):
        raise ConfigError('Using local fs as target is only supported with '
                          'local execution')


eggo_config = _init_eggo_config()
# raise a ConfigError if there is a problem:
assert_eggo_config_complete(eggo_config)
validate_eggo_config(eggo_config)

supported_formats = ['bdg']  # # TODO: support ga4gh


def generate_luigi_cfg():
    cfg = ('[core]\n'
           'logging_conf_file:{work_path}/eggo/conf/luigi/luigi_logging.cfg\n'
           '[hadoop]\n'
           'command: hadoop\n')
    return cfg.format(work_path=eggo_config.get('worker_env', 'work_path'))


# TOAST CONFIGURATION

def validate_toast_config(d):
    """Validate a JSON config file for an eggo dataset (a "toast")."""
    pass
