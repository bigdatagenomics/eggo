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

import json

from click import group, option, File

from eggo import operations


@group(context_settings={'help_option_names': ['-h', '--help']})
def main():
    """eggo-data -- operations on common genomics datasets"""
    pass


@main.command()
@option('--input', help='Path to datapackage.json file for dataset')
@option('--output', help='Fully-qualified HDFS destination path')
def dnload_raw(input, output):
    """Parallel download raw dataset from datapackage.json using Hadoop"""
    with open(input) as ip:
        datapackage = json.load(ip)
    operations.download_dataset_with_hadoop(datapackage, output)


@main.command()
@option('--cm-host', help='Hostname for Cloudera Manager')
@option('--cm-port', default=7180, show_default=True,
        help='Port for Cloudera Manager')
@option('--username', default='admin', show_default=True, help='CM username')
@option('--password', default='admin', show_default=True, help='CM password')
@option('--output', type=File(mode='w'), default='-', show_default=True,
        help='Output destination ("-" for stdout)')
def gen_env_vars(cm_host, cm_port, username, password, output):
    """Generate env vars required for eggo scripts to run"""
    env_vars = operations.generate_eggo_env_vars(cm_host, cm_port, username,
                                                 password)
    for (k, v) in env_vars.iteritems():
        output.write("export {0}={1}\n".format(k, v))
