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

from fabric.api import local, prefix, run, execute

from eggo.config import eggo_config


def provision():
    provision_cmd = ('{spark_home}/ec2/spark-ec2 -k {ec2_key_pair} '
                     '-i {ec2_private_key_file} -s {slaves} -t {type_} '
                     '-r {region} {zone_arg} {spot_price_arg} '
                     '--copy-aws-credentials launch {stack_name}')
    az = eggo_config.get('spark_ec2', 'availability_zone')
    zone_arg = '--zone {0}'.format(az) if az != '' else ''
    spot_price = eggo_config.get('spark_ec2', 'spot_price')
    spot_price_arg = '--spot-price {0}'.format(spot_price) if spot_price != '' else ''
    interp_cmd = provision_cmd.format(
        spark_home=eggo_config.get('client_env', 'spark_home'),
        ec2_key_pair=eggo_config.get('aws', 'ec2_key_pair'),
        ec2_private_key_file=eggo_config.get('aws', 'ec2_private_key_file'),
        slaves=eggo_config.get('spark_ec2', 'num_slaves'),
        type_=eggo_config.get('spark_ec2', 'instance_type'),
        region=eggo_config.get('spark_ec2', 'region'),
        zone_arg=zone_arg,
        spot_price_arg=spot_price_arg,
        stack_name=eggo_config.get('spark_ec2', 'stack_name'))
    local(interp_cmd)


def get_master_host():
    getmaster_cmd = ('{spark_home}/ec2/spark-ec2 -k {ec2_key_pair} '
                     '-i {ec2_private_key_file} get-master {stack_name}')
    interp_cmd = getmaster_cmd.format(
        spark_home=eggo_config.get('client_env', 'spark_home'),
        ec2_key_pair=eggo_config.get('aws', 'ec2_key_pair'),
        ec2_private_key_file=eggo_config.get('aws', 'ec2_private_key_file'),
        stack_name=eggo_config.get('spark_ec2', 'stack_name'))
    result = local(interp_cmd, capture=True)
    return result.split('\n')[2].strip()


def get_slave_hosts():
    def do():
        with prefix('source /root/spark-ec2/ec2-variables.sh'):
            result = run('echo $SLAVES').split()
        return result
    master = get_master_host()
    return execute(do, hosts=master)[master]


def teardown():
    teardown_cmd = ('{spark_home}/ec2/spark-ec2 -k {ec2_key_pair} '
                    '-i {ec2_private_key_file} destroy {stack_name}')
    interp_cmd = teardown_cmd.format(
        spark_home=eggo_config.get('client_env', 'spark_home'),
        ec2_key_pair=eggo_config.get('aws', 'ec2_key_pair'),
        ec2_private_key_file=eggo_config.get('aws', 'ec2_private_key_file'),
        stack_name=eggo_config.get('spark_ec2', 'stack_name'))
    local(interp_cmd)
