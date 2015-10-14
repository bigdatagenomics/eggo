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
import json
from os.path import join as pjoin
from subprocess import check_call

from cm_api.api_client import ApiResource

from eggo.util import make_local_tmp, make_hdfs_tmp
from eggo.compat import check_output


# This module includes operations to be performed on an actual Hadoop cluster
# to operate on data.  The functionality here can be used in scripts for ETLing
# data sets


STREAMING_JAR = ('/opt/cloudera/parcels/CDH-*/lib/hadoop-mapreduce/'
                 'hadoop-streaming.jar')


def download_dataset_with_hadoop(datapackage, hdfs_path):
    with make_local_tmp() as tmp_local_dir:
        with make_hdfs_tmp(permissions='777') as tmp_hdfs_dir:
            # NOTE: 777 used so user yarn can write to this dir
            # create input file for MR job that downloads the files and puts
            # them in HDFS
            local_resource_file = pjoin(tmp_local_dir, 'resource_file.txt')
            with open(local_resource_file, 'w') as op:
                for resource in datapackage['resources']:
                    op.write('{0}\n'.format(json.dumps(resource)))
            check_call('hadoop fs -put {0} {1}'.format(local_resource_file,
                                                       tmp_hdfs_dir),
                       shell=True)

            # construct and execute hadoop streaming command to initiate dnload
            cmd = ('hadoop jar {streaming_jar} '
                   '-D mapreduce.job.reduces=0 '
                   '-D mapreduce.map.speculative=false '
                   '-D mapreduce.task.timeout=12000000 '
                   '-files {mapper_script_path} '
                   '-input {resource_file} -output {dummy_output} '
                   '-mapper {mapper_script_name} '
                   '-inputformat {input_format} -outputformat {output_format} '
                   '-cmdenv STAGING_PATH={staging_path} ')
            args = {'streaming_jar': STREAMING_JAR,
                    'resource_file': pjoin(tmp_hdfs_dir, 'resource_file.txt'),
                    'dummy_output': pjoin(tmp_hdfs_dir, 'dummy_output'),
                    'mapper_script_name': 'download_mapper.py',
                    'mapper_script_path': pjoin(
                        os.path.dirname(__file__), 'resources',
                        'download_mapper.py'),
                    'input_format': (
                        'org.apache.hadoop.mapred.lib.NLineInputFormat'),
                    'output_format': (
                        'org.apache.hadoop.mapred.lib.NullOutputFormat'),
                    'staging_path': pjoin(tmp_hdfs_dir, 'staging')}
            print(cmd.format(**args))
            check_call(cmd.format(**args), shell=True)

            # move dnloaded data to final path
            check_call('hadoop fs -mkdir -p {0}'.format(hdfs_path), shell=True)
            check_call(
                'sudo -u hdfs hadoop fs -chown -R ec2-user:supergroup {0}'
                .format(tmp_hdfs_dir), shell=True)
            check_call(
                'hadoop fs -mv "{0}/*" {1}'.format(
                    pjoin(tmp_hdfs_dir, 'staging'), hdfs_path), shell=True)


def get_parquet_avro_schema(path):
    cmd = 'hadoop jar parquet-tools-*.jar meta {0}'.format(path)
    print(cmd)
    raw1 = check_output(cmd, shell=True)
    raw2 = filter(lambda x: x.startswith('extra:'), raw1.split('\n'))[0]
    match = re.match(r'^extra:\s*parquet.avro.schema = (.*)', raw2)
    schema = match.group(1)
    print(schema)
    return schema


def get_cluster_info(manager_host, server_port=7180, username='admin',
                     password='admin'):
    cm_api = ApiResource(manager_host, username=username, password=password,
                         server_port=server_port, version=9)
    host = list(cm_api.get_all_hosts())[0]  # all hosts same instance type
    cluster = list(cm_api.get_all_clusters())[0]
    yarn = filter(lambda x: x.type == 'YARN',
                  list(cluster.get_all_services()))[0]
    hive = filter(lambda x: x.type == 'HIVE',
                  list(cluster.get_all_services()))[0]
    impala = filter(lambda x: x.type == 'IMPALA',
                    list(cluster.get_all_services()))[0]
    hive_hs2 = hive.get_roles_by_type('HIVESERVER2')[0]
    hive_host = cm_api.get_host(hive_hs2.hostRef.hostId).hostname
    hive_port = int(
        hive_hs2.get_config('full')['hs2_thrift_address_port'].default)
    impala_hs2 = impala.get_roles_by_type('IMPALAD')[0]
    impala_host = cm_api.get_host(impala_hs2.hostRef.hostId).hostname
    impala_port = int(impala_hs2.get_config('full')['hs2_port'].default)
    return {'num_worker_nodes': len(yarn.get_roles_by_type('NODEMANAGER')),
            'node_cores': host.numCores, 'node_memory': host.totalPhysMemBytes,
            'hive_host': hive_host, 'hive_port': hive_port,
            'impala_host': impala_host, 'impala_port': impala_port}


def generate_eggo_env_vars(cm_host, cm_port=7180, username='admin',
                           password='admin'):
    info = get_cluster_info(cm_host, cm_port, username, password)
    cores_per_executor = min(4, info['node_cores'])
    executors_per_node = info['node_cores'] / cores_per_executor
    total_executors = executors_per_node * info['num_worker_nodes']
    memory_per_executor = int(0.8 * info['node_memory'] / executors_per_node)
    return {'NUM_WORKER_NODES': str(info['num_worker_nodes']),
            'NODE_CORES': str(info['node_cores']),
            'NODE_MEMORY': str(info['node_memory']),
            'CORES_PER_EXECUTOR': str(cores_per_executor),
            'EXECUTORS_PER_NODE': str(executors_per_node),
            'TOTAL_EXECUTORS': str(total_executors),
            'MEMORY_PER_EXECUTOR': str(memory_per_executor)}
