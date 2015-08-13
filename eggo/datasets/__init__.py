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
import os
from os.path import join as pjoin
from subprocess import check_call

from eggo.util import make_local_tmp, make_hdfs_tmp


STREAMING_JAR = ('/opt/cloudera/parcels/CDH-5.3.3-1.cdh5.3.3.p0.5/jars/'
                 'hadoop-streaming-2.5.0-cdh5.3.3.jar')


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
            check_call('hadoop fs -put {0} {1}'.format(
                           local_resource_file, tmp_hdfs_dir),
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
                        os.path.dirname(__file__), '_scripts',
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
                'sudo -u hdfs hadoop fs -chown -R ec2-user:supergroup {0}'.format(
                    tmp_hdfs_dir), shell=True)
            check_call(
                'hadoop fs -mv "{0}/*" {1}'.format(
                    pjoin(tmp_hdfs_dir, 'staging'), hdfs_path), shell=True)
