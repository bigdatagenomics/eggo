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

"""Luigi tasks etc for implementing ADAM ETL jobs."""

import os
import sys
import json
from time import sleep
from shutil import rmtree
from tempfile import mkdtemp
from subprocess import Popen

from luigi import Task
from luigi.s3 import S3Target, S3FlagTarget, S3Client
from luigi.parameter import Parameter

from eggo.config import (
    validate_config, EGGO_S3_BUCKET_URL, EGGO_S3N_BUCKET_URL, EGGO_S3_RAW_URL,
    EGGO_S3N_RAW_URL, EGGO_S3_TMP_URL)
from eggo.util import random_id, build_s3_filename

def create_SUCCESS_file(s3_path):
    s3client = S3Client(os.environ['AWS_ACCESS_KEY_ID'],
                        os.environ['AWS_SECRET_ACCESS_KEY'])
    s3client.put_string('', os.path.join(s3_path, '_SUCCESS'))


class ConfigParameter(Parameter):

    def parse(self, p):
        with open(p, 'r') as ip:
            json_data = json.load(ip)
        validate_config(json_data)
        return json_data


class DownloadFileTask(Task):
    """Download a file, decompress, and move to S3."""

    source = Parameter()  # string: URL suitable for curl
    target = Parameter()  # string: full S3 path of destination file name
    compression = Parameter()  # bool: whether file needs to be decompressed

    def run(self):
        try:
            EPHEMERAL_MOUNT = os.environ.get('EPHEMERAL_MOUNT', '/mnt')
            tmp_dir = mkdtemp(prefix='tmp_eggo_', dir=EPHEMERAL_MOUNT)

            # 1. dnload file
            dnload_cmd = 'pushd {tmp_dir} && curl -s -L -O {source} && popd'
            p = Popen(dnload_cmd.format(tmp_dir=tmp_dir, source=self.source),
                      shell=True)
            p.wait()

            # 2. decompress if necessary
            if self.compression:
                compression_type = os.path.splitext(self.source)[-1]
                if compression_type == '.gz':
                    decompr_cmd = ('pushd {tmp_dir} && gunzip *.gz && popd')
                else:
                    raise ValueError("Unknown compression type: {0}".format(
                        compression_type))
                p = Popen(decompr_cmd.format(tmp_dir=tmp_dir), shell=True)
                p.wait()

            # 3. upload to tmp S3 location
            tmp_s3_path = os.path.join(EGGO_S3_TMP_URL, random_id())
            upload_cmd = 'pushd {tmp_dir} && aws s3 cp ./* {s3_path} && popd'
            p = Popen(upload_cmd.format(tmp_dir=tmp_dir, s3_path=tmp_s3_path),
                      shell=True)
            p.wait()

            # 4. rename to final target location
            rename_cmd = 'aws s3 mv {tmp_path} {final_path}'
            p = Popen(rename_cmd.format(tmp_path=tmp_s3_path,
                                        final_path=self.target),
                      shell=True)
            p.wait()
        except:
            raise
        finally:
            rmtree(tmp_dir)

    def output(self):
        return S3Target(path=self.target)


class DownloadDatasetTask(Task):

    config = ConfigParameter()
    destination = Parameter()  # full S3 prefix to put data

    def requires(self):
        for source in self.config['sources']:
            dest_name = build_s3_filename(source['url'],
                                          decompress=source['compression'])
            yield DownloadFileTask(
                source=source['url'],
                target=os.path.join(self.destination, dest_name),
                compression=source['compression'])

    def run(self):
        create_SUCCESS_file(self.destination)

    def output(self):
        return S3FlagTarget(self.destination)


class DownloadDatasetParallelTask(Task):

    config = ConfigParameter()
    destination = Parameter()  # full S3 prefix to put data

    def run(self):
        try:
            EPHEMERAL_MOUNT = os.environ.get('EPHEMERAL_MOUNT', '/mnt')
            tmp_dir = mkdtemp(prefix='tmp_eggo_', dir=EPHEMERAL_MOUNT)

            s3client = S3Client(os.environ['AWS_ACCESS_KEY_ID'],
                                os.environ['AWS_SECRET_ACCESS_KEY'])

            # 1. determine which files need to be dnloaded and which already exist
            sources_to_download = []
            for source in self.config['sources']:
                dest_name = build_s3_filename(source['url'],
                                              decompress=source['compression'])
                dest_url = os.path.join(self.destination, dest_name)
                if not s3client.exists(dest_url):
                    sources_to_download.append(source)

            sys.stderr.write("Sources to download:\n")
            for s in sources_to_download:
                sys.stderr.write("    {0}\n".format(s['url']))
            sys.stderr.flush()

            if len(sources_to_download) == 0:
                create_SUCCESS_file(self.destination)
                return

            # 2. build the remote command for each source
            tmp_command_file = '{0}/command_file'.format(tmp_dir)
            with open(tmp_command_file, 'w') as command_file:
                for source in sources_to_download:
                    # compute some parameters for the download
                    tmp_s3_path = os.path.join(EGGO_S3_TMP_URL, random_id())
                    dest_name = build_s3_filename(source['url'],
                                                  decompress=source['compression'])
                    dest_url = os.path.join(self.destination, dest_name)
                    if not source['compression']:
                        compression_type = 'NONE'
                    else:
                        compression_ext = os.path.splitext(source['url'])[-1]
                        if compression_ext == '.gz':
                            compression_type = 'GZIP'
                        else:
                            raise ValueError("Unknown compression type: {0}".format(
                                compression_ext))
                    command_file.write(('{ephem} {source} {compress} {tmp_s3_path} '
                                        '{final_path}\n').format(
                                      ephem=EPHEMERAL_MOUNT, source=source['url'],
                                      compress=compression_type,
                                      tmp_s3_path=tmp_s3_path, final_path=dest_url))

            # 3. Copy command file to Hadoop filesystem
            hadoop_tmp_command_file = mkdtemp(prefix='tmp_eggo_', dir='/tmp')
            copy_cmd = '{hadoop_home}/bin/hadoop fs -put {source} {target}'.format(
                hadoop_home=os.environ['HADOOP_HOME'], source=tmp_command_file,
                target=hadoop_tmp_command_file)
            p = Popen(copy_cmd, shell=True)
            p.wait()

            # 4. Run streaming job to download files in parallel
            streaming_cmd = '{hadoop_home}/bin/hadoop jar {streaming_jar}' \
                            '  -D mapred.reduce.tasks=0' \
                            '  -D mapred.map.tasks.speculative.execution=false' \
                            '  -D mapred.task.timeout=12000000' \
                            '  -input {input}' \
                            '  -inputformat org.apache.hadoop.mapred.lib.NLineInputFormat' \
                            '  -output {output}' \
                            '  -outputformat org.apache.hadoop.mapred.lib.NullOutputFormat' \
                            '  -mapper bin/download_upload_mapper.sh' \
                            '  -file bin/download_upload_mapper.sh'.format(
                hadoop_home=os.environ['HADOOP_HOME'],
                streaming_jar=os.environ['STREAMING_JAR'],
                input=hadoop_tmp_command_file, output=self.destination.replace('s3:', 's3n:'))
            p = Popen(streaming_cmd, shell=True)
            p.wait()

        except:
            raise
        finally:
            rmtree(tmp_dir)

    def output(self):
        return S3FlagTarget(self.destination)

class VCF2ADAMTask(Task):

    config = ConfigParameter()

    def _raw_data_s3_url(self):
        return os.path.join(EGGO_S3_RAW_URL, self.config['name']) + '/'

    def _raw_data_s3n_url(self):
        return os.path.join(EGGO_S3N_RAW_URL, self.config['name']) + '/'

    def _target_s3_url(self):
        return os.path.join(EGGO_S3_BUCKET_URL, self.config['target']) + '/'

    def _target_s3n_url(self):
        return os.path.join(EGGO_S3N_BUCKET_URL, self.config['target']) + '/'

    def requires(self):
        return DownloadDatasetParallelTask(config=self.config,
                                           destination=self._raw_data_s3_url())

    def run(self):
        format = self.config['sources'][0]['format']
        if format.lower() != 'vcf':
            raise ValueError("Expected 'vcf' format; got {0}".format(format))

        # 1. Copy the data from S3 to Hadoop's default filesystem
        tmp_hadoop_path = '/tmp/{rand_id}'.format(rand_id=random_id())
        distcp_cmd = '{hadoop_home}/bin/hadoop distcp {source} {target}'.format(
            hadoop_home=os.environ['HADOOP_HOME'],
            source=self._raw_data_s3n_url(), target=tmp_hadoop_path)
        p = Popen(distcp_cmd, shell=True)
        p.wait()

        # 2. Run the adam-submit job
        adam_cmd = ('{adam_home}/bin/adam-submit --master {spark_master_url} vcf2adam'
                    ' {source} {target}').format(
            adam_home=os.environ['ADAM_HOME'],
            spark_master_url=os.environ['SPARK_MASTER_URL'],
            source=tmp_hadoop_path, target=self._target_s3n_url())
        p = Popen(adam_cmd, shell=True)
        p.wait()
        if p.returncode == 0:
            create_SUCCESS_file(self._target_s3_url())

    def output(self):
        return S3FlagTarget(self._target_s3_url())

class BAM2ADAMTask(Task):

    config = ConfigParameter()

    def _raw_data_s3_url(self):
        return os.path.join(EGGO_S3_RAW_URL, self.config['name']) + '/'

    def _raw_data_s3n_url(self):
        return os.path.join(EGGO_S3N_RAW_URL, self.config['name']) + '/'

    def _target_s3_url(self):
        return os.path.join(EGGO_S3_BUCKET_URL, self.config['target']) + '/'

    def _target_s3n_url(self):
        return os.path.join(EGGO_S3N_BUCKET_URL, self.config['target']) + '/'

    def requires(self):
        return DownloadDatasetParallelTask(config=self.config,
                                           destination=self._raw_data_s3_url())

    def run(self):
        format = self.config['sources'][0]['format']
        if format.lower() != 'bam' and format.lower() != 'sam':
            raise ValueError("Expected 'sam' or 'bam'  format; got {0}".format(format))


        # 1. Copy the data from S3 to Hadoop's default filesystem
        tmp_hadoop_path = '/tmp/{rand_id}'.format(rand_id=random_id())
        distcp_cmd = '{hadoop_home}/bin/hadoop distcp {source} {target}'.format(
            hadoop_home=os.environ['HADOOP_HOME'],
            source=self._raw_data_s3n_url(), target=tmp_hadoop_path)
        p = Popen(distcp_cmd, shell=True)
        p.wait()

        # 2. Run the adam-submit job
        adam_cmd = ('{adam_home}/bin/adam-submit --master {spark_master_url} transform'
                    ' {source} {target}').format(
            adam_home=os.environ['ADAM_HOME'],
            spark_master_url=os.environ['SPARK_MASTER_URL'],
            source=tmp_hadoop_path, target=self._target_s3n_url())
        p = Popen(adam_cmd, shell=True)
        p.wait()
        if p.returncode == 0:
            create_SUCCESS_file(self._target_s3_url())

    def output(self):
        return S3FlagTarget(self._target_s3_url())
