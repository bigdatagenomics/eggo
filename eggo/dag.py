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
from shutil import rmtree
from tempfile import mkdtemp
from subprocess import Popen

from luigi import Task, Config
from luigi.s3 import S3Target, S3FlagTarget, S3Client
from luigi.hdfs import HdfsClient, HdfsTarget
from luigi.file import LocalTarget
from luigi.hadoop import JobTask, HadoopJobRunner
from luigi.parameter import Parameter

from eggo.config import eggo_config, validate_toast_config
from eggo.util import random_id, build_dest_filename


class JsonFileParameter(Parameter):
    def parse(self, p):
        with open(p, 'r') as ip:
            json_data = json.load(ip)
        validate_toast_config(json_data)
        return json_data


class ToastConfig(Config):
    config = JsonFileParameter()  # the toast (JSON) configuration

    def raw_data_url(self):
        return os.path.join(eggo_config.get('dfs', 'dfs_raw_data_url'),
                            self.config['name'])

    def dataset_url(self):
        return os.path.join(eggo_config.get('dfs', 'dfs_root_url'),
                            self.config['name'])

    def edition_url(self, format='bdg', edition='basic'):
        return os.path.join(eggo_config.get('dfs', 'dfs_root_url'),
                            self.config['name'], format, edition)

    def dfs_tmp_data_url(self):
        return os.path.join(eggo_config.get('dfs', 'dfs_tmp_data_url'),
                            self.config['name'],
                            eggo_config.get('execution', 'random_id'))


class EggoS3FlagTarget(S3FlagTarget):
    # NOTE: we are implementing our own version of S3FlagTarget even though
    # Luigi supplies this class because the Luigi version requires paths to end
    # in a slash, which is annoying to keep track of.  Instead, we join paths
    # using os.path.join()
    def __init__(self, path, format=None, client=None, flag='_SUCCESS'):
        # skip luigi.s3.S3FlagTarget and init *its* superclass: luigi.s3.S3Target
        super(S3FlagTarget, self).__init__(path)
        self.flag = flag

    def exists(self):
        return self.fs.exists(os.path.join(self.path, self.flag))


class HdfsFlagTarget(HdfsTarget):
    def __init__(self, path, flag='_SUCCESS'):
        super(HdfsFlagTarget, self).__init__(path)
        self.flag = flag

    def exists(self):
        return self.fs.exists(os.path.join(self.path, self.flag))


class LocalFlagTarget(LocalTarget):
    def __init__(self, path, flag='_SUCCESS'):
        super(LocalFlagTarget, self).__init__(path)
        self.flag = flag

    def exists(self):
        return self.fs.exists(os.path.join(self.path, self.flag))


def flag_target(path):
    if (path.startswith('s3:') or path.startswith('s3n:')
            or path.startswith('s3a:')):
        return EggoS3FlagTarget(path)
    elif path.startswith('hdfs:'):
        return HdfsFlagTarget(path)
    elif path.startswith('file:'):
        return LocalFlagTarget(path)
    else:
        raise ValueError('Unrecognized URI protocol: {path}'.format(path))


def file_target(path):
    if (path.startswith('s3:') or path.startswith('s3n:')
            or path.startswith('s3a:')):
        return S3Target(path)
    elif path.startswith('hdfs:'):
        return HdfsTarget(path)
    elif path.startswith('file:'):
        return LocalTarget(path)
    else:
        raise ValueError('Unrecognized URI protocol: {path}'.format(path))


def create_SUCCESS_file(path):
    if (path.startswith('s3:') or path.startswith('s3n:')
            or path.startswith('s3a:')):
        s3_client = S3Client(eggo_config.get('aws', 'aws_access_key_id'),
                            eggo_config.get('aws', 'aws_secret_access_key'))
        s3_client.put_string('', os.path.join(path, '_SUCCESS'))
    elif path.startswith('hdfs:'):
        hdfs_client = HdfsClient()
        hdfs_client.put('/dev/null', os.path.join(path, '_SUCCESS'))
    elif path.startswith('file:'):
        open(os.path.join(path, '_SUCCESS'), 'a').close()


def _dnload_to_local_upload_to_dfs(source, destination, compression):
    # source: (string) URL suitable for curl
    # destination: (string) full Hadoop path of destination file name
    # compression: (bool) whether file needs to be decompressed
    try:
        tmp_local_dir = mkdtemp(
            prefix='tmp_eggo_',
            dir=eggo_config.get('worker_env', 'work_path'))
        
        # 1. dnload file
        dnload_cmd = 'pushd {tmp_local_dir} && curl -L -O {source} && popd'
        p = Popen(dnload_cmd.format(tmp_local_dir=tmp_local_dir,
                                    source=source),
                  shell=True)
        p.wait()

        # 2. decompress if necessary
        if compression:
            compression_type = os.path.splitext(source)[-1]
            if compression_type == '.gz':
                decompr_cmd = ('pushd {tmp_local_dir} && gunzip *.gz && popd')
            else:
                raise ValueError("Unknown compression type: {0}".format(
                    compression_type))
            p = Popen(decompr_cmd.format(tmp_local_dir=tmp_local_dir),
                      shell=True)
            p.wait()

        try:
            # 3. upload to tmp distributed filesystem location (e.g. S3)
            tmp_staged_dir = os.path.join(
                eggo_config.get('dfs', 'dfs_tmp_data_url'),
                'staged',
                random_id())
            # get the name of the local file that we're uploading
            local_files = os.listdir(tmp_local_dir)
            if len(local_files) != 1:
                # TODO: generate warning/error here
                pass
            filename = local_files[0]
            # ensure the dfs directory exists; this cmd may fail if the dir
            # already exists, but that's ok (though it shouldn't already exist)
            create_dir_cmd = '$HADOOP_HOME/bin/hadoop fs -mkdir {tmp_dfs_dir}'
            p = Popen(create_dir_cmd.format(tmp_dfs_dir=tmp_staged_dir),
                      shell=True)
            p.wait()
            upload_cmd = '$HADOOP_HOME/bin/hadoop fs -put {tmp_local_file} {tmp_dfs_file}'
            p = Popen(upload_cmd.format(
                          tmp_local_file=os.path.join(tmp_local_dir, filename),
                          tmp_dfs_file=os.path.join(tmp_staged_dir, filename)),
                      shell=True)
            p.wait()

            # 4. rename to final target location
            rename_cmd = '$HADOOP_HOME/bin/hadoop fs -mv {tmp_path} {final_path}'
            p = Popen(rename_cmd.format(
                          tmp_path=os.path.join(tmp_staged_dir, filename),
                          final_path=destination),
                      shell=True)
            p.wait()
        except:
            raise
        finally:
            pass # TODO: clean up dfs tmp dir
    except:
        raise
    finally:
        rmtree(tmp_local_dir)


class DownloadFileToDFSTask(Task):
    """Download a file, decompress, and move to S3."""

    source = Parameter()  # string: URL suitable for curl
    target = Parameter()  # string: full URL path of destination file name
    compression = Parameter()  # bool: whether file needs to be decompressed

    def run(self):
        _dnload_to_local_upload_to_dfs(
            self.source, self.target, self.compression)

    def output(self):
        return file_target(path=self.target)


class DownloadDatasetTask(Task):
    # downloads the files serially in the scheduler

    destination = Parameter()  # full S3 prefix to put data

    def requires(self):
        for source in ToastConfig().config['sources']:
            dest_name = build_dest_filename(source['url'],
                                            decompress=source['compression'])
            yield DownloadFileToDFSTask(
                source=source['url'],
                target=os.path.join(self.destination, dest_name),
                compression=source['compression'])

    def run(self):
        create_SUCCESS_file(self.destination)

    def output(self):
        return flag_target(self.destination)


class PrepareHadoopDownloadTask(Task):
    hdfs_path = Parameter()

    def run(self):
        tmp_dir = mkdtemp(
            prefix='tmp_eggo_',
            dir=eggo_config.get('worker_env', 'work_path'))
        try:
            # build the remote command for each source
            tmp_command_file = '{0}/command_file'.format(tmp_dir)
            with open(tmp_command_file, 'w') as command_file:
                for source in ToastConfig().config['sources']:
                    command_file.write('{0}\n'.format(json.dumps(source)))

            # 3. Copy command file to Hadoop filesystem
            hdfs_client = HdfsClient()
            hdfs_client.put(tmp_command_file, self.hdfs_path)
        except:
            raise
        finally:
            rmtree(tmp_dir)

    def output(self):
        return HdfsTarget(path=self.hdfs_path)


class DownloadDatasetHadoopTask(JobTask):
    destination = Parameter()  # full Hadoop path to put data

    def requires(self):
        return PrepareHadoopDownloadTask(
            hdfs_path=ToastConfig().dfs_tmp_data_url())

    def job_runner(self):
        addl_conf = {'mapred.map.tasks.speculative.execution': 'false',
                     'mapred.task.timeout': 12000000}
        # TODO: can we delete the AWS vars with Director? does it set AWS cred in core-site.xml?
        streaming_args=['-cmdenv', 'EGGO_CONFIG=' + eggo_config.get('worker_env', 'eggo_config_path'),
                        '-cmdenv', 'AWS_ACCESS_KEY_ID=' + eggo_config.get('aws', 'aws_access_key_id'),
                        '-cmdenv', 'AWS_SECRET_ACCESS_KEY=' + eggo_config.get('aws', 'aws_secret_access_key')]
        return HadoopJobRunner(streaming_jar=os.environ['STREAMING_JAR'],
                               streaming_args=streaming_args,
                               jobconfs=addl_conf,
                               input_format='org.apache.hadoop.mapred.lib.NLineInputFormat',
                               output_format='org.apache.hadoop.mapred.lib.NullOutputFormat',
                               end_job_with_atomic_move_dir=False)

    def mapper(self, line):
        source = json.loads('\t'.join(line.split('\t')[1:]))
        dest_name = build_dest_filename(source['url'],
                                        decompress=source['compression'])
        dest_url = os.path.join(self.destination, dest_name)
        if dest_url.startswith("s3:") or dest_url.startswith("s3n:"):
            client = S3Client(eggo_config.get('aws', 'aws_access_key_id'),
                              eggo_config.get('aws', 'aws_secret_access_key'))
        else:
            client = HdfsClient()
        if not client.exists(dest_url):
            _dnload_to_local_upload_to_dfs(
                source['url'], dest_url, source['compression'])

        yield (source['url'], 1)  # dummy output

    def output(self):
        return flag_target(self.destination)


class DeleteDatasetTask(Task):

    def run(self):
        delete_raw_cmd = '$HADOOP_HOME/bin/hadoop fs -rm -r {raw} {target}'.format(
            raw=ToastConfig().raw_data_url(),
            target=ToastConfig().dataset_url())
        p = Popen(delete_raw_cmd, shell=True)
        p.wait()


class ADAMBasicTask(Task):

    adam_command = Parameter()
    allowed_file_formats = Parameter()
    edition = 'basic'

    def requires(self):
        return DownloadDatasetHadoopTask(
            destination=ToastConfig().raw_data_url())

    def run(self):
        format = ToastConfig().config['sources'][0]['format'].lower()
        if format not in self.allowed_file_formats:
            raise ValueError("Format '{0}' not in allowed formats {1}.".format(
                format, self.allowed_file_formats))

        # 1. Copy the data from source (e.g. S3) to Hadoop's default filesystem
        tmp_hadoop_path = '/tmp/{rand_id}.{format}'.format(rand_id=random_id(),
                                                           format=format)
        distcp_cmd = '$HADOOP_HOME/bin/hadoop distcp {source} {target}'.format(
            source=ToastConfig().raw_data_url(), target=tmp_hadoop_path)
        p = Popen(distcp_cmd, shell=True)
        p.wait()

        # 2. Run the adam-submit job
        adam_cmd = ('$ADAM_HOME/bin/adam-submit --master $SPARK_MASTER_URI {adam_command} '
                    '{source} {target}').format(
                        adam_command=self.adam_command, source=tmp_hadoop_path,
                        target=ToastConfig().edition_url(edition=self.edition))
        p = Popen(adam_cmd, shell=True)
        p.wait()

    def output(self):
        return flag_target(ToastConfig().edition_url(edition=self.edition))


class ADAMFlattenTask(Task):

    adam_command = Parameter()
    allowed_file_formats = Parameter()
    source_edition = 'basic'
    edition = 'flat'

    def requires(self):
        return ADAMBasicTask(adam_command=self.adam_command,
                             allowed_file_formats=self.allowed_file_formats)

    def run(self):
        adam_cmd = ('$ADAM_HOME/bin/adam-submit --master $SPARK_MASTER_URI flatten '
                    '{source} {target}').format(
                        source=ToastConfig().edition_url(
                            edition=self.source_edition),
                        target=ToastConfig().edition_url(edition=self.edition))
        p = Popen(adam_cmd, shell=True)
        p.wait()

    def output(self):
        return flag_target(ToastConfig().edition_url(edition=self.edition))


class ToastTask(Task):

    def output(self):
        return flag_target(ToastConfig().edition_url(edition=self.edition))


class VCF2ADAMTask(Task):

    def requires(self):
        basic = ADAMBasicTask(adam_command='vcf2adam',
                              allowed_file_formats=['vcf'])
        flat = ADAMFlattenTask(adam_command='vcf2adam',
                               allowed_file_formats=['vcf'])
        dependencies = [basic]
        conf = ToastConfig().config
        editions = conf['editions'] if 'editions' in conf else []
        for edition in editions:
            if edition == 'basic':
                pass # included by default
            elif edition == 'flat':
                dependencies.append(flat)
        return dependencies

    def run(self):
        pass

    def output(self):
        pass


class BAM2ADAMTask(Task):

    def requires(self):
        basic = ADAMBasicTask(adam_command='transform',
                              allowed_file_formats=['sam', 'bam'])
        flat = ADAMFlattenTask(adam_command='transform',
                               allowed_file_formats=['sam', 'bam'])
        dependencies = [basic]
        conf = ToastConfig().config
        editions = conf['editions'] if 'editions' in conf else []
        for edition in editions:
            if edition == 'basic':
                pass # included by default
            elif edition == 'flat':
                dependencies.append(flat)
        return dependencies

