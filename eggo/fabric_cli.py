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
import json

from fabric.api import (
    task, env, execute, local, open_shell, put, cd, run, prefix, shell_env,
    require, hosts)
from fabric.contrib.files import append

import eggo.director
from eggo.util import build_dest_filename
from eggo.config import eggo_config


# user that fabric connects as
env.user = eggo_config.get('spark_ec2', 'user')
# ensure fabric uses EC2 private key when connecting
if not env.key_filename:
    env.key_filename = eggo_config.get('aws', 'ec2_private_key_file')


@task
def provision():
    provision_cmd = ('{spark_home}/ec2/spark-ec2 -k {ec2_key_pair} '
                     '-i {ec2_private_key_file} -s {slaves} -t {type_} '
                     '-r {region} --copy-aws-credentials launch bdg-eggo')
    interp_cmd = provision_cmd.format(
        spark_home=eggo_config.get('local_env', 'spark_home'),
        ec2_key_pair=eggo_config.get('aws', 'ec2_key_pair'),
        ec2_private_key_file=eggo_config.get('aws', 'ec2_private_key_file'),
        slaves=eggo_config.get('spark_ec2', 'num_slaves'),
        type_=eggo_config.get('spark_ec2', 'instance_type'),
        region=eggo_config.get('spark_ec2', 'region'))
    return local(interp_cmd)


def get_master_host():
    getmaster_cmd = ('{spark_home}/ec2/spark-ec2 -k {ec2_key_pair} '
                     '-i {ec2_private_key_file} get-master bdg-eggo')
    interp_cmd = getmaster_cmd.format(
        spark_home=eggo_config.get('local_env', 'spark_home'),
        ec2_key_pair=eggo_config.get('aws', 'ec2_key_pair'),
        ec2_private_key_file=eggo_config.get('aws', 'ec2_private_key_file'))
    result = local(interp_cmd, capture=True)
    host = result.split('\n')[2].strip()
    return host


def get_slave_hosts():
    def do():
        return run('echo $SLAVES').split()
    master = get_master_host()
    return execute(do, hosts=master)[master]


def get_worker_hosts():
    return [get_master_host()] + get_slave_hosts()


@task
def deploy_eggo_config():
    # copy local eggo config file to remote cluster and set EGGO_CONFIG var
    eggo_config_worker_path = eggo_config.get('worker_env',
                                              'eggo_config_worker_path')
    def do():
        put(local_path=os.environ['EGGO_CONFIG'],
            remote_path=eggo_config_worker_path)
        append('~/.bash_profile', 'export EGGO_CONFIG={eggo_config}'.format(
            eggo_config=eggo_config_worker_path))
    execute(do, hosts=get_worker_hosts())


def install_pip():
    with cd('/tmp'):
        run('curl -O https://bootstrap.pypa.io/get-pip.py')
        run('python get-pip.py')


def install_fabric_luigi():
    with cd('/tmp'):
        # protobuf for luigi
        run('yum install -y protobuf protobuf-devel protobuf-python')
        run('pip install mechanize')
        run('pip install fabric')
        run('pip install luigi')


def install_maven(version):
    run('mkdir -p /usr/local/apache-maven')
    with cd('/usr/local/apache-maven'):
        run('wget http://apache.mesi.com.ar/maven/maven-3/{version}/binaries/'
            'apache-maven-{version}-bin.tar.gz'.format(version=version))
        run('tar -xzf apache-maven-{version}-bin.tar.gz'.format(
            version=version))
    env_mod = [
        'export M2_HOME=/usr/local/apache-maven/apache-maven-{version}'.format(
            version=version),
        'export M2=$M2_HOME/bin',
        'export PATH=$PATH:$M2']
    append('~/.bash_profile', env_mod)
    run('mvn -version')


def install_adam(path, fork, branch):
    with cd(path):
        run('git clone https://github.com/bigdatagenomics/adam.git')
        with cd('adam'):
            # check out desired fork/branch
            if fork != 'bigdatagenomics':
                run('git pull --no-commit https://github.com/{fork}/adam.git'
                    ' {branch}'.format(fork=fork, branch=branch))
            elif branch != 'master':
                run('git checkout origin/{branch}'.format(branch=branch))
            # build adam
            with shell_env(MAVEN_OPTS='-Xmx512m -XX:MaxPermSize=128m'):
                run('mvn clean package -DskipTests')


def install_eggo(path, fork, branch):
    with cd(path):
        run('git clone https://github.com/bigdatagenomics/eggo.git')
        with cd('eggo'):
            # check out desired fork/branch
            if fork != 'bigdatagenomics':
                run('git pull --no-commit https://github.com/{fork}/eggo.git'
                    ' {branch}'.format(fork=fork, branch=branch))
            elif branch != 'master':
                run('git checkout origin/{branch}'.format(branch=branch))
            run('python setup.py install')


def install_env(files_to_source, hadoop_home, spark_home, streaming_jar,
                spark_master_uri, worker_data_dir):
    env_lines = []
    for file_ in files_to_source.split(','):
        env_lines.append('source {file}'.format(file=file_))
    env_lines.append('export HADOOP_HOME={hadoop_home}'.format(
        hadoop_home=hadoop_home))
    env_lines.append('export SPARK_HOME={spark_home}'.format(
        spark_home=spark_home))
    env_lines.append('export STREAMING_JAR={streaming_jar}'.format(
        streaming_jar=streaming_jar))
    env_lines.append('export SPARK_MASTER_URI={spark_master_uri}'.format(
        spark_master_uri=spark_master_uri))
    env_lines.append('export ADAM_HOME={adam_home}'.format(
        adam_home=os.path.join(worker_data_dir, 'adam')))
    env_lines.append('export EGGO_HOME={eggo_home}'.format(
        eggo_home=os.path.join(worker_data_dir, 'eggo')))
    env_lines.append(
        'export LUIGI_CONFIG_PATH={worker_data_dir}/luigi.cfg'.format(
            worker_data_dir=worker_data_dir))
    append('~/.bash_profile', env_lines)


def write_luigi_config():
    lines = []
    lines.append('[core]')
    lines.append(
        'logging_conf_file: {eggo_home}/conf/luigi/luigi_logging.cfg'.format(
            eggo_home=os.path.join(
                eggo_config.get('paths', 'worker_data_dir'), 'eggo')))
    lines.append('[hadoop]')
    lines.append('command: {hadoop_home}/bin/hadoop'.format(
        hadoop_home=eggo_config.get('worker_env', 'hadoop_home')))
    append('{worker_data_dir}/luigi.cfg'.format(
               worker_data_dir=eggo_config.get('paths', 'worker_data_dir')),
           lines)


@task
def setup_master():
    def do():
        install_pip()
        install_fabric_luigi()
        install_maven(eggo_config.get('versions', 'maven'))
        install_adam(eggo_config.get('paths', 'worker_data_dir'),
                     eggo_config.get('versions', 'adam_fork'),
                     eggo_config.get('versions', 'adam_branch'))
        install_eggo(eggo_config.get('paths', 'worker_data_dir'),
                     eggo_config.get('versions', 'eggo_fork'),
                     eggo_config.get('versions', 'eggo_branch'))
        write_luigi_config()
        install_env(files_to_source=eggo_config.get('worker_env', 'files_to_source'),
                    hadoop_home=eggo_config.get('worker_env', 'hadoop_home'),
                    spark_home=eggo_config.get('worker_env', 'spark_home'),
                    streaming_jar=eggo_config.get('worker_env', 'streaming_jar'),
                    spark_master_uri=eggo_config.get('worker_env', 'spark_master_uri'),
                    worker_data_dir=eggo_config.get('paths', 'worker_data_dir'))
        # restart Hadoop
        run('/root/ephemeral-hdfs/bin/stop-all.sh')
        run('/root/ephemeral-hdfs/bin/start-all.sh')
    execute(do, hosts=get_master_host())


@task
def setup_slaves():
    def do():
        env.parallel = True
        install_pip()
        install_eggo(eggo_config.get('paths', 'worker_data_dir'),
                     eggo_config.get('versions', 'eggo_fork'),
                     eggo_config.get('versions', 'eggo_branch'))
    execute(do, hosts=get_slave_hosts())


@task
def login():
    execute(open_shell, hosts=get_master_host())


@task
def teardown():
    teardown_cmd = ('{spark_home}/ec2/spark-ec2 -k {ec2_key_pair} '
                    '-i {ec2_private_key_file} destroy bdg-eggo')
    interp_cmd = teardown_cmd.format(
        spark_home=eggo_config.get('local_env', 'spark_home'),
        ec2_key_pair=eggo_config.get('aws', 'ec2_key_pair'),
        ec2_private_key_file=eggo_config.get('aws', 'ec2_private_key_file'))
    local(interp_cmd)


@task
def toast(config):
    def do():
        with open(config, 'r') as ip:
            config_data = json.load(ip)
        dag_class = config_data['dag']
        run('test -n "$EGGO_HOME"')  # ensure proper env installed
        # push the toast config to the remote machine
        toast_config_worker_path = os.path.join(
            eggo_config.get('paths', 'worker_data_dir'),
            build_dest_filename(config))
        put(local_path=config,
            remote_path=toast_config_worker_path)
        # TODO: run on central scheduler instead
        toast_cmd = ('toaster.py --local-scheduler {clazz} '
                     '--ToastConfig-config {toast_config}'.format(
                        clazz=dag_class,
                        toast_config=toast_config_worker_path))
        run(toast_cmd)
    execute(do, hosts=get_master_host())


@task
def update_eggo():
    def do():
        env.parallel = True
        run('rm -rf $EGGO_HOME')
        install_eggo(eggo_config.get('paths', 'worker_data_dir'),
                     eggo_config.get('versions', 'eggo_fork'),
                     eggo_config.get('versions', 'eggo_branch'))
    execute(do, hosts=[get_master_host()] + get_slave_hosts())


# Director commands (experimental)

@task
def provision_director():
    env.user = 'ec2-user' # use ec2-user for launcher instance login
    eggo.director.provision()


@task
def list_director():
    eggo.director.list()


@task
def login_director():
    env.user = 'ec2-user' # use ec2-user for gateway instance login
    eggo.director.login()


@task
def cm_web_proxy():
    eggo.director.cm_web_proxy()


@task
def hue_web_proxy():
    eggo.director.hue_web_proxy()


@task
def yarn_web_proxy():
    eggo.director.yarn_web_proxy()


@task
def teardown_director():
    env.user = 'ec2-user' # use ec2-user for launcher instance login
    eggo.director.teardown()
