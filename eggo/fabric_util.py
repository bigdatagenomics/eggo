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

from fabric.api import local, env, cd, run, prefix, shell_env


PROVISION_CMD = (
    '{SPARK_HOME}/ec2/spark-ec2 -k {EC2_KEY_PAIR} -i {EC2_PRIVATE_KEY_FILE} '
    '-s {slaves} -t {type_} -r {region} --copy-aws-credentials '
    'launch bdg-eggo')
TEARDOWN_CMD = (
    '{SPARK_HOME}/ec2/spark-ec2 -k {EC2_KEY_PAIR} -i {EC2_PRIVATE_KEY_FILE} '
    'destroy bdg-eggo')
GETMASTER_CMD = (
    '{SPARK_HOME}/ec2/spark-ec2 -k {EC2_KEY_PAIR} -i {EC2_PRIVATE_KEY_FILE} '
    'get-master bdg-eggo')


def _combine_with_environ(orig_dict):
    new_dict = orig_dict.copy()
    new_dict.update(os.environ)
    return new_dict


def _get_master_host():
    result = local(GETMASTER_CMD.format(**os.environ), capture=True)
    host = result.split('\n')[2].strip()
    return host


def _get_slave_hosts():
    _set_master_host()
    with cd('eggo'):
        with prefix('source eggo-ec2-variables.sh'):
            slaves = run('echo $SLAVES').split()
    return slaves


def _set_master_host(user='root'):
    host = _get_master_host()
    env.user = user
    env.hosts = [host]
    env.host_string = host


def _install_pip():
    with cd('/tmp'):
        run('curl -O https://bootstrap.pypa.io/get-pip.py')
        run('python get-pip.py')
    run('pip install -U awscli')


def _install_fabric_luigi():
    with cd('/tmp'):
        # protobuf for luigi
        run('yum install -y protobuf protobuf-devel protobuf-python')
        run('pip install mechanize')
        run('pip install fabric')
        run('pip install luigi')


def _install_maven(version):
    run('mkdir -p /usr/local/apache-maven')
    with cd('/usr/local/apache-maven'):
        run('wget http://apache.mesi.com.ar/maven/maven-3/{version}/binaries/apache-maven-{version}-bin.tar.gz'.format(version=version))
        run('tar -xzf apache-maven-{version}-bin.tar.gz'.format(
            version=version))
    run('echo "export M2_HOME=/usr/local/apache-maven/apache-maven-{version}" '
        '>> ~/.bash_profile'.format(version=version))
    run('echo "export M2=$M2_HOME/bin" >> ~/.bash_profile')
    run('echo "export PATH=$PATH:$M2" >> ~/.bash_profile')
    run('source ~/.bash_profile')
    run('mvn -version')


def _install_adam():
    # check out latest adam master
    with cd('~'):
        run('git clone https://github.com/bigdatagenomics/adam.git')
    # build adam
    with cd('adam'):
        with shell_env(MAVEN_OPTS='-Xmx512m -XX:MaxPermSize=128m'):
            run('mvn clean package -DskipTests')


def _install_eggo(fork='bigdatagenomics', branch='master'):
    # check out eggo
    with cd('~'):
        run('git clone https://github.com/bigdatagenomics/eggo.git')
    with cd('eggo'):
        if fork != 'bigdatagenomics':
            run('git pull --no-commit https://github.com/{fork}/eggo.git'
                ' {branch}'.format(fork=fork, branch=branch))
        elif branch != 'master':
            run('git checkout origin/{branch}'.format(branch=branch))
        run('python setup.py install')


def _setup_master(maven_version='3.2.5', eggo_fork='bigdatagenomics',
                  eggo_branch='master'):
    _install_pip()
    _install_fabric_luigi()
    _install_maven(maven_version)
    _install_adam()
    _install_eggo(fork=eggo_fork, branch=eggo_branch)
    run('/root/ephemeral-hdfs/bin/stop-all.sh')
    run('/root/ephemeral-hdfs/bin/start-all.sh')


def _setup_slave(eggo_fork='bigdatagenomics', eggo_branch='master'):
    _install_pip()
    _install_eggo(fork=eggo_fork, branch=eggo_branch)


def _toast(config):
    with open(config, 'r') as ip:
        config_data = json.load(ip)
    dag_class = config_data['dag']
    with cd('eggo'):
        with prefix('source eggo-ec2-variables.sh'):
            # TODO: eliminate this for central scheduler instead
            run('bin/toaster.py --local-scheduler {clazz} '
                '--config {config}'.format(clazz=dag_class, config=config))
            # run('./toaster.py --dataset {dataset}'.format(dataset=dataset))
