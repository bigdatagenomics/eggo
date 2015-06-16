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
from shutil import rmtree
from getpass import getuser
from urlparse import urlparse
from cStringIO import StringIO

from fabric.api import (
    task, env, execute, local, open_shell, put, cd, run, prefix, shell_env,
    require, hosts, path, sudo, lcd)
from fabric.contrib.files import append, exists
from boto.ec2 import connect_to_region
from boto.s3.connection import S3Connection

import eggo.director
import eggo.spark_ec2
from eggo.util import build_dest_filename
from eggo.config import eggo_config, generate_luigi_cfg


exec_ctx = eggo_config.get('execution', 'context')
work_path = eggo_config.get('worker_env', 'work_path')
eggo_config_path = eggo_config.get('worker_env', 'eggo_config_path')
luigi_config_path = eggo_config.get('worker_env', 'luigi_config_path')
adam_fork = eggo_config.get('versions', 'adam_fork')
adam_branch = eggo_config.get('versions', 'adam_branch')
adam_home = eggo_config.get('worker_env', 'adam_home')
eggo_fork = eggo_config.get('versions', 'eggo_fork')
eggo_branch = eggo_config.get('versions', 'eggo_branch')
eggo_home = eggo_config.get('worker_env', 'eggo_home')
maven_version = eggo_config.get('versions', 'maven')


if exec_ctx == 'local':
    wrun = local
elif exec_ctx == 'spark_ec2':
    wrun = run
elif exec_ctx == 'director':
    wrun = sudo


# set some global Fabric env settings
if exec_ctx in ['spark_ec2', 'director']:
    # user that fabric connects as
    env.user = eggo_config.get(exec_ctx, 'user')
    # ensure fabric uses EC2 private key when connecting
    if not env.key_filename:
        env.key_filename = eggo_config.get('aws', 'ec2_private_key_file')


def get_master_host():
    if exec_ctx == 'spark_ec2':
        return eggo.spark_ec2.get_master_host()
    elif exec_ctx == 'director':
        return eggo.director.get_gateway_host()
    elif exec_ctx == 'local':
        return 'localhost'
    else:
        raise NotImplementedError('{0} exec ctx is not supported for this '
                                  'method'.format(exec_ctx))


def get_slave_hosts():
    if exec_ctx == 'spark_ec2':
        return eggo.spark_ec2.get_slave_hosts()
    elif exec_ctx == 'director':
        return eggo.director.get_worker_hosts()
    elif exec_ctx == 'local':
        return []
    else:
        raise NotImplementedError('{0} exec ctx is not supported for this '
                                  'method'.format(exec_ctx))


def get_worker_hosts():
    return [get_master_host()] + get_slave_hosts()


@task
def provision():
    if exec_ctx == 'spark_ec2':
        eggo.spark_ec2.provision()
    elif exec_ctx == 'director':
        eggo.director.provision()
    # at this point, get_master() should be valid

    # if the DFS is on the local fs, the directories may need to be created
    url = urlparse(eggo_config.get('dfs', 'dfs_root_url'))
    if url.scheme == 'file':
        local('mkdir -p {0}'.format(url.path))
        url = urlparse(eggo_config.get('dfs', 'dfs_raw_data_url'))
        local('mkdir -p {0}'.format(url.path))
        url = urlparse(eggo_config.get('dfs', 'dfs_tmp_data_url'))
        local('mkdir -p {0}'.format(url.path))

    # tag all the provisioned instances
    if exec_ctx in ['spark_ec2', 'director']:
        conn = connect_to_region(eggo_config.get(exec_ctx, 'region'))
        instances = conn.get_only_instances(
            filters={'key-name': [eggo_config.get('aws', 'ec2_key_pair')]})
        for instance in instances:
            instance.add_tag('owner', getuser())
            instance.add_tag('stack_name',
                             eggo_config.get(exec_ctx, 'stack_name'))


@task
def deploy_config():
    def do():
        # 0. ensure that the work path exists on the worker nodes
        wrun('mkdir -p -m 777 {work_path}'.format(work_path=work_path))

        # 1. copy local eggo config file to remote cluster
        put(local_path=os.environ['EGGO_CONFIG'],
            remote_path=eggo_config_path)

        # 2. deploy the luigi config file
        buf = StringIO(generate_luigi_cfg())
        put(local_path=buf,
            remote_path=luigi_config_path)

    execute(do, hosts=get_worker_hosts())


def install_pypa():
    # does a global install on distrib system;
    # on local system will install into venv if activated
    wrun('curl -s https://bootstrap.pypa.io/get-pip.py | python')
    wrun('pip install -U pip')
    wrun('pip install -U setuptools')


def install_git():
    wrun('yum install -y git')


def install_fabric_luigi():
    if exec_ctx == 'director':
        # python dev tools for fabric (pycrypto)
        wrun('yum install -y gcc python-devel python-setuptools')
    elif exec_ctx == 'spark_ec2':
        # protobuf for luigi
        wrun('yum install -y protobuf protobuf-devel protobuf-python')
    wrun('pip install mechanize')
    wrun('pip install fabric')
    wrun('pip install ordereddict')  # for py2.6 compat (for luigi)
    wrun('pip install luigi')


def install_adam(work_path, adam_home, maven_version, fork, branch):
    # dnload mvn
    mvn_path = os.path.join(work_path, 'apache-maven')
    wrun('mkdir -p {0}'.format(mvn_path))
    with cd(mvn_path):
        wrun('wget http://apache.mesi.com.ar/maven/maven-3/{version}/binaries/'
             'apache-maven-{version}-bin.tar.gz'.format(version=maven_version))
        wrun('tar -xzf apache-maven-{0}-bin.tar.gz'.format(maven_version))
    # checkout adam
    if not exists(adam_home):
        adam_parent = os.path.dirname(adam_home)
        wrun('mkdir -p {0}'.format(adam_parent))
        with cd(adam_parent):
            wrun('git clone https://github.com/{0}/adam.git'.format(fork))
            if branch != 'master':
                with cd('adam'):
                    wrun('git checkout origin/{branch}'.format(branch=branch))
    # build adam
    shell_vars = {}
    shell_vars['M2_HOME'] = os.path.join(
        mvn_path, 'apache-maven-{0}'.format(maven_version))
    shell_vars['M2'] = os.path.join(shell_vars['M2_HOME'], 'bin')
    shell_vars['MAVEN_OPTS'] = '-Xmx1024m -XX:MaxPermSize=512m'
    if exec_ctx == 'director':
        shell_vars['JAVA_HOME'] = '/usr/java/jdk1.7.0_67-cloudera'
    with cd(adam_home):
        with shell_env(**shell_vars):
            wrun('$M2/mvn clean package -DskipTests')


def install_eggo(work_path, eggo_home, fork, branch):
    if not exists(eggo_home):
        eggo_parent = os.path.dirname(eggo_home)
        wrun('mkdir -p {0}'.format(eggo_parent))
        with cd(eggo_parent):
            wrun('git clone https://github.com/{0}/eggo.git'.format(fork))
            if branch != 'master':
                with cd('eggo'):
                    wrun('git checkout origin/{0}'.format(branch))
    if exec_ctx == 'local':
        with lcd(eggo_home):
            local('pip install .')
    else:
        with cd(eggo_home):
            wrun('pip install .')


def create_hdfs_users():
    wrun('hadoop fs -mkdir /user/{user}'.format(user=env.user), user='hdfs')
    wrun('hadoop fs -chown {user} /user/{user}'.format(user=env.user), user='hdfs')


@task
def setup_master():
    def do():
        if exec_ctx == 'director':
            install_git()
            create_hdfs_users()
        install_pypa()
        install_fabric_luigi()
        install_adam(work_path, adam_home, maven_version, adam_fork, adam_branch)
        install_eggo(work_path, eggo_home, eggo_fork, eggo_branch)
        if exec_ctx == 'spark_ec2':
            # restart Hadoop
            wrun('/root/ephemeral-hdfs/bin/stop-all.sh')
            wrun('/root/ephemeral-hdfs/bin/start-all.sh')

    execute(do, hosts=get_master_host())


@task
def print_worker_env():
    def do():
        wrun('java -version')
        wrun('javac -version')
        wrun('mvn -version')

    execute(do, hosts=get_master_host())


@task
def setup_slaves():
    def do():
        env.parallel = True
        if exec_ctx == 'director':
            install_git()
        install_pypa()
        install_fabric_luigi()
        install_eggo(work_path, eggo_home, eggo_fork, eggo_branch)

    if exec_ctx in ['director', 'spark_ec2']:
        execute(do, hosts=get_slave_hosts())


@task
def list():
    if exec_ctx == 'director':
        eggo.director.list()
    else:
        raise NotImplementedError('{} exec ctx is not supported for this '
                                  'method'.format(exec_ctx))


@task
def login():
    execute(open_shell, hosts=get_master_host())


@task
def teardown():
    if exec_ctx == 'spark_ec2':
        eggo.spark_ec2.teardown()
    elif exec_ctx == 'director':
        eggo.director.teardown()


@task
def toast(config):
    def do():
        with open(config, 'r') as ip:
            config_data = json.load(ip)
        dag_class = config_data['dag']
        # push the toast config to the remote machine
        toast_config_worker_path = os.path.join(
            eggo_config.get('worker_env', 'work_path'),
            build_dest_filename(config))
        put(local_path=config,
            remote_path=toast_config_worker_path)
        # TODO: run on central scheduler instead
        toast_cmd = ('toaster.py --local-scheduler {clazz} '
                     '--ToastConfig-config {toast_config}'.format(
                        clazz=dag_class,
                        toast_config=toast_config_worker_path))
        
        hadoop_bin = os.path.join(eggo_config.get('worker_env', 'hadoop_home'), 'bin')
        toast_env = {'EGGO_HOME': eggo_config.get('worker_env', 'eggo_home'),  # toaster.py imports eggo_config, which needs EGGO_HOME on worker
                     'EGGO_CONFIG': eggo_config.get('worker_env', 'eggo_config_path'),  # bc toaster.py imports eggo_config which must be init on the worker
                     'LUIGI_CONFIG_PATH': eggo_config.get('worker_env', 'luigi_config_path'),
                     'AWS_ACCESS_KEY_ID': eggo_config.get('aws', 'aws_access_key_id'),  # bc dataset dnload pushes data to S3 TODO: should only be added if the dfs is S3
                     'AWS_SECRET_ACCESS_KEY': eggo_config.get('aws', 'aws_secret_access_key'),  # TODO: should only be added if the dfs is S3
                     'SPARK_HOME': eggo_config.get('worker_env', 'spark_home')}
        if exec_ctx == 'local':
                # this should copy vars that maintain venv info
                env_copy = os.environ.copy()
                env_copy.update(toast_env)
                toast_env = env_copy
        with path(hadoop_bin):
            with shell_env(**toast_env):
                wrun(toast_cmd)
    
    execute(do, hosts=get_master_host())


@task
def delete_raw(config):
    with open(config, 'r') as ip:
        config_data = json.load(ip)
    url = os.path.join(eggo_config.get('dfs', 'dfs_raw_data_url'),
                       config_data['name'])
    url = urlparse(url)
    if url.scheme == 's3n':
        conn = S3Connection()
        bucket = conn.get_bucket(url.netloc)
        keys = bucket.list(url.path.lstrip('/'))
        bucket.delete_keys(keys)
    elif url.scheme == 'file':
        # TODO: this assumes that file:// is in local mode
        rmtree(url.path, ignore_errors=True)
    else:
        raise NotImplementedError(
            "{0} dfs scheme not supported".format(url.scheme))


@task
def delete_tmp(config):
    with open(config, 'r') as ip:
        config_data = json.load(ip)
    url = os.path.join(eggo_config.get('dfs', 'dfs_tmp_data_url'),
                       config_data['name'])
    url = urlparse(url)
    if url.scheme == 's3n':
        conn = S3Connection()
        bucket = conn.get_bucket(url.netloc)
        keys = bucket.list(url.path.lstrip('/'))
        bucket.delete_keys(keys)
    elif url.scheme == 'file':
        rmtree(url.path, ignore_errors=True)
    else:
        raise NotImplementedError(
            "{0} dfs scheme not supported".format(url.scheme))


@task
def delete_toasted(config):
    with open(config, 'r') as ip:
        config_data = json.load(ip)
    url = os.path.join(eggo_config.get('dfs', 'dfs_root_url'),
                       config_data['name'])
    url = urlparse(url)
    if url.scheme == 's3n':
        conn = S3Connection()
        bucket = conn.get_bucket(url.netloc)
        keys = bucket.list(url.path.lstrip('/'))
        bucket.delete_keys(keys)
    elif url.scheme == 'file':
        rmtree(url.path, ignore_errors=True)
    else:
        raise NotImplementedError(
            "{0} dfs scheme not supported".format(url.scheme))


@task
def delete_all(config):
    delete_tmp(config)
    delete_raw(config)
    delete_toasted(config)


@task
def update_eggo():
    work_path = eggo_config.get('worker_env', 'work_path')
    venv_path = eggo_config.get('worker_env', 'venv_path')
    eggo_fork = eggo_config.get('versions', 'eggo_fork')
    eggo_branch = eggo_config.get('versions', 'eggo_branch')
    eggo_home = eggo_config.get('worker_env', 'eggo_home')

    def do():
        env.parallel = True
        if exec_ctx in ['director', 'spark_ec2']:
            wrun('rm -rf {0}'.format(eggo_home))
        install_eggo(work_path, eggo_home, eggo_fork, eggo_branch)

    execute(do, hosts=get_worker_hosts())


# Director commands (experimental)

@task
def cm_web_proxy():
    eggo.director.cm_web_proxy()


@task
def hue_web_proxy():
    eggo.director.hue_web_proxy()


@task
def yarn_web_proxy():
    eggo.director.yarn_web_proxy()
