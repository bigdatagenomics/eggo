import os

from fabric.api import task, env, execute, local, open_shell

from eggo.fabric_util import (
    PROVISION_CMD, TEARDOWN_CMD, _setup_master, _setup_slave,
    _combine_with_environ, _get_master_host, _get_slave_hosts, _toast)


# ensure fabric connects as root (using default Amazon Linux AMI)
env.user = 'root'
# ensure fabric uses EC2 private key when connecting
if not env.key_filename:
    env.key_filename = os.environ['EC2_PRIVATE_KEY_FILE']


@task
def provision(slaves, type_='m3.large', region='us-east-1'):
    opts = {'slaves': slaves, 'type_': type_, 'region': region}
    full_opts = _combine_with_environ(opts)
    cmd = PROVISION_CMD.format(**full_opts)
    return local(cmd)


@task
def setup_master():
    hosts = _get_master_host()
    execute(_setup_master, hosts=hosts)


@task
def setup_slaves():
    hosts = _get_slave_hosts()
    env.parallel = True
    execute(_setup_slave, hosts=hosts)


@task
def login():
    hosts = _get_master_host()
    execute(open_shell, hosts=hosts)


@task
def teardown():
    cmd = TEARDOWN_CMD.format(**os.environ)
    local(cmd)


@task
def toast(config):
    hosts = _get_master_host()
    execute(_toast, config=config, hosts=hosts)


# for debugging
def _update_eggo():
    from fabric.api import cd, run
    from eggo.fabric_util import _install_eggo
    with cd('~'):
        run('rm -rf eggo')
    _install_eggo()

@task
def update_eggo():
    hosts = [_get_master_host()] + _get_slave_hosts()
    env.parallel = True
    execute(_update_eggo, hosts=hosts)
