# `eggo`

Eggo is two things:

1. CLI for easily provisioning fully-functioning Hadoop clusters (CDH) using
   Cloudera Director

2. A set of Parquet-formatted public 'omics data sets in S3 for easily
   performing integrative genomics on the Hadoop stack (including Spark and
   Impala).

Eggo includes all the of scripts for processing the data, including the
necessary DDL statements to register the data sets with the Hive Metastore and
make them accessible to Hive/Impala.

At the moment, Eggo is geared specifically towards scaling up variant stores
and related functionality (e.g., population genomics, clinical genomics)

The pre-converted data sets are hosted at a publicly available S3 bucket:

```
s3://bdg-eggo
```

See the `datasets/` directory for a list of available data sets (with metadata
conforming to the [DataPackage spec](http://dataprotocols.org/data-packages/)).


## Getting started

```
pip install git+https://github.com/bigdatagenomics/eggo.git
```

Eggo makes use of [Fabric](http://www.fabfile.org/),
[Boto](https://boto.readthedocs.org/), and [Click](http://click.pocoo.org/).


## `eggo-cluster` command -- provisioning clusters

Simply run `eggo-cluster` at the command line.  The `eggo-cluster` tool expects
the following four environment variables:

* `AWS_ACCESS_KEY_ID`

* `AWS_SECRET_ACCESS_KEY`

* `EC2_KEY_PAIR` -- the name of the EC2-registered key pair to use for instance
   authentication

* `EC2_PRIVATE_KEY_FILE` -- the local path to the corresponding private key

```
$ eggo-cluster -h
Usage: eggo-cluster [OPTIONS] COMMAND [ARGS]...

  eggo-cluster -- provisions Hadoop clusters using Cloudera Director

Options:
  -h, --help  Show this message and exit.

Commands:
  config_cluster    Configure cluster for genomics, incl.
  describe          Describe the EC2 instances in the cluster
  get_director_log  DEBUG: get the Director application log from...
  login             Login to the cluster
  provision         Provision a new cluster on AWS
  reinstall_eggo    DEBUG: reinstall a specific version of eggo
  teardown          Tear down a cluster and stack on AWS
  web_proxy         Set up ssh tunnels to web UIs
```

A typical set of commands for creating a cluster would be:

```bash
eggo-cluster provision -n 5  # takes about 45 min
eggo-cluster config_cluster  # takes about 15 min

# login to the cluster's master node
eggo-cluster login

# in another terminal set up local ssh tunnels to give you access to the WebUIs
eggo-cluster web_proxy
# open up localhost:7180 to access Cloudera Manager

# when you are done with the cluster, tear it down
eggo-cluster teardown
```

### `eggo-cluster provision`

```
$ eggo-cluster provision -h
Usage: eggo-cluster provision [OPTIONS]

  Provision a new cluster on AWS

Options:
  --region TEXT                  AWS Region  [default: us-east-1]
  --stack-name TEXT              Stack name for CloudFormation and cluster
                                 name  [default: bdg-eggo]
  --availability-zone TEXT       AWS Availability Zone  [default: us-east-1b]
  --cf-template-path TEXT        Path to AWS Cloudformation Template
                                 [default:
                                 /Users/laserson/miniconda/lib/python2.7/site-p
                                 ackages/eggo-0.1.0.dev0-py2.7.egg/eggo/resour
                                 ces/cloudformation.template]
  --launcher-ami TEXT            The AMI to use for the launcher node
                                 [default: ami-00a11e68]
  --launcher-instance-type TEXT  The instance type to use for the launcher
                                 node  [default: m3.medium]
  --director-conf-path TEXT      Path to Director conf for AWS cloud
                                 [default:
                                 /Users/laserson/miniconda/lib/python2.7/site-p
                                 ackages/eggo-0.1.0.dev0-py2.7.egg/eggo/resour
                                 ces/aws.conf]
  --cluster-ami TEXT             The AMI to use for the worker nodes
                                 [default: ami-00a11e68]
  -n, --num-workers INTEGER      The total number of worker nodes to provision
                                 [default: 3]
  -h, --help                     Show this message and exit.

```


### `eggo-cluster config_cluster`

```
$ eggo-cluster config_cluster -h
Usage: eggo-cluster config_cluster [OPTIONS]

  Configure cluster for genomics, incl. ADAM, OpenCB, Quince, etc

Options:
  --region TEXT           AWS Region  [default: us-east-1]
  --stack-name TEXT       Stack name for CloudFormation and cluster name
                          [default: bdg-eggo]
  --adam / --no-adam      Install ADAM?  [default: True]
  --adam-fork TEXT        GitHub fork to use for ADAM  [default:
                          bigdatagenomics]
  --adam-branch TEXT      GitHub branch to use for ADAM  [default: master]
  --opencb / --no-opencb  Install OpenCB?  [default: False]
  --gatk / --no-gatk      Install GATK? (v4 aka Hellbender)  [default: True]
  --quince / --no-quince  Install quince?  [default: True]
  -h, --help              Show this message and exit.
```


### `eggo-cluster login`

```
$ eggo-cluster login -h
Usage: eggo-cluster login [OPTIONS]

  Login to the cluster

Options:
  --region TEXT                   AWS Region  [default: us-east-1]
  --stack-name TEXT               Stack name for CloudFormation and cluster
                                  name  [default: bdg-eggo]
  -n, --node [master|manager|launcher]
                                  The node to login to  [default: master]
  -h, --help                      Show this message and exit.
```
