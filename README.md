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


## `eggo` command -- provisioning clusters

Simply run `eggo` at the command line.  The `eggo` tool expects the following
four environment variables:

* `AWS_ACCESS_KEY_ID`

* `AWS_SECRET_ACCESS_KEY`

* `EC2_KEY_PAIR` -- the name of the EC2-registered key pair to use for instance
   authentication

* `EC2_PRIVATE_KEY_FILE` -- the local path to the corresponding private key

```
$ eggo -h
Usage: eggo [OPTIONS] COMMAND [ARGS]...

  eggo -- provisions Hadoop clusters in AWS using Cloudera Director

Options:
  -h, --help  Show this message and exit.

Commands:
  describe   Describe the EC2 instances in the cluster
  login      Login to gateway node of cluster
  provision  Provision a new cluster on AWS
  setup      DOES NOTHING AT THE MOMENT
  teardown   Tear down a cluster and stack on AWS
```

### `eggo provision`

```
$ eggo provision -h
Usage: eggo provision [OPTIONS]

  Provision a new cluster on AWS

Options:
  --region TEXT                  AWS Region  [default: us-east-1]
  --stack-name TEXT              Stack name for CloudFormation and cluster
                                 name  [default: bdg-eggo]
  --availability-zone TEXT       AWS Availability Zone  [default: us-east-1b]
  --cf-template-path TEXT        Path to AWS Cloudformation Template
                                 [default: /usr/local/lib/python2.7/site-packa
                                 ges/eggo-0.1.0.dev0-py2.7.egg/eggo/cluster/cl
                                 oudformation.template]
  --launcher-ami TEXT            The AMI to use for the launcher node
                                 [default: ami-00a11e68]
  --launcher-instance-type TEXT  The instance type to use for the launcher
                                 node  [default: m3.medium]
  --director-conf-path TEXT      Path to Director conf for AWS cloud
                                 [default: /usr/local/lib/python2.7/site-packa
                                 ges/eggo-0.1.0.dev0-py2.7.egg/eggo/cluster/aw
                                 s.conf]
  --cluster-ami TEXT             The AMI to use for the worker nodes
                                 [default: ami-00a11e68]
  -n, --num-workers INTEGER      The total number of worker nodes to provision
                                 [default: 3]
  -h, --help                     Show this message and exit.
```


## Eggo data sets

### `datasets/`











OLD OLD OLD OLD OLD OLD OLD OLD

DELETE DELETE DELETE DELETE

vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv

### `registry/`

The `registry/` directory contains the metadata for the data sets we ingest and
convert to ADAM/Parquet.  Each data set is stored as a JSON file loosely based
on the Data Protocols spec.

### Environment

If using AWS, ensure the following variables are set locally:

```
export SPARK_HOME= # local path to Spark
export EC2_KEY_PAIR= # EC2 name of the registered key pair
export EC2_PRIVATE_KEY_FILE= # local path to associated private key (.pem file)
export AWS_ACCESS_KEY_ID= # AWS credentials
export AWS_SECRET_ACCESS_KEY= # AWS credentials
```

These variables must be set remotely, which can be done by `source eggo-
ec2-variables.sh`:

```
export AWS_ACCESS_KEY_ID= # AWS credentials
export AWS_SECRET_ACCESS_KEY= # AWS credentials
export SPARK_HOME= # remote path to Spark
export ADAM_HOME= # remote path to ADAM
export SPARK_MASTER= # Spark master host name
```

### Setting up a cluster

Set `EGGO_EXP=TRUE` to have the setup commands use the `experiment` branch of
eggo.

```bash
cd path/to/eggo

# provision a cluster on EC2 with 5 slave (worker) nodes
fab provision:5,r3.2xlarge

# configure proper environment on the instances
fab setup_master
fab setup_slaves

# (Cloudera infra-only)
./tag-my-instances.py

# get an interactive shell on the master node
fab login

# destroy the cluster
fab teardown
```

There is experimental support for using Cloudera Director to provision a cluster. This
is useful for running a cluster with more services, including YARN, the Hive metastore,
YARN, and Impala; however it takes longer (>30mins) to bring up a cluster than the
Spark EC2 scripts.

```bash
# provision a cluster on EC2 with 5 worker nodes
fab provision_director

# run a proxy to access Cloudera Manager via http://localhost:7180
# type 'exit' to quit process
fab cm_web_proxy

# log in to the gateway node
fab login_director

# destroy the cluster
fab teardown_director
```


### Converting data sets

The `toast` command will build the Luigi DAG for downloading the necessary data
to S3 and running the ADAM command to transform it to Parquet.

```
# toast the 1000 Genomes data set
fab toast:registry/1kg.json
```


### Configuration

Environment variables that should be set


ec2/spark-ec2 -k laserson-cloudera -i ~/.ssh/laserson-cloudera.pem -s 3 -t m3.large -z us-east-1a --delete-groups --copy-aws-credentials launch eggo
ec2/spark-ec2 -k laserson-cloudera -i ~/.ssh/laserson-cloudera.pem login eggo
ec2/spark-ec2 -k laserson-cloudera -i ~/.ssh/laserson-cloudera.pem destroy eggo

curl http://169.254.169.254/latest/meta-data/public-hostname


def verify_env():
    require('SPARK_HOME')
    require('EC2_KEY_PAIR')
    require('EC2_PRIVATE_KEY_FILE')
    require('AWS_ACCESS_KEY_ID')
    require('AWS_SECRET_ACCESS_KEY')



TODO: have to CLI commands: `eggo` for users and `toaster` for maintainers.

## Testing

You can run Eggo from a local machine, which is helpful while developing Eggo itself.

Ensure that Hadoop, Spark, and ADAM are all installed.

Set up the environment with:

```bash
export AWS_DEFAULT_REGION=us-east-1
export EPHEMERAL_MOUNT=/tmp
export ADAM_HOME=~/workspace/adam
export HADOOP_HOME=~/sw/hadoop-2.5.1/
export SPARK_HOME=~/sw/spark-1.3.0-bin-hadoop2.4/
export SPARK_MASTER_URI=local
export STREAMING_JAR=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.5.1.jar
export PATH=$PATH:$HADOOP_HOME/bin
```

By default, datasets will be stored on S3, and you will need to set
`fs.s3n.awsAccessKeyId` and `fs.s3n.awsSecretAccessKey` in Hadoop's _core-site.xml_ file.

To store datasets locally, set the `EGGO_BASE_URL` environment variable to a Hadoop path:

```bash
export EGGO_BASE_URL=file:///tmp/bdg-eggo
```

Generate a test dataset with

```bash
bin/toaster.py --local-scheduler VCF2ADAMTask --ToastConfig-config test/registry/test-genotypes.json
```

or

```bash
bin/toaster.py --local-scheduler BAM2ADAMTask --ToastConfig-config test/registry/test-alignments.json
```

You can delete the test datasets with

```bash
bin/toaster.py --local-scheduler DeleteDatasetTask --ToastConfig-config test/registry/test-genotypes.json
bin/toaster.py --local-scheduler DeleteDatasetTask --ToastConfig-config test/registry/test-alignments.json
```


## NEW config-file-based organization

Concepts:

* dfs: the target "distributed" filesystem that will contain the final ETL'd data

* workers: the machines on which ETL is executed

* worker_env: an environment which we assume available on the worker machines, including env variables and paths to write data

* client: the local machine from which we issue the CLI commands

* client_env: the environment assumed on the local machine

The only environment variable that MUST be set on the local client machine is
EGGO_CONFIG.  This config file will be deployed across all relevant worker
machines.

Other local client env vars that will be respected include: SPARK_HOME,
AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, EC2_KEY_PAIR, EC2_PRIVATE_KEY_FILE.
Everything else is derived from the EGGO_CONFIG file.

One of the workers is designated a master, which is where the computations are
executed.  This node needs additional configuration.

```
eggo provision
eggo deploy_config
eggo setup_master
eggo setup_slaves
eggo delete_all:config=$EGGO_HOME/test/registry/test-genotypes.json
eggo toast:config=$EGGO_HOME/test/registry/test-genotypes.json
eggo teardown
```
