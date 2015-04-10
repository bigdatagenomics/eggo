# eggo

Provides Parquet-formatted public 'omics datasets in S3 for easily using ADAM
and the Hadoop stack (including Spark and Impala). Instead of acquiring and
converting the data sets yourself, simply get them from the eggo S3 bucket:

```
s3://bdg-eggo
```

## User interface

Not implemented yet.

### Getting started

```
git clone https://github.com/bigdatagenomics/eggo.git
cd eggo
python setup.py install
```

TODO: pip installable scripts for listing datasets ets.

You need to [install](http://www.fabfile.org/installing.html) `fabric` too.

## Developer/maintainer interface

The `eggo` machinery uses Fabric and Luigi for its operation.

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

Ensure that Hadoop, AWS CLI, Spark, and ADAM are all installed.

Set `fs.s3n.awsAccessKeyId` and `fs.s3n.awsSecretAccessKey` in Hadoop's _core-site.xml_
 file.

Set up the environment with:

```bash
export SLAVES=localhost
export AWS_DEFAULT_REGION=us-east-1
export EPHEMERAL_MOUNT=/tmp
export ADAM_HOME=~/workspace/adam
export HADOOP_HOME=~/sw/hadoop-2.5.1/
```

Generate the test dataset with

```bash
bin/toaster.py --local-scheduler VCF2ADAMTask --config test/registry/test-genotypes.json
```

You can delete the test files with

```bash
aws s3 rm --recursive s3://bdg-eggo/raw/test-genotypes
aws s3 rm --recursive s3://bdg-eggo/test/genotypes
```