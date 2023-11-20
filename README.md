
# AWS Glue CDK observability dashboard

This is a sample [AWS CDK](https://aws.amazon.com/cdk/) template for dashboard using AWS Glue observability metrics.

Typically, you have multiple accounts to manage and run resources for your data pipeline. 
In this template, we assume the following two types of accounts:

* Monitoring account - This hosts the central Amazon S3 bucket, central AWS Glue data catalog, and Amazon QuickSight related resources.
* Source account - This hosts individual data pipeline resources on AWS Glue, and the resources to send metrics to the monitoring account.

The template works even when monitoring account and source account are the same.

This sample consists of four stacks:

* S3 stack - This provisions S3 bucket.
* Catalog stack - This provisions AWS Glue database, table, and crawler.
* QuickSight stack - This provisions QuickSight data source, data set, and analysis.
* Metrics sender stack - This provisions Amazon CloudWatch metric stream, Kinesis Data Firehose, and AWS Lambda function for transformation.

## Pre-requisite

* Python 3.9 or later
* AWS accounts for Monitoring account, and Source account
* [AWS Named profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) for Monitoring account and Source account
* The [AWS CDK Toolkit (cdk command)](https://docs.aws.amazon.com/cdk/v2/guide/cli.html) 2.87.0 or later

### Initialize the project

To initialize the project, complete the following steps:

1. Clone [the cdk template](https://github.com/aws-samples/aws-glue-cdk-observability-dashboard) to your workplace.

```
$ git clone git@github.com:aws-samples/aws-glue-cdk-baseline.git
$ cd aws-glue-cdk-baseline.git
```

2. Create a Python [virtual environment](https://docs.python.org/3/library/venv.html) specific to the project on the client machine.

```
$ python3 -m venv .venv
```

We use a virtual environment in order to isolate the Python environment for this project and not install software globally.

3. Activate the virtual environment according to your OS:

* On MacOS and Linux, use the following code:

```
$ source .venv/bin/activate
```

* On a Windows platform, use the following code:

```
% .venv\Scripts\activate.bat
```

After this step, the subsequent steps run within the bounds of the virtual environment on the client machine and interact with the AWS account as needed.

4. Install the required dependencies described in [requirements.txt](https://github.com/aws-samples/aws-glue-cdk-observability-dashboard/blob/main/requirements.txt) to the virtual environment:

```
$ pip install -r requirements.txt
```


5. Edit the configuration file `default-config.yaml` based on your environments (replace each account ID with your own):

```
create_s3_stack: false
create_metrics_sender_stack: false
create_catalog_stack: false
create_quicksight_stack: true

s3_bucket_name: glue-observability-demo-dashboard

firehose_log_group_name: /aws/kinesisfirehose/observability-demo-metric-stream
firehose_lambda_buffer_size_mb: 2
firehose_lambda_buffer_interval_seconds: 60
firehose_s3_buffer_size_mb: 128
firehose_s3_buffer_interval_seconds: 300

glue_database_name: observability_demo_db
glue_table_name: metric_data
glue_crawler_name: observability_demo_crawler
glue_crawler_cron_schedule: "cron(42 * * * ? *)"

athena_workgroup_name: primary
```

### Bootstrap your AWS environments

Run the following commands to bootstrap your AWS environments.

1. In the monitoring account, replace `MONITORING-ACCOUNT-NUMBER`, `REGION`, and `MONITORING-PROFILE` with your own values:

```
$ cdk bootstrap aws://<MONITORING-ACCOUNT-NUMBER>/<REGION> --profile <MONITORING-PROFILE> \
    --cloudformation-execution-policies arn:aws:iam::aws:policy/AdministratorAccess
```

2. In the source account, replace `SOURCE-ACCOUNT-NUMBER`, `REGION`, and `SOURCE-PROFILE` with your own values:

```
$ cdk bootstrap aws://<SOURCE-ACCOUNT-NUMBER>/<REGION> --profile <SOURCE-PROFILE> \
    --cloudformation-execution-policies arn:aws:iam::aws:policy/AdministratorAccess
```

When you use only one account for all environments, you can just run the `cdk bootstrap` command one time.

### Deploy your AWS resources

Run the command using Monitoring account to deploy resources defined in the AWS CDK template:

```
$ cdk deploy '*' --profile <MONITORING-PROFILE>
```

Run the command using Source account to deploy resources defined in the AWS CDK template:

```
$ cdk deploy MetricSenderStack --profile <SOURCE-PROFILE>
```

