from typing import Dict

from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_iam as iam,
    aws_cloudwatch as cloudwatch,
    aws_logs as logs,
    aws_lambda as awslambda,
    aws_kinesisfirehose_alpha as firehose,
    aws_kinesisfirehose_destinations_alpha as firehose_destinations,
    Duration,
    Size,
)
from constructs import Construct


class MetricsSenderStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, config: Dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        firehose_log_group = logs.LogGroup(
            self,
            'FirehoseLogGroup',
            log_group_name=config["firehose_log_group_name"])

        firehose_role = iam.Role(
            self,
            'FirehoseRole',
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com")
        )
        firehose_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    's3:PutObject',
                    's3:GetBucketLocation',
                ],
                resources=[
                    f"arn:aws:s3:::{config['s3_bucket_name']}",
                    f"arn:aws:s3:::{config['s3_bucket_name']}/*",
                ],
            )
        )
        firehose_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    'logs:PutLogEvents'
                ],
                resources=[
                    firehose_log_group.log_group_arn
                ],
            )
        )

        firehose_lambda = awslambda.Function(
            self,
            'FirehoseLambda',
            runtime=awslambda.Runtime.PYTHON_3_11,
            code=awslambda.Code.from_asset('aws_glue_cdk_observability_dashboard/lambda/'),
            handler='firehose_lambda.lambda_handler',
            timeout=Duration.seconds(300),
        )

        firehose_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    'lambda:InvokeFunction',
                    'lambda:GetFunctionConfiguration',
                ],
                resources=[
                    firehose_lambda.function_arn,
                ],
            )
        )

        firehose_lambda_processor = firehose.LambdaFunctionProcessor(
            firehose_lambda,
            retries=3,
            buffer_size=Size.mebibytes(config["firehose_lambda_buffer_size_mb"]),
            buffer_interval=Duration.seconds(config["firehose_lambda_buffer_interval_seconds"]),
        )

        delivery_stream = firehose.DeliveryStream(
            self,
            'FirehoseDeliveryStream',
            destinations=[
                firehose_destinations.S3Bucket(
                    s3.Bucket.from_bucket_name(
                        self,
                        'MetricsBucket',
                        bucket_name=config["s3_bucket_name"]
                    ),
                    role=firehose_role,
                    data_output_prefix="data/account_id=!{partitionKeyFromLambda:account_id}/region=!{partitionKeyFromLambda:region}/year=!{partitionKeyFromLambda:year}/month=!{partitionKeyFromLambda:month}/day=!{partitionKeyFromLambda:day}/hour=!{partitionKeyFromLambda:hour}/",
                    error_output_prefix="error/",
                    buffering_size=Size.mebibytes(config["firehose_s3_buffer_size_mb"]),
                    buffering_interval=Duration.seconds(config["firehose_s3_buffer_interval_seconds"]),
                    log_group=firehose_log_group,
                    processor=firehose_lambda_processor,
                )
            ]
        )
        cfn_delivery_stream = delivery_stream.node.default_child
        cfn_delivery_stream.add_property_override(
            'ExtendedS3DestinationConfiguration.DynamicPartitioningConfiguration',
            {'Enabled': True}
        )

        metricstream_role = iam.Role(
            self,
            'MetricStreamRole',
            assumed_by=iam.ServicePrincipal("streams.metrics.cloudwatch.amazonaws.com")
        )
        metricstream_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    'firehose:PutRecord',
                    'firehose:PutRecordBatch',
                ],
                resources=[
                    delivery_stream.delivery_stream_arn
                ],
            )
        )
        metric_stream = cloudwatch.CfnMetricStream(
            self,
            "ObservabilityMetricStream",
            firehose_arn=delivery_stream.delivery_stream_arn,
            output_format="json",
            include_filters=[{"namespace": "Glue"}],
            role_arn=metricstream_role.role_arn
        )