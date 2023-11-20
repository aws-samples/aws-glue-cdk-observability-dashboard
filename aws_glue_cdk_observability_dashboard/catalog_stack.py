import os
from typing import Dict

from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_glue as glue,
)
from constructs import Construct


class CatalogStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, config: Dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        account_id = os.getenv('CDK_DEFAULT_ACCOUNT')
        region = os.getenv('CDK_DEFAULT_REGION')

        glue_database = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=account_id,
            database_input={
                "name": config["glue_database_name"]
            }
        )
        glue_table = glue.CfnTable(
            self,
            "GlueTable",
            catalog_id=account_id,
            database_name=glue_database.database_input.name,
            table_input=glue.CfnTable.TableInputProperty(
                name=config["glue_table_name"],
                table_type="EXTERNAL_TABLE",
                parameters={
                    "classification": "json"
                },
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[
                        {
                            "name": "metric_stream_name",
                            "type": "string"
                        },
                        {
                            "name": "namespace",
                            "type": "string"
                        },
                        {
                            "name": "metric_name",
                            "type": "string"
                        },
                        {
                            "name": "dimensions",
                            "type": "struct<JobName:string,JobRunId:string,Type:string,Source:string,Sink:string,ObservabilityGroup:string,ExecutionClass:string,GlueVersion:string,JobType:string>"
                        },
                        {
                            "name": "timestamp",
                            "type": "bigint"
                        },
                        {
                            "name": "value",
                            "type": "struct<max:double,min:double,sum:double,count:double>"
                        },
                        {
                            "name": "unit",
                            "type": "string"
                        }
                    ],
                    location=f"s3://{config['s3_bucket_name']}/data/",
                    input_format="org.apache.hadoop.mapred.TextInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    compressed=False,
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.openx.data.jsonserde.JsonSerDe",
                        parameters={
                            "serialization.format": "json"
                        }
                    ),
                ),
                partition_keys=[
                    {
                        "name": "account_id",
                        "type": "string"
                    },
                    {
                        "name": "region",
                        "type": "string"
                    },
                    {
                        "name": "year",
                        "type": "string"
                    },
                    {
                        "name": "month",
                        "type": "string"
                    },
                    {
                        "name": "day",
                        "type": "string"
                    },
                    {
                        "name": "hour",
                        "type": "string"
                    }
                ]
            )
        )
        glue_table.add_dependency(glue_database)

        crawler_role = iam.Role(
            self,
            'CrawlerRole',
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com")
        )
        crawler_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    's3:GetObject',
                    's3:ListBucket',
                ],
                resources=[
                    f"arn:aws:s3:::{config['s3_bucket_name']}",
                    f"arn:aws:s3:::{config['s3_bucket_name']}/*",
                ],
            )
        )
        crawler_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole'))

        glue_crawler = glue.CfnCrawler(
            self,
            "GlueCrawler",
            name=config["glue_crawler_name"],
            role=crawler_role.role_arn,
            targets={
                "catalogTargets": [
                    {
                        "databaseName": glue_database.database_input.name,
                        "tables": [glue_table.table_input.name]
                    }
                ]
            },
            configuration="{\"Version\":1.0,\"CrawlerOutput\":{\"Partitions\":{\"AddOrUpdateBehavior\":\"InheritFromTable\"}},\"Grouping\":{\"TableGroupingPolicy\":\"CombineCompatibleSchemas\"}}",
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="LOG",
                delete_behavior="LOG"
            ),
            database_name=glue_database.database_input.name,
            schedule=glue.CfnCrawler.ScheduleProperty(
                schedule_expression=config["glue_crawler_cron_schedule"]
            )
        )
        glue_crawler.add_dependency(glue_table)