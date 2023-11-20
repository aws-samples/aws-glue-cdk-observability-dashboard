from typing import Dict

from aws_cdk import (
    Stack,
    aws_s3 as s3,
)
from constructs import Construct


class S3Stack(Stack):

    def __init__(self, scope: Construct, construct_id: str, config: Dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = s3.Bucket(
            self,
            'MetricsBucket',
            bucket_name=config["s3_bucket_name"]
        )