#!/usr/bin/env python3
import os
import yaml

import aws_cdk as cdk

from aws_glue_cdk_observability_dashboard.s3_stack import S3Stack
from aws_glue_cdk_observability_dashboard.metrics_sender_stack import MetricsSenderStack
from aws_glue_cdk_observability_dashboard.catalog_stack import CatalogStack
from aws_glue_cdk_observability_dashboard.quicksight_stack import QuickSightStack


app = cdk.App()

config_file = app.node.try_get_context('config')

if config_file:
    configFilePath = config_file
else:
    configFilePath = "./default-config.yaml"
with open(configFilePath, 'r', encoding="utf-8") as f:
    config = yaml.load(f, Loader=yaml.SafeLoader)

s3_stack = None
metrics_sender_stack = None
catalog_stack = None
quicksight_stack = None

if config["create_s3_stack"]:
    s3_stack = S3Stack(
        app,
        "S3Stack",
        config=config
    )

if config["create_metrics_sender_stack"]:
    metrics_sender_stack = MetricsSenderStack(
        app,
        "MetricsSenderStack",
        config=config
    )

if config["create_catalog_stack"]:
    catalog_stack = CatalogStack(
        app,
        "CatalogStack",
        config=config
    )

if config["create_quicksight_stack"]:
    quicksight_stack = QuickSightStack(
        app,
        "QuickSightStack",
        config=config
    )

# S3Stack -> MetricsSenderStack/CatalogStack -> QuickSightStack
if s3_stack and metrics_sender_stack:
    metrics_sender_stack.add_dependency(s3_stack)
if s3_stack and catalog_stack:
    catalog_stack.add_dependency(s3_stack)
if catalog_stack and quicksight_stack:
    quicksight_stack.add_dependency(catalog_stack)


app.synth()
