import os
from typing import Dict

from aws_cdk import (
    Stack,
    aws_quicksight as quicksight,
)
from constructs import Construct

SHEET_ID_MONITORING = "MONITORING_SHEET"
SHEET_ID_INSIGHTS = "INSIGHTS_SHEET"
VISUAL_ID_JOB_RUN_ERRORS_BREAKDOWN = "JOB_RUN_ERRORS_BREAKDOWN_VISUAL"
VISUAL_ID_JOB_RUN_ERRORS = "JOB_RUN_ERRORS_VISUAL"
VISUAL_ID_SKEWNESS_JOB = "SKEWNESS_JOB_VISUAL"
VISUAL_ID_SKEWNESS_JOB_PER_JOB = "SKEWNESS_JOB_PER_JOB_VISUAL"
VISUAL_ID_WORKER_UTILIZATION = "WORKER_UTILIZATION_VISUAL"
VISUAL_ID_WORKER_UTILIZATION_PER_JOB = "WORKER_UTILIZATION_PER_JOB_VISUAL"
VISUAL_ID_THROUGHPUT_READ = "THROUGHPUT_READ_VISUAL"
VISUAL_ID_THROUGHPUT_WRITE = "THROUGHPUT_WRITE_VISUAL"
VISUAL_ID_DISK_AVAILABLE = "DISK_AVAILABLE_VISUAL"
VISUAL_ID_DISK_USED = "DISK_USED_VISUAL"
VISUAL_ID_EXECUTOR_OOM_COUNT = "EXECUTOR_OOM_COUNT_VISUAL"
VISUAL_ID_EXECUTOR_HEAP_MEMORY = "EXECUTOR_HEAP_MEMORY_VISUAL"
VISUAL_ID_DRIVER_OOM_COUNT = "DRIVER_OOM_COUNT_VISUAL"
VISUAL_ID_DRIVER_HEAP_MEMORY = "DRIVER_HEAP_MEMORY_VISUAL"

VISUAL_ID_BOTTOM_RANKED_WORKER_UTILIZATION = "BOTTOM_RANKED_WORKER_UTILIZATION_INSIGHT"
VISUAL_ID_TOP_RANKED_SKEWNESS_JOB = "TOP_RANKED_SKEWNESS_JOB_INSIGHT"
VISUAL_ID_FORECAST_WORKER_UTILIZATION = "FORECAST_WORKER_UTILIZATION_INSIGHT"
VISUAL_ID_TOP_MOVER_THROUGHPUT_READ = "TOP_MOVER_THROUGHPUT_READ_INSIGHT"

COLUMN_ID_ANY = "ANY-COLUMN"
COLUMN_ID_DATE = "DATE-COLUMN"

DATASET_KEY = "DATASET-KEY"
PARAM_KEY = "PARAM-KEY"
FIELD_ID_SUFFIX_ANY = "any"
FIELD_ID_SUFFIX_METRIC_NAME_1 = "metric_name.1"
FIELD_ID_SUFFIX_METRIC_NAME_2 = "metric_name.2"
FIELD_ID_SUFFIX_DATE = "date"
FIELD_ID_SUFFIX_MAX = "max"
FIELD_ID_SUFFIX_MIN = "min"
FIELD_ID_SUFFIX_JOBNAME = "jobname"

FILTER_GROUP_ID_JOB_RUN_ERRORS = "JOB_RUN_ERRORS_VISUAL_FG"
FILTER_GROUP_ID_SKEWNESS = "SKEWNESS_VISUAL_FG"
FILTER_GROUP_ID_WORKER_UTILIZATION = "WORKER_UTILIZATION_VISUAL_FG"
FILTER_GROUP_ID_THROUGHPUT = "THROUGHPUT_VISUAL_FG"
FILTER_GROUP_ID_THROUGHPUT_READ = "THROUGHPUT_READ_VISUAL_FG"
FILTER_GROUP_ID_THROUGHPUT_WRITE = "THROUGHPUT_WRITE_VISUAL_FG"
FILTER_GROUP_ID_DISK_USED = "DISK_USED_VISUAL_FG"
FILTER_GROUP_ID_DISK_AVAILBLE = "DISK_AVAILABLE_VISUAL_FG"
FILTER_GROUP_ID_DRIVER_OOM_COUNT = "DRIVER_OOM_COUNT_VISUAL_FG"
FILTER_GROUP_ID_DRIVER_HEAP_MEMORY = "DRIVER_HEAP_MEMORY_VISUAL_FG"
FILTER_GROUP_ID_EXECUTOR_OOM_COUNT = "EXECUTOR_OOM_COUNT_VISUAL_FG"
FILTER_GROUP_ID_EXECUTOR_HEAP_MEMORY = "EXECUTOR_HEAP_MEMORY_VISUAL_FG"
FILTER_GROUP_ID_DATE_PARAM = "DATE_PARAM_FG"
FILTER_GROUP_ID_REGION_PARAM = "REGION_PARAM_FG"
FILTER_GROUP_ID_ACCOUNT_PARAM = "ACCOUNT_PARAM_FG"
FILTER_GROUP_ID_JOBNAME_PARAM = "JOBNAME_PARAM_FG"
FILTER_GROUP_ID_JOBRUN_ID_PARAM = "JOBRUN_ID_PARAM_FG"
FILTER_GROUP_ID_SOURCE_PARAM = "SOURCE_PARAM_FG"
FILTER_GROUP_ID_SINK_PARAM = "SINK_PARAM_FG"
FILTER_GROUP_ID_WORKER_UTILIZATION_INSIGHT = "WORKER_UTILIZATION_INSIGHT_FG"
FILTER_GROUP_ID_SKEWNESS_INSIGHT = "SKEWNESS_INSIGHT_FG"

class QuickSightStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, config: Dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        account_id = os.getenv('CDK_DEFAULT_ACCOUNT')
        region = os.getenv('CDK_DEFAULT_REGION')

        quicksight_datasource = quicksight.CfnDataSource(
            self,
            "ObservabilityDataSource",
            data_source_id="observability_datasource",
            aws_account_id=account_id,
            name="metrics_data",
            type="ATHENA",
            data_source_parameters={
                "AthenaParameters": {
                    "WorkGroup": config["athena_workgroup_name"]
                }
            },
            ssl_properties={
                "DisableSsl": False
            }
        )
        quicksight_dataset = quicksight.CfnDataSet(
            self,
            "ObservabilityDataset",
            data_set_id="observability_dataset",
            aws_account_id=account_id,
            name="observability_demo.metrics_data",
            import_mode="SPICE",
            physical_table_map={
                "0986fb70-0481-4065-b80c-5247b01b7524": quicksight.CfnDataSet.PhysicalTableProperty(
                    custom_sql=quicksight.CfnDataSet.CustomSqlProperty(
                        data_source_arn=quicksight_datasource.attr_arn,
                        sql_query=f"SELECT account_id, region, namespace, metric_name, dimensions.jobname, dimensions.jobrunid, dimensions.type, dimensions.source, dimensions.sink, dimensions.observabilitygroup, timestamp, value.max, value.min, value.sum, value.count, unit, year, month, day, hour FROM \"{config['glue_database_name']}\".\"{config['glue_table_name']}\"",
                        name="observability_demo.metrics_data",
                        columns=[
                            quicksight.CfnDataSet.InputColumnProperty(name="account_id", type="STRING"),
                            quicksight.CfnDataSet.InputColumnProperty(name="region", type="STRING"),
                            quicksight.CfnDataSet.InputColumnProperty(name="namespace", type="STRING"),
                            quicksight.CfnDataSet.InputColumnProperty(name="metric_name", type="STRING"),
                            quicksight.CfnDataSet.InputColumnProperty(name="jobname", type="STRING"),
                            quicksight.CfnDataSet.InputColumnProperty(name="jobrunid", type="STRING"),
                            quicksight.CfnDataSet.InputColumnProperty(name="type", type="STRING"),
                            quicksight.CfnDataSet.InputColumnProperty(name="source", type="STRING"),
                            quicksight.CfnDataSet.InputColumnProperty(name="sink", type="STRING"),
                            quicksight.CfnDataSet.InputColumnProperty(name="observabilitygroup", type="STRING"),
                            quicksight.CfnDataSet.InputColumnProperty(name="timestamp", type="INTEGER"),
                            quicksight.CfnDataSet.InputColumnProperty(name="max", type="DECIMAL"),
                            quicksight.CfnDataSet.InputColumnProperty(name="min", type="DECIMAL"),
                            quicksight.CfnDataSet.InputColumnProperty(name="sum", type="DECIMAL"),
                            quicksight.CfnDataSet.InputColumnProperty(name="count", type="DECIMAL"),
                            quicksight.CfnDataSet.InputColumnProperty(name="unit", type="STRING"),
                            quicksight.CfnDataSet.InputColumnProperty(name="year", type="STRING"),
                            quicksight.CfnDataSet.InputColumnProperty(name="month", type="STRING"),
                            quicksight.CfnDataSet.InputColumnProperty(name="day", type="STRING"),
                            quicksight.CfnDataSet.InputColumnProperty(name="hour", type="STRING")
                        ]
                    )
                )
            },
            logical_table_map={
                DATASET_KEY: quicksight.CfnDataSet.LogicalTableProperty(
                    alias="observability_demo.metrics_data",
                    data_transforms=[
                        quicksight.CfnDataSet.TransformOperationProperty(
                            create_columns_operation=quicksight.CfnDataSet.CreateColumnsOperationProperty(
                                columns=[
                                    quicksight.CfnDataSet.CalculatedColumnProperty(
                                        column_id=COLUMN_ID_ANY,
                                        column_name="avg",
                                        expression="{sum}/{count}"
                                    )
                                ]
                            )
                        ),
                        quicksight.CfnDataSet.TransformOperationProperty(
                            create_columns_operation=quicksight.CfnDataSet.CreateColumnsOperationProperty(
                                columns=[
                                    quicksight.CfnDataSet.CalculatedColumnProperty(
                                        column_id=COLUMN_ID_DATE,
                                        column_name="date",
                                        expression="parseDate(concat({year}, '-', {month}, '-', {day}, ' ', {hour}, ':00:00'), \"yyyy-MM-dd HH:mm:ss\")"
                                    )
                                ]
                            )
                        ),
                        quicksight.CfnDataSet.TransformOperationProperty(
                            project_operation=quicksight.CfnDataSet.ProjectOperationProperty(
                                projected_columns=[
                                    "account_id",
                                    "region",
                                    "namespace",
                                    "metric_name",
                                    "jobname",
                                    "jobrunid",
                                    "type",
                                    "source",
                                    "sink",
                                    "observability_group",
                                    "timestamp",
                                    "max",
                                    "min",
                                    "sum",
                                    "count",
                                    "unit",
                                    "year",
                                    "month",
                                    "day",
                                    "hour",
                                    "avg",
                                    "date"
                                ]
                            )
                        )
                    ],
                    source=quicksight.CfnDataSet.LogicalTableSourceProperty(
                        physical_table_id="0986fb70-0481-4065-b80c-5247b01b7524"
                    )
                )
            }
        )

        quicksight_refresh_schedule = quicksight.CfnRefreshSchedule(
            self,
            "ObservabilityRefreshSchedule",
            aws_account_id=account_id,
            data_set_id="observability_dataset",
            schedule=quicksight.CfnRefreshSchedule.RefreshScheduleMapProperty(
                schedule_id="observability_refresh_schedule",
                refresh_type="FULL_REFRESH",
                schedule_frequency=quicksight.CfnRefreshSchedule.ScheduleFrequencyProperty(
                    interval="HOURLY",
                    time_zone="America/Los_Angeles"
                )
            )
        )
        quicksight_refresh_schedule.add_dependency(quicksight_dataset)

        sheet_monitoring = quicksight.CfnAnalysis.SheetDefinitionProperty(
            sheet_id=SHEET_ID_MONITORING,
            name="Monitoring",
            parameter_controls=parameter_controls_for_sheet(SHEET_ID_MONITORING),
            visuals=[
                pie_chart_visual_for_metric(
                    visual_id=VISUAL_ID_JOB_RUN_ERRORS_BREAKDOWN,
                    visual_title="<visual-title>[Reliability] Job Run Errors Breakdown</visual-title>",
                    column_name="count",
                    value_statistics="SUM"
                ),
                combo_chart_visual_for_metric(
                    visual_id=VISUAL_ID_JOB_RUN_ERRORS,
                    visual_title="<visual-title>[Reliability] Job Run Errors (Total)</visual-title>"
                ),
                combo_chart_visual_for_metric_min_max_avg(
                    visual_id=VISUAL_ID_SKEWNESS_JOB,
                    visual_title="<visual-title>[Performance] Skewness Job</visual-title>"
                ),
                line_chart_visual_for_metric(
                    visual_id=VISUAL_ID_SKEWNESS_JOB_PER_JOB,
                    visual_title="<visual-title>[Performance] Skewness Job per Job</visual-title>",
                    column_name="avg",
                    color_column_name="jobname",
                    value_statistics="AVERAGE",
                    chart_type="LINE"
                ),
                combo_chart_visual_for_metric_min_max_avg(
                    visual_id=VISUAL_ID_WORKER_UTILIZATION,
                    visual_title="<visual-title>[Resource Utilization] Worker Utilization</visual-title>"
                ),
                line_chart_visual_for_metric(
                    visual_id=VISUAL_ID_WORKER_UTILIZATION_PER_JOB,
                    visual_title="<visual-title>[Resource Utilization] Worker Utilization per Job</visual-title>",
                    column_name="avg",
                    color_column_name="jobname",
                    value_statistics="AVERAGE",
                    chart_type="LINE"
                ),
                line_chart_visual_for_metric(
                    visual_id=VISUAL_ID_THROUGHPUT_READ,
                    visual_title="<visual-title>[Throughput] BytesRead, RecordsRead, FilesRead, PartitionsRead (Avg)</visual-title>",
                    column_name="avg",
                    color_column_name="metric_name",
                    value_statistics="AVERAGE",
                    chart_type="LINE"
                ),
                line_chart_visual_for_metric(
                    visual_id=VISUAL_ID_THROUGHPUT_WRITE,
                    visual_title="<visual-title>[Throughput] BytesWritten, RecordsWritten, FilesWritten (Avg)</visual-title>",
                    column_name="avg",
                    color_column_name="metric_name",
                    value_statistics="AVERAGE",
                    chart_type="LINE"
                ),
                line_chart_visual_for_metric(
                    visual_id=VISUAL_ID_DISK_AVAILABLE,
                    visual_title="<visual-title>[Resource Utilization] Disk Available GB (Min)</visual-title>",
                    column_name="min",
                    color_column_name="metric_name",
                    value_statistics="MIN",
                    chart_type="LINE"
                ),
                line_chart_visual_for_metric(
                    visual_id=VISUAL_ID_DISK_USED,
                    visual_title="<visual-title>[Resource Utilization] Max Disk Used % (Max)</visual-title>",
                    column_name="max",
                    color_column_name="metric_name",
                    value_statistics="MAX",
                    chart_type="LINE"
                ),
                kpi_visual_for_metric(
                    visual_id=VISUAL_ID_DRIVER_OOM_COUNT,
                    visual_title="<visual-title>[Driver OOM] OOM Error Count</visual-title>",
                    column_name="count",
                    value_statistics="SUM"
                ),
                line_chart_visual_for_metric(
                    visual_id=VISUAL_ID_DRIVER_HEAP_MEMORY,
                    visual_title="<visual-title>[Driver OOM] Max Heap Memory Used % (Max)</visual-title>",
                    column_name="max",
                    color_column_name="metric_name",
                    value_statistics="MAX",
                    chart_type="LINE"
                ),
                kpi_visual_for_metric(
                    visual_id=VISUAL_ID_EXECUTOR_OOM_COUNT,
                    visual_title="<visual-title>[Executor OOM] OOM Error Count</visual-title>",
                    column_name="count",
                    value_statistics="SUM"
                ),
                line_chart_visual_for_metric(
                    visual_id=VISUAL_ID_EXECUTOR_HEAP_MEMORY,
                    visual_title="<visual-title>[Executor OOM] Max Heap Memory Used % (Max)</visual-title>",
                    column_name="max",
                    color_column_name="metric_name",
                    value_statistics="MAX",
                    chart_type="LINE"
                ),
            ]
        )

        sheet_insights = quicksight.CfnAnalysis.SheetDefinitionProperty(
            sheet_id=SHEET_ID_INSIGHTS,
            name="Insights",
            parameter_controls=parameter_controls_for_sheet(SHEET_ID_INSIGHTS),
            visuals=[
                insight_visual_bottom_ranked_for_metrics(
                    visual_id=VISUAL_ID_BOTTOM_RANKED_WORKER_UTILIZATION,
                    visual_title="<visual-title>Bottom Ranked Worker Utilization</visual-title>",
                    computation_id="BottomRankedWorkerUtilization",
                    column_name="min"
                ),
                insight_visual_top_ranked_for_metrics(
                    visual_id=VISUAL_ID_TOP_RANKED_SKEWNESS_JOB,
                    visual_title="<visual-title>Top Ranked Skewness Job</visual-title>",
                    computation_id="TopRankedSkewnessJob",
                    column_name="max"
                ),
                insight_visual_forecast_for_metrics(
                    visual_id=VISUAL_ID_FORECAST_WORKER_UTILIZATION,
                    visual_title="<visual-title>Forecast Worker Utilization</visual-title>",
                    computation_id="Forecast",
                    column_name="avg"
                ),
                insight_visual_top_mover_for_metrics(
                    visual_id=VISUAL_ID_TOP_MOVER_THROUGHPUT_READ,
                    visual_title="<visual-title>Top Mover readBytes</visual-title>",
                    computation_id="TopMover",
                    column_name="avg"
                )
            ]
        )

        quicksight_definition = quicksight.CfnAnalysis.AnalysisDefinitionProperty(
            data_set_identifier_declarations=[
                quicksight.CfnAnalysis.DataSetIdentifierDeclarationProperty(
                    data_set_arn=quicksight_dataset.attr_arn,
                    identifier="observability_demo.metrics_data"
                )
            ],
            sheets=[
                sheet_monitoring,
                sheet_insights
            ],
            parameter_declarations=[
                quicksight.CfnAnalysis.ParameterDeclarationProperty(
                    string_parameter_declaration=quicksight.CfnAnalysis.StringParameterDeclarationProperty(
                        name="Region",
                        parameter_value_type="MULTI_VALUED",
                        value_when_unset=quicksight.CfnAnalysis.StringValueWhenUnsetConfigurationProperty(
                            value_when_unset_option="RECOMMENDED_VALUE"
                        )
                    )
                ),
                quicksight.CfnAnalysis.ParameterDeclarationProperty(
                    string_parameter_declaration=quicksight.CfnAnalysis.StringParameterDeclarationProperty(
                        name="Account",
                        parameter_value_type="MULTI_VALUED",
                        value_when_unset=quicksight.CfnAnalysis.StringValueWhenUnsetConfigurationProperty(
                            value_when_unset_option="RECOMMENDED_VALUE"
                        )
                    )
                ),
                quicksight.CfnAnalysis.ParameterDeclarationProperty(
                    string_parameter_declaration=quicksight.CfnAnalysis.StringParameterDeclarationProperty(
                        name="JobName",
                        parameter_value_type="MULTI_VALUED",
                        value_when_unset=quicksight.CfnAnalysis.StringValueWhenUnsetConfigurationProperty(
                            value_when_unset_option="RECOMMENDED_VALUE"
                        )
                    )
                ),
                quicksight.CfnAnalysis.ParameterDeclarationProperty(
                    string_parameter_declaration=quicksight.CfnAnalysis.StringParameterDeclarationProperty(
                        name="JobRunId",
                        parameter_value_type="MULTI_VALUED",
                        value_when_unset=quicksight.CfnAnalysis.StringValueWhenUnsetConfigurationProperty(
                            value_when_unset_option="RECOMMENDED_VALUE"
                        )
                    )
                ),
                quicksight.CfnAnalysis.ParameterDeclarationProperty(
                    string_parameter_declaration=quicksight.CfnAnalysis.StringParameterDeclarationProperty(
                        name="Source",
                        parameter_value_type="MULTI_VALUED",
                        value_when_unset=quicksight.CfnAnalysis.StringValueWhenUnsetConfigurationProperty(
                            value_when_unset_option="RECOMMENDED_VALUE"
                        )
                    )
                ),
                quicksight.CfnAnalysis.ParameterDeclarationProperty(
                    string_parameter_declaration=quicksight.CfnAnalysis.StringParameterDeclarationProperty(
                        name="Sink",
                        parameter_value_type="MULTI_VALUED",
                        value_when_unset=quicksight.CfnAnalysis.StringValueWhenUnsetConfigurationProperty(
                            value_when_unset_option="RECOMMENDED_VALUE"
                        )
                    )
                ),
                quicksight.CfnAnalysis.ParameterDeclarationProperty(
                    date_time_parameter_declaration=quicksight.CfnAnalysis.DateTimeParameterDeclarationProperty(
                        name="StartTime",
                        time_granularity="DAY",
                        default_values=quicksight.CfnAnalysis.DateTimeDefaultValuesProperty(
                            rolling_date=quicksight.CfnAnalysis.RollingDateConfigurationProperty(
                                expression="addDateTime(-4, 'WK', truncDate('WK', now()))"
                            )
                        )
                    )
                ),
                quicksight.CfnAnalysis.ParameterDeclarationProperty(
                    date_time_parameter_declaration=quicksight.CfnAnalysis.DateTimeParameterDeclarationProperty(
                        name="EndTime",
                        time_granularity="DAY",
                        default_values=quicksight.CfnAnalysis.DateTimeDefaultValuesProperty(
                            rolling_date=quicksight.CfnAnalysis.RollingDateConfigurationProperty(
                                expression="addDateTime(-1, 'SS', addDateTime(1, 'WK', truncDate('WK', now())))"
                            )
                        )
                    )
                ),
            ],
            filter_groups=[
                filter_group_for_metrics(
                    filter_group_id=FILTER_GROUP_ID_JOB_RUN_ERRORS,
                    filter_id=f"{FILTER_GROUP_ID_JOB_RUN_ERRORS}-filter",
                    match_operator="STARTS_WITH",
                    category_value="glue.error",
                    sheet_id=SHEET_ID_MONITORING,
                    visual_ids=[
                        VISUAL_ID_JOB_RUN_ERRORS_BREAKDOWN,
                        VISUAL_ID_JOB_RUN_ERRORS
                    ]
                ),
                filter_group_for_metrics(
                    filter_group_id=f"{FILTER_GROUP_ID_JOB_RUN_ERRORS}-except-all",
                    filter_id=f"{FILTER_GROUP_ID_JOB_RUN_ERRORS}-except-all-filter",
                    match_operator="DOES_NOT_EQUAL",
                    category_value="glue.error.ALL",
                    sheet_id=SHEET_ID_MONITORING,
                    visual_ids=[
                        VISUAL_ID_JOB_RUN_ERRORS_BREAKDOWN,
                        VISUAL_ID_JOB_RUN_ERRORS
                    ]
                ),
                filter_group_for_metrics(
                    filter_group_id=FILTER_GROUP_ID_SKEWNESS,
                    filter_id=f"{FILTER_GROUP_ID_SKEWNESS}-filter",
                    match_operator="EQUALS",
                    category_value="glue.driver.skewness.job",
                    sheet_id=SHEET_ID_MONITORING,
                    visual_ids=[
                        VISUAL_ID_SKEWNESS_JOB,
                        VISUAL_ID_SKEWNESS_JOB_PER_JOB
                    ]
                ),
                filter_group_for_metrics(
                    filter_group_id=FILTER_GROUP_ID_WORKER_UTILIZATION,
                    filter_id=f"{FILTER_GROUP_ID_WORKER_UTILIZATION}-filter",
                    match_operator="EQUALS",
                    category_value="glue.driver.workerUtilization",
                    sheet_id=SHEET_ID_MONITORING,
                    visual_ids=[
                        VISUAL_ID_WORKER_UTILIZATION,
                        VISUAL_ID_WORKER_UTILIZATION_PER_JOB
                    ]
                ),
                filter_group_for_metrics(
                    filter_group_id=FILTER_GROUP_ID_THROUGHPUT,
                    filter_id=f"{FILTER_GROUP_ID_THROUGHPUT}-filter",
                    match_operator="STARTS_WITH",
                    category_value="glue.driver",
                    sheet_id=SHEET_ID_MONITORING,
                    visual_ids=[
                        VISUAL_ID_THROUGHPUT_READ,
                        VISUAL_ID_THROUGHPUT_WRITE
                    ]
                ),
                filter_group_for_metrics(
                    filter_group_id=FILTER_GROUP_ID_THROUGHPUT_READ,
                    filter_id=f"{FILTER_GROUP_ID_THROUGHPUT_READ}-filter",
                    match_operator="ENDS_WITH",
                    category_value="Read",
                    sheet_id=SHEET_ID_MONITORING,
                    visual_ids=[
                        VISUAL_ID_THROUGHPUT_READ
                    ]
                ),
                filter_group_for_metrics(
                    filter_group_id=FILTER_GROUP_ID_THROUGHPUT_WRITE,
                    filter_id=f"{FILTER_GROUP_ID_THROUGHPUT_WRITE}-filter",
                    match_operator="ENDS_WITH",
                    category_value="Written",
                    sheet_id=SHEET_ID_MONITORING,
                    visual_ids=[
                        VISUAL_ID_THROUGHPUT_WRITE
                    ]
                ),
                filter_group_for_metrics_two_categories(
                    filter_group_id=FILTER_GROUP_ID_DISK_USED,
                    filter_id1=f"{FILTER_GROUP_ID_DISK_USED}-filter1",
                    filter_id2=f"{FILTER_GROUP_ID_DISK_USED}-filter2",
                    match_operator="EQUALS",
                    category_value1="glue.driver.disk.used.percentage",
                    category_value2="glue.ALL.disk.used.percentage",
                    sheet_id=SHEET_ID_MONITORING,
                    visual_ids=[
                        VISUAL_ID_DISK_USED
                    ]
                ),
                filter_group_for_metrics_two_categories(
                    filter_group_id=FILTER_GROUP_ID_DISK_AVAILBLE,
                    filter_id1=f"{FILTER_GROUP_ID_DISK_AVAILBLE}-filter1",
                    filter_id2=f"{FILTER_GROUP_ID_DISK_AVAILBLE}-filter2",
                    match_operator="EQUALS",
                    category_value1="glue.driver.disk.available_GB",
                    category_value2="glue.ALL.disk.available_GB",
                    sheet_id=SHEET_ID_MONITORING,
                    visual_ids=[
                        VISUAL_ID_DISK_AVAILABLE
                    ]
                ),
                filter_group_for_metrics(
                    filter_group_id=FILTER_GROUP_ID_DRIVER_OOM_COUNT,
                    filter_id=f"{FILTER_GROUP_ID_DRIVER_OOM_COUNT}-filter",
                    match_operator="EQUALS",
                    category_value="glue.driver.error.OUT_OF_MEMORY_ERROR",
                    sheet_id=SHEET_ID_MONITORING,
                    visual_ids=[
                        VISUAL_ID_DRIVER_OOM_COUNT
                    ]
                ),
                filter_group_for_metrics(
                    filter_group_id=FILTER_GROUP_ID_DRIVER_HEAP_MEMORY,
                    filter_id=f"{FILTER_GROUP_ID_DRIVER_HEAP_MEMORY}-filter",
                    match_operator="EQUALS",
                    category_value="glue.driver.memory.heap.used.percentage",
                    sheet_id=SHEET_ID_MONITORING,
                    visual_ids=[
                        VISUAL_ID_DRIVER_HEAP_MEMORY
                    ]
                ),
                filter_group_for_metrics(
                    filter_group_id=FILTER_GROUP_ID_EXECUTOR_OOM_COUNT,
                    filter_id=f"{FILTER_GROUP_ID_EXECUTOR_OOM_COUNT}-filter",
                    match_operator="EQUALS",
                    category_value="glue.ALL.error.OUT_OF_MEMORY_ERROR",
                    sheet_id=SHEET_ID_MONITORING,
                    visual_ids=[
                        VISUAL_ID_EXECUTOR_OOM_COUNT
                    ]
                ),
                filter_group_for_metrics(
                    filter_group_id=FILTER_GROUP_ID_EXECUTOR_HEAP_MEMORY,
                    filter_id=f"{FILTER_GROUP_ID_EXECUTOR_HEAP_MEMORY}-filter",
                    match_operator="EQUALS",
                    category_value="glue.ALL.memory.heap.used.percentage",
                    sheet_id=SHEET_ID_MONITORING,
                    visual_ids=[
                        VISUAL_ID_EXECUTOR_HEAP_MEMORY
                    ]
                ),
                filter_group_for_metrics_with_param_all_visuals_date(
                    filter_group_id=FILTER_GROUP_ID_DATE_PARAM,
                    filter_id=f"{FILTER_GROUP_ID_DATE_PARAM}-filter",
                    column_name="date",
                    parameter_name_start="StartTime",
                    parameter_name_end="EndTime",
                    sheet_id=SHEET_ID_MONITORING,
                ),
                filter_group_for_metrics_with_param_all_visuals(
                    filter_group_id=FILTER_GROUP_ID_REGION_PARAM,
                    filter_id=f"{FILTER_GROUP_ID_REGION_PARAM}-filter",
                    column_name="region",
                    match_operator="EQUALS",
                    parameter_name="Region",
                    sheet_id=SHEET_ID_MONITORING,
                ),
                filter_group_for_metrics_with_param_all_visuals(
                    filter_group_id=FILTER_GROUP_ID_ACCOUNT_PARAM,
                    filter_id=f"{FILTER_GROUP_ID_ACCOUNT_PARAM}-filter",
                    column_name="account_id",
                    match_operator="EQUALS",
                    parameter_name="Account",
                    sheet_id=SHEET_ID_MONITORING,
                ),
                filter_group_for_metrics_with_param_all_visuals(
                    filter_group_id=FILTER_GROUP_ID_JOBNAME_PARAM,
                    filter_id=f"{FILTER_GROUP_ID_JOBNAME_PARAM}-filter",
                    column_name="jobname",
                    match_operator="EQUALS",
                    parameter_name="JobName",
                    sheet_id=SHEET_ID_MONITORING,
                ),
                filter_group_for_metrics_with_param_all_visuals(
                    filter_group_id=FILTER_GROUP_ID_JOBRUN_ID_PARAM,
                    filter_id=f"{FILTER_GROUP_ID_JOBRUN_ID_PARAM}-filter",
                    column_name="jobrunid",
                    match_operator="EQUALS",
                    parameter_name="JobRunId",
                    sheet_id=SHEET_ID_MONITORING,
                ),
                filter_group_for_metrics_with_param(
                    filter_group_id=FILTER_GROUP_ID_SOURCE_PARAM,
                    filter_id=f"{FILTER_GROUP_ID_SOURCE_PARAM}-filter",
                    column_name="source",
                    match_operator="EQUALS",
                    parameter_name="Source",
                    sheet_id=SHEET_ID_MONITORING,
                    visual_ids=[
                        VISUAL_ID_THROUGHPUT_READ
                    ]
                ),
                filter_group_for_metrics_with_param(
                    filter_group_id=FILTER_GROUP_ID_SINK_PARAM,
                    filter_id=f"{FILTER_GROUP_ID_SINK_PARAM}-filter",
                    column_name="sink",
                    match_operator="EQUALS",
                    parameter_name="Sink",
                    sheet_id=SHEET_ID_MONITORING,
                    visual_ids=[
                        VISUAL_ID_THROUGHPUT_WRITE
                    ]
                ),
                filter_group_for_metrics(
                    filter_group_id=FILTER_GROUP_ID_WORKER_UTILIZATION_INSIGHT,
                    filter_id=f"{FILTER_GROUP_ID_WORKER_UTILIZATION_INSIGHT}-filter",
                    match_operator="EQUALS",
                    category_value="glue.driver.workerUtilization",
                    sheet_id=SHEET_ID_INSIGHTS,
                    visual_ids=[
                        VISUAL_ID_BOTTOM_RANKED_WORKER_UTILIZATION,
                        VISUAL_ID_FORECAST_WORKER_UTILIZATION
                    ]
                ),
                filter_group_for_metrics(
                    filter_group_id=FILTER_GROUP_ID_SKEWNESS_INSIGHT,
                    filter_id=f"{FILTER_GROUP_ID_SKEWNESS_INSIGHT}-filter",
                    match_operator="EQUALS",
                    category_value="glue.driver.skewness.job",
                    sheet_id=SHEET_ID_INSIGHTS,
                    visual_ids=[
                        VISUAL_ID_TOP_RANKED_SKEWNESS_JOB
                    ]
                ),
            ]
        )
        analysis = quicksight.CfnAnalysis(
            self,
            "ObservabilityAnalysis",
            analysis_id="observability_analysis",
            aws_account_id=account_id,
            name="GlueObservabilityAnalysis",
            definition=quicksight_definition
        )


def parameter_controls_for_sheet(sheet_id):
    return [
        quicksight.CfnAnalysis.ParameterControlProperty(
            dropdown=quicksight.CfnAnalysis.ParameterDropDownControlProperty(
                parameter_control_id=sheet_id+"-Region",
                source_parameter_name="Region",
                title="Region",
                selectable_values=quicksight.CfnAnalysis.ParameterSelectableValuesProperty(
                    link_to_data_set_column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                        column_name="region",
                        data_set_identifier="observability_demo.metrics_data"
                    )
                )
            )
        ),
        quicksight.CfnAnalysis.ParameterControlProperty(
            dropdown=quicksight.CfnAnalysis.ParameterDropDownControlProperty(
                parameter_control_id=sheet_id+"-Account",
                source_parameter_name="Account",
                title="Account",
                selectable_values=quicksight.CfnAnalysis.ParameterSelectableValuesProperty(
                    link_to_data_set_column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                        column_name="account_id",
                        data_set_identifier="observability_demo.metrics_data"
                    )
                )
            )
        ),
        quicksight.CfnAnalysis.ParameterControlProperty(
            dropdown=quicksight.CfnAnalysis.ParameterDropDownControlProperty(
                parameter_control_id=sheet_id+"-JobName",
                source_parameter_name="JobName",
                title="JobName",
                selectable_values=quicksight.CfnAnalysis.ParameterSelectableValuesProperty(
                    link_to_data_set_column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                        column_name="jobname",
                        data_set_identifier="observability_demo.metrics_data"
                    )
                )
            )
        ),
        quicksight.CfnAnalysis.ParameterControlProperty(
            dropdown=quicksight.CfnAnalysis.ParameterDropDownControlProperty(
                parameter_control_id=sheet_id+"-JobRunId",
                source_parameter_name="JobRunId",
                title="JobRunId",
                selectable_values=quicksight.CfnAnalysis.ParameterSelectableValuesProperty(
                    link_to_data_set_column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                        column_name="jobrunid",
                        data_set_identifier="observability_demo.metrics_data"
                    )
                )
            )
        ),
        quicksight.CfnAnalysis.ParameterControlProperty(
            dropdown=quicksight.CfnAnalysis.ParameterDropDownControlProperty(
                parameter_control_id=sheet_id+"-Source",
                source_parameter_name="Source",
                title="Source",
                selectable_values=quicksight.CfnAnalysis.ParameterSelectableValuesProperty(
                    link_to_data_set_column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                        column_name="source",
                        data_set_identifier="observability_demo.metrics_data"
                    )
                )
            )
        ),
        quicksight.CfnAnalysis.ParameterControlProperty(
            dropdown=quicksight.CfnAnalysis.ParameterDropDownControlProperty(
                parameter_control_id=sheet_id+"-Sink",
                source_parameter_name="Sink",
                title="Sink",
                selectable_values=quicksight.CfnAnalysis.ParameterSelectableValuesProperty(
                    link_to_data_set_column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                        column_name="sink",
                        data_set_identifier="observability_demo.metrics_data"
                    )
                )
            )
        ),
        quicksight.CfnAnalysis.ParameterControlProperty(
            date_time_picker=quicksight.CfnAnalysis.ParameterDateTimePickerControlProperty(
                parameter_control_id=sheet_id+"-StartTime",
                source_parameter_name="StartTime",
                title="StartTime",
                display_options=quicksight.CfnAnalysis.DateTimePickerControlDisplayOptionsProperty(
                    date_time_format="YYYY/MM/DD HH:mm:ss",
                    title_options=quicksight.CfnAnalysis.LabelOptionsProperty(
                        visibility="VISIBLE",
                        font_configuration=quicksight.CfnAnalysis.FontConfigurationProperty(
                            font_size=quicksight.CfnAnalysis.FontSizeProperty(
                                relative="MEDIUM"
                            )
                        )
                    )
                )
            )
        ),
        quicksight.CfnAnalysis.ParameterControlProperty(
            date_time_picker=quicksight.CfnAnalysis.ParameterDateTimePickerControlProperty(
                parameter_control_id=sheet_id+"-EndTime",
                source_parameter_name="EndTime",
                title="EndTime",
                display_options=quicksight.CfnAnalysis.DateTimePickerControlDisplayOptionsProperty(
                    date_time_format="YYYY/MM/DD HH:mm:ss",
                    title_options=quicksight.CfnAnalysis.LabelOptionsProperty(
                        visibility="VISIBLE",
                        font_configuration=quicksight.CfnAnalysis.FontConfigurationProperty(
                            font_size=quicksight.CfnAnalysis.FontSizeProperty(
                                relative="MEDIUM"
                            )
                        )
                    )
                )
            )
        ),
    ]

def pie_chart_visual_for_metric(visual_id, visual_title, column_name, value_statistics):
    return quicksight.CfnAnalysis.VisualProperty(
        pie_chart_visual=quicksight.CfnAnalysis.PieChartVisualProperty(
            visual_id=visual_id,
            title=quicksight.CfnAnalysis.VisualTitleLabelOptionsProperty(
                visibility="VISIBLE",
                format_text=quicksight.CfnAnalysis.ShortFormatTextProperty(
                    rich_text=visual_title
                )
            ),
            chart_configuration=quicksight.CfnAnalysis.PieChartConfigurationProperty(
                field_wells=quicksight.CfnAnalysis.PieChartFieldWellsProperty(
                    pie_chart_aggregated_field_wells=quicksight.CfnAnalysis.PieChartAggregatedFieldWellsProperty(
                        category=[
                            quicksight.CfnAnalysis.DimensionFieldProperty(
                                categorical_dimension_field=quicksight.CfnAnalysis.CategoricalDimensionFieldProperty(
                                    field_id=f"{DATASET_KEY}.{FIELD_ID_SUFFIX_METRIC_NAME_1}",
                                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                        column_name="metric_name",
                                        data_set_identifier="observability_demo.metrics_data"
                                    )
                                )
                            )
                        ],
                        values=[
                            quicksight.CfnAnalysis.MeasureFieldProperty(
                                numerical_measure_field=quicksight.CfnAnalysis.NumericalMeasureFieldProperty(
                                    field_id=f"{COLUMN_ID_ANY}.{FIELD_ID_SUFFIX_ANY}",
                                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                        column_name=column_name,
                                        data_set_identifier="observability_demo.metrics_data"
                                    ),
                                    aggregation_function=quicksight.CfnAnalysis.NumericalAggregationFunctionProperty(
                                        simple_numerical_aggregation=value_statistics
                                    )
                                )
                            )
                        ]
                    )
                ),
                sort_configuration=quicksight.CfnAnalysis.PieChartSortConfigurationProperty(
                    category_sort=[
                        quicksight.CfnAnalysis.FieldSortOptionsProperty(
                            field_sort=quicksight.CfnAnalysis.FieldSortProperty(
                                field_id=f"{COLUMN_ID_ANY}.{FIELD_ID_SUFFIX_ANY}",
                                direction="DESC"
                            )
                        )
                    ],
                    category_items_limit=quicksight.CfnAnalysis.ItemsLimitConfigurationProperty(
                        other_categories="INCLUDE"
                    ),
                    small_multiples_limit_configuration=quicksight.CfnAnalysis.ItemsLimitConfigurationProperty(
                        other_categories="INCLUDE"
                    )
                ),
                donut_options=quicksight.CfnAnalysis.DonutOptionsProperty(
                    arc_options=quicksight.CfnAnalysis.ArcOptionsProperty(
                        arc_thickness="WHOLE"
                    )
                ),
                data_labels=quicksight.CfnAnalysis.DataLabelOptionsProperty(
                    visibility="VISIBLE",
                    overlap="DISABLE_OVERLAP"
                ),
                tooltip=quicksight.CfnAnalysis.TooltipOptionsProperty(
                    tooltip_visibility="VISIBLE",
                    selected_tooltip_type="DETAILED",
                    field_based_tooltip=quicksight.CfnAnalysis.FieldBasedTooltipProperty(
                        aggregation_visibility="HIDDEN",
                        tooltip_title_type="PRIMARY_VALUE",
                        tooltip_fields=[
                            quicksight.CfnAnalysis.TooltipItemProperty(
                                field_tooltip_item=quicksight.CfnAnalysis.FieldTooltipItemProperty(
                                    field_id=f"{COLUMN_ID_ANY}.{FIELD_ID_SUFFIX_ANY}",
                                    visibility="VISIBLE"
                                )
                            ),
                            quicksight.CfnAnalysis.TooltipItemProperty(
                                field_tooltip_item=quicksight.CfnAnalysis.FieldTooltipItemProperty(
                                    field_id=f"{DATASET_KEY}.{FIELD_ID_SUFFIX_METRIC_NAME_1}",
                                    visibility="VISIBLE"
                                )
                            )
                        ]
                    )
                )
            )
        )
    )


def line_chart_visual_for_metric(visual_id, visual_title, column_name, color_column_name, value_statistics, chart_type):
    return quicksight.CfnAnalysis.VisualProperty(
        line_chart_visual=quicksight.CfnAnalysis.LineChartVisualProperty(
            visual_id=visual_id,
            title=quicksight.CfnAnalysis.VisualTitleLabelOptionsProperty(
                visibility="VISIBLE",
                format_text=quicksight.CfnAnalysis.ShortFormatTextProperty(
                    rich_text=visual_title
                )
            ),
            chart_configuration=quicksight.CfnAnalysis.LineChartConfigurationProperty(
                field_wells=quicksight.CfnAnalysis.LineChartFieldWellsProperty(
                    line_chart_aggregated_field_wells=quicksight.CfnAnalysis.LineChartAggregatedFieldWellsProperty(
                        category=[
                            quicksight.CfnAnalysis.DimensionFieldProperty(
                                date_dimension_field=quicksight.CfnAnalysis.DateDimensionFieldProperty(
                                    field_id=f"{PARAM_KEY}.{FIELD_ID_SUFFIX_DATE}",
                                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                        column_name="date",
                                        data_set_identifier="observability_demo.metrics_data"
                                    ),
                                    date_granularity="DAY",
                                    hierarchy_id=f"{PARAM_KEY}.{FIELD_ID_SUFFIX_DATE}"
                                )
                            )
                        ],
                        values=[
                            quicksight.CfnAnalysis.MeasureFieldProperty(
                                numerical_measure_field=quicksight.CfnAnalysis.NumericalMeasureFieldProperty(
                                    field_id=f"{COLUMN_ID_ANY}.{FIELD_ID_SUFFIX_ANY}",
                                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                        column_name=column_name,
                                        data_set_identifier="observability_demo.metrics_data"
                                    ),
                                    aggregation_function=quicksight.CfnAnalysis.NumericalAggregationFunctionProperty(
                                        simple_numerical_aggregation=value_statistics
                                    )
                                )
                            )
                        ],
                        colors=[
                            quicksight.CfnAnalysis.DimensionFieldProperty(
                                categorical_dimension_field=quicksight.CfnAnalysis.CategoricalDimensionFieldProperty(
                                    field_id=f"{DATASET_KEY}.{color_column_name}.{FIELD_ID_SUFFIX_ANY}",
                                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                        column_name=color_column_name,
                                        data_set_identifier="observability_demo.metrics_data"
                                    )
                                )
                            )
                        ]
                    )
                ),
                sort_configuration=quicksight.CfnAnalysis.LineChartSortConfigurationProperty(
                    category_sort=[
                        quicksight.CfnAnalysis.FieldSortOptionsProperty(
                            field_sort=quicksight.CfnAnalysis.FieldSortProperty(
                                field_id=f"{PARAM_KEY}.{FIELD_ID_SUFFIX_DATE}",
                                direction="DESC"
                            )
                        )
                    ],
                    category_items_limit_configuration=quicksight.CfnAnalysis.ItemsLimitConfigurationProperty(
                        other_categories="INCLUDE"
                    ),
                    color_items_limit_configuration=quicksight.CfnAnalysis.ItemsLimitConfigurationProperty(
                        other_categories="INCLUDE"
                    ),
                    small_multiples_limit_configuration=quicksight.CfnAnalysis.ItemsLimitConfigurationProperty(
                        other_categories="INCLUDE"
                    )
                ),
                type=chart_type,
                data_labels=quicksight.CfnAnalysis.DataLabelOptionsProperty(
                    visibility="VISIBLE",
                    overlap="DISABLE_OVERLAP"
                )
            ),
            column_hierarchies=[
                quicksight.CfnAnalysis.ColumnHierarchyProperty(
                    date_time_hierarchy=quicksight.CfnAnalysis.DateTimeHierarchyProperty(
                        hierarchy_id=f"{PARAM_KEY}.{FIELD_ID_SUFFIX_DATE}"
                    )
                )
            ]
        )
    )


def combo_chart_visual_for_metric(visual_id, visual_title):
    return quicksight.CfnAnalysis.VisualProperty(
        combo_chart_visual=quicksight.CfnAnalysis.ComboChartVisualProperty(
            visual_id=visual_id,
            title=quicksight.CfnAnalysis.VisualTitleLabelOptionsProperty(
                visibility="VISIBLE",
                format_text=quicksight.CfnAnalysis.ShortFormatTextProperty(
                    rich_text=visual_title
                )
            ),
            chart_configuration=quicksight.CfnAnalysis.ComboChartConfigurationProperty(
                field_wells=quicksight.CfnAnalysis.ComboChartFieldWellsProperty(
                    combo_chart_aggregated_field_wells=quicksight.CfnAnalysis.ComboChartAggregatedFieldWellsProperty(
                        category=[
                            quicksight.CfnAnalysis.DimensionFieldProperty(
                                date_dimension_field=quicksight.CfnAnalysis.DateDimensionFieldProperty(
                                    field_id=f"{PARAM_KEY}.{FIELD_ID_SUFFIX_DATE}",
                                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                        column_name="date",
                                        data_set_identifier="observability_demo.metrics_data"
                                    ),
                                    date_granularity="DAY",
                                    hierarchy_id=f"{PARAM_KEY}.{FIELD_ID_SUFFIX_DATE}"
                                )
                            )
                        ],
                        bar_values=[],
                        line_values=[
                            quicksight.CfnAnalysis.MeasureFieldProperty(
                                categorical_measure_field=quicksight.CfnAnalysis.CategoricalMeasureFieldProperty(
                                    field_id=f"{DATASET_KEY}.{FIELD_ID_SUFFIX_METRIC_NAME_1}",
                                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                        column_name="metric_name",
                                        data_set_identifier="observability_demo.metrics_data"
                                    ),
                                    aggregation_function="COUNT"
                                )
                            )
                        ],
                        colors=[
                            quicksight.CfnAnalysis.DimensionFieldProperty(
                                categorical_dimension_field=quicksight.CfnAnalysis.CategoricalDimensionFieldProperty(
                                    field_id=f"{DATASET_KEY}.{FIELD_ID_SUFFIX_METRIC_NAME_2}",
                                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                        column_name="metric_name",
                                        data_set_identifier="observability_demo.metrics_data"
                                    )
                                )
                            )
                        ]
                    )
                ),
                sort_configuration=quicksight.CfnAnalysis.ComboChartSortConfigurationProperty(
                    category_sort=[
                        quicksight.CfnAnalysis.FieldSortOptionsProperty(
                            field_sort=quicksight.CfnAnalysis.FieldSortProperty(
                                field_id=f"{PARAM_KEY}.{FIELD_ID_SUFFIX_DATE}",
                                direction="ASC"
                            )
                        )
                    ],
                    category_items_limit=quicksight.CfnAnalysis.ItemsLimitConfigurationProperty(
                        other_categories="INCLUDE"
                    ),
                    color_items_limit=quicksight.CfnAnalysis.ItemsLimitConfigurationProperty(
                        other_categories="INCLUDE"
                    )
                ),
                bars_arrangement="STACKED",
                bar_data_labels=quicksight.CfnAnalysis.DataLabelOptionsProperty(
                    visibility="HIDDEN",
                    overlap="DISABLE_OVERLAP"
                ),
                tooltip=quicksight.CfnAnalysis.TooltipOptionsProperty(
                    tooltip_visibility="VISIBLE",
                    selected_tooltip_type="DETAILED",
                    field_based_tooltip=quicksight.CfnAnalysis.FieldBasedTooltipProperty(
                        aggregation_visibility="HIDDEN",
                        tooltip_title_type="PRIMARY_VALUE",
                        tooltip_fields=[
                            quicksight.CfnAnalysis.TooltipItemProperty(
                                field_tooltip_item=quicksight.CfnAnalysis.FieldTooltipItemProperty(
                                    field_id=f"{PARAM_KEY}.{FIELD_ID_SUFFIX_DATE}",
                                    visibility="VISIBLE"
                                )
                            ),
                            quicksight.CfnAnalysis.TooltipItemProperty(
                                field_tooltip_item=quicksight.CfnAnalysis.FieldTooltipItemProperty(
                                    field_id=f"{DATASET_KEY}.{FIELD_ID_SUFFIX_METRIC_NAME_1}",
                                    visibility="VISIBLE"
                                )
                            )
                        ]
                    )
                )
            ),
            column_hierarchies=[
                quicksight.CfnAnalysis.ColumnHierarchyProperty(
                    date_time_hierarchy=quicksight.CfnAnalysis.DateTimeHierarchyProperty(
                        hierarchy_id=f"{PARAM_KEY}.{FIELD_ID_SUFFIX_DATE}"
                    )
                )
            ]
        )
    )


#
def combo_chart_visual_for_metric_min_max_avg(visual_id, visual_title):
    return quicksight.CfnAnalysis.VisualProperty(
        combo_chart_visual=quicksight.CfnAnalysis.ComboChartVisualProperty(
            visual_id=visual_id,
            title=quicksight.CfnAnalysis.VisualTitleLabelOptionsProperty(
                visibility="VISIBLE",
                format_text=quicksight.CfnAnalysis.ShortFormatTextProperty(
                    rich_text=visual_title
                )
            ),
            chart_configuration=quicksight.CfnAnalysis.ComboChartConfigurationProperty(
                field_wells=quicksight.CfnAnalysis.ComboChartFieldWellsProperty(
                    combo_chart_aggregated_field_wells=quicksight.CfnAnalysis.ComboChartAggregatedFieldWellsProperty(
                        category=[
                            quicksight.CfnAnalysis.DimensionFieldProperty(
                                date_dimension_field=quicksight.CfnAnalysis.DateDimensionFieldProperty(
                                    field_id=f"{PARAM_KEY}.{FIELD_ID_SUFFIX_DATE}",
                                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                        column_name="date",
                                        data_set_identifier="observability_demo.metrics_data"
                                    ),
                                    date_granularity="DAY",
                                    hierarchy_id=f"{PARAM_KEY}.{FIELD_ID_SUFFIX_DATE}"
                                )
                            )
                        ],
                        bar_values=[],
                        colors=[],
                        line_values=[
                            quicksight.CfnAnalysis.MeasureFieldProperty(
                                numerical_measure_field=quicksight.CfnAnalysis.NumericalMeasureFieldProperty(
                                    field_id=f"{DATASET_KEY}.{FIELD_ID_SUFFIX_MAX}",
                                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                        column_name="max",
                                        data_set_identifier="observability_demo.metrics_data"
                                    ),
                                    aggregation_function=quicksight.CfnAnalysis.NumericalAggregationFunctionProperty(
                                        simple_numerical_aggregation="MAX"
                                    )
                                )
                            ),
                            quicksight.CfnAnalysis.MeasureFieldProperty(
                                numerical_measure_field=quicksight.CfnAnalysis.NumericalMeasureFieldProperty(
                                    field_id=f"{COLUMN_ID_ANY}.{FIELD_ID_SUFFIX_ANY}",
                                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                        column_name="avg",
                                        data_set_identifier="observability_demo.metrics_data"
                                    ),
                                    aggregation_function=quicksight.CfnAnalysis.NumericalAggregationFunctionProperty(
                                        simple_numerical_aggregation="AVERAGE"
                                    )
                                )
                            ),
                            quicksight.CfnAnalysis.MeasureFieldProperty(
                                numerical_measure_field=quicksight.CfnAnalysis.NumericalMeasureFieldProperty(
                                    field_id=f"{DATASET_KEY}.{FIELD_ID_SUFFIX_MIN}",
                                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                        column_name="min",
                                        data_set_identifier="observability_demo.metrics_data"
                                    ),
                                    aggregation_function=quicksight.CfnAnalysis.NumericalAggregationFunctionProperty(
                                        simple_numerical_aggregation="MIN"
                                    )
                                )
                            ),
                        ]
                    )
                ),
                sort_configuration=quicksight.CfnAnalysis.ComboChartSortConfigurationProperty(
                    category_sort=[
                        quicksight.CfnAnalysis.FieldSortOptionsProperty(
                            field_sort=quicksight.CfnAnalysis.FieldSortProperty(
                                field_id=f"{PARAM_KEY}.{FIELD_ID_SUFFIX_DATE}",
                                direction="ASC"
                            )
                        )
                    ],
                    category_items_limit=quicksight.CfnAnalysis.ItemsLimitConfigurationProperty(
                        other_categories="INCLUDE"
                    ),
                    color_items_limit=quicksight.CfnAnalysis.ItemsLimitConfigurationProperty(
                        other_categories="INCLUDE"
                    )
                ),
                bars_arrangement="STACKED",
                bar_data_labels=quicksight.CfnAnalysis.DataLabelOptionsProperty(
                    visibility="HIDDEN",
                    overlap="DISABLE_OVERLAP"
                )
            ),
            actions=[],
            column_hierarchies=[
                quicksight.CfnAnalysis.ColumnHierarchyProperty(
                    date_time_hierarchy=quicksight.CfnAnalysis.DateTimeHierarchyProperty(
                        hierarchy_id=f"{PARAM_KEY}.{FIELD_ID_SUFFIX_DATE}"
                    )
                )
            ]
        )
    )


def kpi_visual_for_metric(visual_id, visual_title, column_name, value_statistics):
    return quicksight.CfnAnalysis.VisualProperty(
        kpi_visual=quicksight.CfnAnalysis.KPIVisualProperty(
            visual_id=visual_id,
            title=quicksight.CfnAnalysis.VisualTitleLabelOptionsProperty(
                visibility="VISIBLE",
                format_text=quicksight.CfnAnalysis.ShortFormatTextProperty(
                    rich_text=visual_title
                )
            ),
            chart_configuration=quicksight.CfnAnalysis.KPIConfigurationProperty(
                field_wells=quicksight.CfnAnalysis.KPIFieldWellsProperty(
                    values=[
                        quicksight.CfnAnalysis.MeasureFieldProperty(
                            numerical_measure_field=quicksight.CfnAnalysis.NumericalMeasureFieldProperty(
                                field_id=f"{COLUMN_ID_ANY}.{FIELD_ID_SUFFIX_ANY}",
                                column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                    column_name=column_name,
                                    data_set_identifier="observability_demo.metrics_data"
                                ),
                                aggregation_function=quicksight.CfnAnalysis.NumericalAggregationFunctionProperty(
                                    simple_numerical_aggregation=value_statistics
                                )
                            )
                        )
                    ],
                    trend_groups=[
                        quicksight.CfnAnalysis.DimensionFieldProperty(
                            date_dimension_field=quicksight.CfnAnalysis.DateDimensionFieldProperty(
                                field_id=f"{PARAM_KEY}.{FIELD_ID_SUFFIX_DATE}",
                                column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                    column_name="date",
                                    data_set_identifier="observability_demo.metrics_data"
                                ),
                                date_granularity="DAY",
                                hierarchy_id=f"{PARAM_KEY}.{FIELD_ID_SUFFIX_DATE}"
                            )
                        )
                    ]
                ),
                sort_configuration=quicksight.CfnAnalysis.KPISortConfigurationProperty(
                    trend_group_sort=[
                        quicksight.CfnAnalysis.FieldSortOptionsProperty(
                            field_sort=quicksight.CfnAnalysis.FieldSortProperty(
                                field_id=f"{PARAM_KEY}.{FIELD_ID_SUFFIX_DATE}",
                                direction="DESC"
                            )
                        )
                    ]
                ),
                kpi_options=quicksight.CfnAnalysis.KPIOptionsProperty(
                    comparison=quicksight.CfnAnalysis.ComparisonConfigurationProperty(
                        comparison_method="PERCENT_DIFFERENCE"
                    ),
                    primary_value_display_type="ACTUAL",
                    secondary_value_font_configuration=quicksight.CfnAnalysis.FontConfigurationProperty(
                        font_size=quicksight.CfnAnalysis.FontSizeProperty(
                            relative="EXTRA_LARGE"
                        )
                    )
                )
            ),
            column_hierarchies=[
                quicksight.CfnAnalysis.ColumnHierarchyProperty(
                    date_time_hierarchy=quicksight.CfnAnalysis.DateTimeHierarchyProperty(
                        hierarchy_id=f"{PARAM_KEY}.{FIELD_ID_SUFFIX_DATE}"
                    )
                )
            ]
        )
    )


def insight_visual_top_ranked_for_metrics(visual_id, visual_title, computation_id, column_name):
    return quicksight.CfnAnalysis.VisualProperty(
        insight_visual=quicksight.CfnAnalysis.InsightVisualProperty(
            visual_id=visual_id,
            title=quicksight.CfnAnalysis.VisualTitleLabelOptionsProperty(
                visibility="VISIBLE",
                format_text=quicksight.CfnAnalysis.ShortFormatTextProperty(
                    rich_text=visual_title
                )
            ),
            insight_configuration=quicksight.CfnAnalysis.InsightConfigurationProperty(
                computations=[
                    quicksight.CfnAnalysis.ComputationProperty(
                        top_bottom_ranked=quicksight.CfnAnalysis.TopBottomRankedComputationProperty(
                            computation_id=computation_id,
                            name="Top",
                            type="TOP",
                            result_size=10,
                            category=quicksight.CfnAnalysis.DimensionFieldProperty(
                                categorical_dimension_field=quicksight.CfnAnalysis.CategoricalDimensionFieldProperty(
                                    field_id=f"{DATASET_KEY}.{FIELD_ID_SUFFIX_JOBNAME}",
                                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                        column_name="jobname",
                                        data_set_identifier="observability_demo.metrics_data"
                                    )
                                )
                            ),
                            value=quicksight.CfnAnalysis.MeasureFieldProperty(
                                numerical_measure_field=quicksight.CfnAnalysis.NumericalMeasureFieldProperty(
                                    field_id=f"{COLUMN_ID_ANY}.{FIELD_ID_SUFFIX_ANY}",
                                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                        column_name=column_name,
                                        data_set_identifier="observability_demo.metrics_data"
                                    ),
                                    aggregation_function=quicksight.CfnAnalysis.NumericalAggregationFunctionProperty(
                                        simple_numerical_aggregation="AVERAGE"
                                    )
                                )
                            )
                        )
                    )
                ],
                custom_narrative=quicksight.CfnAnalysis.CustomNarrativeOptionsProperty(
                    narrative="""
                    <narrative>
                        Top 
                        <if condition=\"Top.itemsCount &gt; 1\" level=\"inline\">
                            <expression>Top.itemsCount</expression>
                             
                        </if>
                        <expression>Top.categoryField.name</expression>
                         
                        for average
                         
                        <expression>Top.metricField.name</expression>
                        <if condition=\"Top.itemsCount &gt; 1\" level=\"inline\"> are:</if>
                        <if condition=\"Top.itemsCount &lt; 10\" level=\"inline\"> is:</if>
                        <br/>
                        <for level=\"block\" listExpression=\"Top.items\">
                            <ul>
                                <li>
                                    <inline color=\"#21a0d7\"><b><expression>Top.items[index].categoryValue.formattedValue</expression></b></inline>
                                     with 
                                    <expression>Top.items[index].metricValue.formattedValue</expression>
                                </li>
                            </ul>
                        </for>
                        <br/>
                    </narrative>
                    """
                )
            ),
            data_set_identifier="observability_demo.metrics_data"
        )
    )


def insight_visual_bottom_ranked_for_metrics(visual_id, visual_title, computation_id, column_name):
    return quicksight.CfnAnalysis.VisualProperty(
        insight_visual=quicksight.CfnAnalysis.InsightVisualProperty(
            visual_id=visual_id,
            title=quicksight.CfnAnalysis.VisualTitleLabelOptionsProperty(
                visibility="VISIBLE",
                format_text=quicksight.CfnAnalysis.ShortFormatTextProperty(
                    rich_text=visual_title
                )
            ),
            insight_configuration=quicksight.CfnAnalysis.InsightConfigurationProperty(
                computations=[
                    quicksight.CfnAnalysis.ComputationProperty(
                        top_bottom_ranked=quicksight.CfnAnalysis.TopBottomRankedComputationProperty(
                            computation_id=computation_id,
                            name="Bottom",
                            type="BOTTOM",
                            result_size=10,
                            category=quicksight.CfnAnalysis.DimensionFieldProperty(
                                categorical_dimension_field=quicksight.CfnAnalysis.CategoricalDimensionFieldProperty(
                                    field_id=f"{DATASET_KEY}.{FIELD_ID_SUFFIX_JOBNAME}",
                                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                        column_name="jobname",
                                        data_set_identifier="observability_demo.metrics_data"
                                    )
                                )
                            ),
                            value=quicksight.CfnAnalysis.MeasureFieldProperty(
                                numerical_measure_field=quicksight.CfnAnalysis.NumericalMeasureFieldProperty(
                                    field_id=f"{DATASET_KEY}.{FIELD_ID_SUFFIX_MIN}",
                                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                        column_name=column_name,
                                        data_set_identifier="observability_demo.metrics_data"
                                    ),
                                    aggregation_function=quicksight.CfnAnalysis.NumericalAggregationFunctionProperty(
                                        simple_numerical_aggregation="AVERAGE"
                                    )
                                )
                            )
                        )
                    )
                ],
                custom_narrative=quicksight.CfnAnalysis.CustomNarrativeOptionsProperty(
                    narrative="""
                    <narrative>
                        Bottom 
                        <if condition=\"Bottom.itemsCount &gt; 1\" level=\"inline\">
                            <expression>Bottom.itemsCount</expression>
                             
                        </if>
                        <expression>Bottom.categoryField.name</expression>
                         
                        for average
                         
                        <expression>Bottom.metricField.name</expression>
                        <if condition=\"Bottom.itemsCount &gt; 1\" level=\"inline\"> are:</if>
                        <if condition=\"Bottom.itemsCount &lt; 10\" level=\"inline\"> is:</if>
                        <br/>
                        <for level=\"block\" listExpression=\"Bottom.items\">
                            <ul>
                                <li>
                                    <inline color=\"#21a0d7\"><b><expression>Bottom.items[index].categoryValue.formattedValue</expression></b></inline>
                                     with 
                                    <expression>Bottom.items[index].metricValue.formattedValue</expression>
                                </li>
                            </ul>
                        </for>
                        <br/>
                    </narrative>
                    """
                )
            ),
            data_set_identifier="observability_demo.metrics_data"
        )
    )

def insight_visual_forecast_for_metrics(visual_id, visual_title, computation_id, column_name):
    return quicksight.CfnAnalysis.VisualProperty(
        insight_visual=quicksight.CfnAnalysis.InsightVisualProperty(
            visual_id=visual_id,
            title=quicksight.CfnAnalysis.VisualTitleLabelOptionsProperty(
                visibility="VISIBLE",
                format_text=quicksight.CfnAnalysis.ShortFormatTextProperty(
                    rich_text=visual_title
                )
            ),
            insight_configuration=quicksight.CfnAnalysis.InsightConfigurationProperty(
                computations=[
                    quicksight.CfnAnalysis.ComputationProperty(
                        forecast=quicksight.CfnAnalysis.ForecastComputationProperty(
                            computation_id=computation_id,
                            name="ForecastInsight",
                            periods_forward=14,
                            periods_backward=0,
                            prediction_interval=90,
                            seasonality="AUTOMATIC",
                            time=quicksight.CfnAnalysis.DimensionFieldProperty(
                                date_dimension_field=quicksight.CfnAnalysis.DateDimensionFieldProperty(
                                    field_id=f"{PARAM_KEY}.{FIELD_ID_SUFFIX_DATE}",
                                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                        column_name="date",
                                        data_set_identifier="observability_demo.metrics_data"
                                    )
                                )
                            ),
                            value=quicksight.CfnAnalysis.MeasureFieldProperty(
                                numerical_measure_field=quicksight.CfnAnalysis.NumericalMeasureFieldProperty(
                                    field_id=f"{DATASET_KEY}.{FIELD_ID_SUFFIX_MIN}",
                                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                        column_name=column_name,
                                        data_set_identifier="observability_demo.metrics_data"
                                    ),
                                    aggregation_function=quicksight.CfnAnalysis.NumericalAggregationFunctionProperty(
                                        simple_numerical_aggregation="AVERAGE"
                                    )
                                )
                            )
                        )
                    )
                ]
            ),
            actions=[],
            data_set_identifier="observability_demo.metrics_data"
        )
    )


def insight_visual_top_mover_for_metrics(visual_id, visual_title, computation_id, column_name):
    return quicksight.CfnAnalysis.VisualProperty(
        insight_visual=quicksight.CfnAnalysis.InsightVisualProperty(
            visual_id=visual_id,
            title=quicksight.CfnAnalysis.VisualTitleLabelOptionsProperty(
                visibility="VISIBLE",
                format_text=quicksight.CfnAnalysis.ShortFormatTextProperty(
                    rich_text=visual_title
                )
            ),
            insight_configuration=quicksight.CfnAnalysis.InsightConfigurationProperty(
                computations=[
                    quicksight.CfnAnalysis.ComputationProperty(
                        top_bottom_movers=quicksight.CfnAnalysis.TopBottomMoversComputationProperty(
                            computation_id=computation_id,
                            name="TopMovers",
                            mover_size=3,
                            sort_order="PERCENT_DIFFERENCE",
                            type="TOP",
                            time=quicksight.CfnAnalysis.DimensionFieldProperty(
                                date_dimension_field=quicksight.CfnAnalysis.DateDimensionFieldProperty(
                                    field_id=f"{PARAM_KEY}.{FIELD_ID_SUFFIX_DATE}",
                                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                        column_name="date",
                                        data_set_identifier="observability_demo.metrics_data"
                                    )
                                )
                            ),
                            category=quicksight.CfnAnalysis.DimensionFieldProperty(
                                categorical_dimension_field=quicksight.CfnAnalysis.CategoricalDimensionFieldProperty(
                                    field_id=f"{DATASET_KEY}.{FIELD_ID_SUFFIX_JOBNAME}",
                                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                        column_name="jobname",
                                        data_set_identifier="observability_demo.metrics_data"
                                    )
                                )
                            ),
                            value=quicksight.CfnAnalysis.MeasureFieldProperty(
                                numerical_measure_field=quicksight.CfnAnalysis.NumericalMeasureFieldProperty(
                                    field_id=f"{DATASET_KEY}.{FIELD_ID_SUFFIX_MIN}",
                                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                                        column_name=column_name,
                                        data_set_identifier="observability_demo.metrics_data"
                                    ),
                                    aggregation_function=quicksight.CfnAnalysis.NumericalAggregationFunctionProperty(
                                        simple_numerical_aggregation="AVERAGE"
                                    )
                                )
                            )
                        )
                    )
                ]
            ),
            actions=[],
            data_set_identifier="observability_demo.metrics_data"
        )
    )


def filter_group_for_metrics_with_param_all_visuals_date(filter_group_id, filter_id, column_name, parameter_name_start, parameter_name_end, sheet_id):
    return quicksight.CfnAnalysis.FilterGroupProperty(
        filter_group_id=filter_group_id,
        filters=[
            quicksight.CfnAnalysis.FilterProperty(
                time_range_filter=quicksight.CfnAnalysis.TimeRangeFilterProperty(
                    filter_id=filter_id,
                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                        data_set_identifier="observability_demo.metrics_data",
                        column_name=column_name
                    ),
                    include_minimum=True,
                    include_maximum=True,
                    range_minimum_value=quicksight.CfnAnalysis.TimeRangeFilterValueProperty(
                        parameter=parameter_name_start
                    ),
                    range_maximum_value=quicksight.CfnAnalysis.TimeRangeFilterValueProperty(
                        parameter=parameter_name_end
                    ),
                    null_option="NON_NULLS_ONLY",
                    time_granularity="DAY"
                )
            )
        ],
        scope_configuration=quicksight.CfnAnalysis.FilterScopeConfigurationProperty(
            selected_sheets=quicksight.CfnAnalysis.SelectedSheetsFilterScopeConfigurationProperty(
                sheet_visual_scoping_configurations=[
                    quicksight.CfnAnalysis.SheetVisualScopingConfigurationProperty(
                        sheet_id=sheet_id,
                        scope="ALL_VISUALS"
                    )
                ]
            )
        ),
        status="ENABLED",
        cross_dataset="SINGLE_DATASET"
    )


def filter_group_for_metrics_with_param_all_visuals(filter_group_id, filter_id, column_name, match_operator, parameter_name, sheet_id):
    return quicksight.CfnAnalysis.FilterGroupProperty(
        filter_group_id=filter_group_id,
        filters=[
            quicksight.CfnAnalysis.FilterProperty(
                category_filter=quicksight.CfnAnalysis.CategoryFilterProperty(
                    filter_id=filter_id,
                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                        data_set_identifier="observability_demo.metrics_data",
                        column_name=column_name
                    ),
                    configuration=quicksight.CfnAnalysis.CategoryFilterConfigurationProperty(
                        custom_filter_configuration=quicksight.CfnAnalysis.CustomFilterConfigurationProperty(
                            match_operator=match_operator,
                            parameter_name=parameter_name,
                            null_option="NON_NULLS_ONLY"
                        )
                    )
                )
            )
        ],
        scope_configuration=quicksight.CfnAnalysis.FilterScopeConfigurationProperty(
            selected_sheets=quicksight.CfnAnalysis.SelectedSheetsFilterScopeConfigurationProperty(
                sheet_visual_scoping_configurations=[
                    quicksight.CfnAnalysis.SheetVisualScopingConfigurationProperty(
                        sheet_id=sheet_id,
                        scope="ALL_VISUALS"
                    )
                ]
            )
        ),
        status="ENABLED",
        cross_dataset="SINGLE_DATASET"
    )


def filter_group_for_metrics_with_param(filter_group_id, filter_id, column_name, match_operator, parameter_name, sheet_id, visual_ids):
    return quicksight.CfnAnalysis.FilterGroupProperty(
        filter_group_id=filter_group_id,
        filters=[
            quicksight.CfnAnalysis.FilterProperty(
                category_filter=quicksight.CfnAnalysis.CategoryFilterProperty(
                    filter_id=filter_id,
                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                        data_set_identifier="observability_demo.metrics_data",
                        column_name=column_name
                    ),
                    configuration=quicksight.CfnAnalysis.CategoryFilterConfigurationProperty(
                        custom_filter_configuration=quicksight.CfnAnalysis.CustomFilterConfigurationProperty(
                            match_operator=match_operator,
                            parameter_name=parameter_name,
                            null_option="NON_NULLS_ONLY"
                        )
                    )
                )
            )
        ],
        scope_configuration=quicksight.CfnAnalysis.FilterScopeConfigurationProperty(
            selected_sheets=quicksight.CfnAnalysis.SelectedSheetsFilterScopeConfigurationProperty(
                sheet_visual_scoping_configurations=[
                    quicksight.CfnAnalysis.SheetVisualScopingConfigurationProperty(
                        sheet_id=sheet_id,
                        scope="SELECTED_VISUALS",
                        visual_ids=visual_ids
                    )
                ]
            )
        ),
        status="ENABLED",
        cross_dataset="SINGLE_DATASET"
    )


def filter_group_for_metrics(filter_group_id, filter_id, match_operator, category_value, sheet_id, visual_ids):
    return quicksight.CfnAnalysis.FilterGroupProperty(
        filter_group_id=filter_group_id,
        filters=[
            quicksight.CfnAnalysis.FilterProperty(
                category_filter=quicksight.CfnAnalysis.CategoryFilterProperty(
                    filter_id=filter_id,
                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                        data_set_identifier="observability_demo.metrics_data",
                        column_name="metric_name"
                    ),
                    configuration=quicksight.CfnAnalysis.CategoryFilterConfigurationProperty(
                        custom_filter_configuration=quicksight.CfnAnalysis.CustomFilterConfigurationProperty(
                            match_operator=match_operator,
                            category_value=category_value,
                            null_option="NON_NULLS_ONLY"
                        )
                    )
                )
            )
        ],
        scope_configuration=quicksight.CfnAnalysis.FilterScopeConfigurationProperty(
            selected_sheets=quicksight.CfnAnalysis.SelectedSheetsFilterScopeConfigurationProperty(
                sheet_visual_scoping_configurations=[
                    quicksight.CfnAnalysis.SheetVisualScopingConfigurationProperty(
                        sheet_id=sheet_id,
                        scope="SELECTED_VISUALS",
                        visual_ids=visual_ids
                    )
                ]
            )
        ),
        status="ENABLED",
        cross_dataset="SINGLE_DATASET"
    )


def filter_group_for_metrics_two_categories(filter_group_id, filter_id1, filter_id2, match_operator, category_value1, category_value2, sheet_id, visual_ids):
    return quicksight.CfnAnalysis.FilterGroupProperty(
        filter_group_id=filter_group_id,
        filters=[
            quicksight.CfnAnalysis.FilterProperty(
                category_filter=quicksight.CfnAnalysis.CategoryFilterProperty(
                    filter_id=filter_id1,
                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                        data_set_identifier="observability_demo.metrics_data",
                        column_name="metric_name"
                    ),
                    configuration=quicksight.CfnAnalysis.CategoryFilterConfigurationProperty(
                        custom_filter_configuration=quicksight.CfnAnalysis.CustomFilterConfigurationProperty(
                            match_operator=match_operator,
                            category_value=category_value1,
                            null_option="NON_NULLS_ONLY"
                        )
                    )
                )
            ),
            quicksight.CfnAnalysis.FilterProperty(
                category_filter=quicksight.CfnAnalysis.CategoryFilterProperty(
                    filter_id=filter_id2,
                    column=quicksight.CfnAnalysis.ColumnIdentifierProperty(
                        data_set_identifier="observability_demo.metrics_data",
                        column_name="metric_name"
                    ),
                    configuration=quicksight.CfnAnalysis.CategoryFilterConfigurationProperty(
                        custom_filter_configuration=quicksight.CfnAnalysis.CustomFilterConfigurationProperty(
                            match_operator=match_operator,
                            category_value=category_value2,
                            null_option="NON_NULLS_ONLY"
                        )
                    )
                )
            )
        ],
        scope_configuration=quicksight.CfnAnalysis.FilterScopeConfigurationProperty(
            selected_sheets=quicksight.CfnAnalysis.SelectedSheetsFilterScopeConfigurationProperty(
                sheet_visual_scoping_configurations=[
                    quicksight.CfnAnalysis.SheetVisualScopingConfigurationProperty(
                        sheet_id=sheet_id,
                        scope="SELECTED_VISUALS",
                        visual_ids=visual_ids
                    )
                ]
            )
        ),
        status="ENABLED",
        cross_dataset="SINGLE_DATASET"
    )
