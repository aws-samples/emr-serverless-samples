import aws_cdk as cdk
from aws_cdk import CfnOutput, Duration, Stack
from aws_cdk import aws_cloudwatch as cw
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_emrserverless as emrs
from constructs import Construct


class EMRServerlessStack(Stack):
    serverless_app: emrs.CfnApplication

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.IVpc,
        namespace: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create an EMR 6.6.0 Spark application in a VPC with pre-initialized capacity
        self.serverless_app = emrs.CfnApplication(
            self,
            f"spark_app_{namespace}",
            release_label="emr-6.11.0",
            type="SPARK",
            name=f"spark-app-{namespace}",
            network_configuration=emrs.CfnApplication.NetworkConfigurationProperty(
                subnet_ids=vpc.select_subnets().subnet_ids,
                security_group_ids=[self.create_security_group(vpc).security_group_id],
            ),
            initial_capacity=[
                emrs.CfnApplication.InitialCapacityConfigKeyValuePairProperty(
                    key="Driver",
                    value=emrs.CfnApplication.InitialCapacityConfigProperty(
                        worker_count=2,
                        worker_configuration=emrs.CfnApplication.WorkerConfigurationProperty(
                            cpu="4vCPU", memory="16gb"
                        ),
                    ),
                ),
                emrs.CfnApplication.InitialCapacityConfigKeyValuePairProperty(
                    key="Executor",
                    value=emrs.CfnApplication.InitialCapacityConfigProperty(
                        worker_count=10,
                        worker_configuration=emrs.CfnApplication.WorkerConfigurationProperty(
                            cpu="4vCPU", memory="16gb"
                        ),
                    ),
                ),
            ],
            auto_stop_configuration=emrs.CfnApplication.AutoStopConfigurationProperty(
                enabled=True, idle_timeout_minutes=15
            ),
        )

        cdk.Tags.of(self.serverless_app).add("namespace", namespace)

        # Also create a CloudWatch Dashboard for the above application
        dashboard = self.build_cloudwatch_dashboard(
            self.serverless_app.attr_application_id, namespace
        )

        CfnOutput(self, "ApplicationID", value=self.serverless_app.attr_application_id)
        CfnOutput(self, "CloudWatchDashboardName", value=dashboard.dashboard_name)

    def create_security_group(self, vpc: ec2.IVpc) -> ec2.SecurityGroup:
        return ec2.SecurityGroup(self, "EMRServerlessSG", vpc=vpc)

    def build_cloudwatch_dashboard(
        self, application_id: str, namespace: str
    ) -> cw.Dashboard:
        dashboard = cw.Dashboard(
            self,
            f"EMRServerlessDashboard_{namespace}",
            dashboard_name=f"emr-serverless-{self.serverless_app.name.replace('_','-')}-{self.serverless_app.attr_application_id}",
        )

        # First we have a set of metrics for running workers broken down by the following:
        # - WorkerType (Driver or Executor)
        # - CapacityAllocationType (PreInitCapacity or OnDemandCapacity)
        preinit_driver_running_workers = cw.Metric(
            metric_name="RunningWorkerCount",
            namespace="AWS/EMRServerless",
            dimensions_map={
                "WorkerType": "Spark_Driver",
                "CapacityAllocationType": "PreInitCapacity",
                "ApplicationId": self.serverless_app.attr_application_id,
            },
            statistic="Sum",
            label="Pre-Initialized",
            period=Duration.minutes(1),
        )
        preinit_executor_running_workers = cw.Metric(
            metric_name="RunningWorkerCount",
            namespace="AWS/EMRServerless",
            dimensions_map={
                "WorkerType": "Spark_Executor",
                "CapacityAllocationType": "PreInitCapacity",
                "ApplicationId": self.serverless_app.attr_application_id,
            },
            statistic="Sum",
            label="Pre-Initialized",
            period=Duration.minutes(1),
        )
        ondemand_driver_running_workers = cw.Metric(
            metric_name="RunningWorkerCount",
            namespace="AWS/EMRServerless",
            dimensions_map={
                "WorkerType": "Spark_Driver",
                "CapacityAllocationType": "OnDemandCapacity",
                "ApplicationId": self.serverless_app.attr_application_id,
            },
            statistic="Sum",
            label="OnDemand",
            period=Duration.minutes(1),
        )
        ondemand_executor_running_workers = cw.Metric(
            metric_name="RunningWorkerCount",
            namespace="AWS/EMRServerless",
            dimensions_map={
                "WorkerType": "Spark_Executor",
                "CapacityAllocationType": "OnDemandCapacity",
                "ApplicationId": self.serverless_app.attr_application_id,
            },
            statistic="Sum",
            label="OnDemand",
            period=Duration.minutes(1),
        )

        idle_workers = cw.Metric(
            metric_name="IdleWorkerCount",
            namespace="AWS/EMRServerless",
            dimensions_map={
                "ApplicationId": self.serverless_app.attr_application_id,
            },
            statistic="Sum",
            period=Duration.minutes(1),
        )
        dashboard.add_widgets(
            cw.GaugeWidget(
                title="Pre-Initialized Capacity Worker Utilization %",
                period=Duration.minutes(1),
                width=12,
                metrics=[
                    cw.MathExpression(
                        expression="100*((m1+m2)/(m1+m2+m3))",
                        label="Pre-Init Worker Utilization %",
                        using_metrics={
                            "m1": cw.Metric(
                                metric_name="RunningWorkerCount",
                                namespace="AWS/EMRServerless",
                                dimensions_map={
                                    "WorkerType": "Spark_Driver",
                                    "CapacityAllocationType": "PreInitCapacity",
                                    "ApplicationId": self.serverless_app.attr_application_id,
                                },
                            ),
                            "m2": cw.Metric(
                                metric_name="RunningWorkerCount",
                                namespace="AWS/EMRServerless",
                                dimensions_map={
                                    "WorkerType": "Spark_Executor",
                                    "CapacityAllocationType": "PreInitCapacity",
                                    "ApplicationId": self.serverless_app.attr_application_id,
                                },
                            ),
                            "m3": cw.Metric(
                                metric_name="IdleWorkerCount",
                                namespace="AWS/EMRServerless",
                                dimensions_map={
                                    "ApplicationId": self.serverless_app.attr_application_id,
                                },
                            ),
                        },
                    )
                ],
            ),
            cw.SingleValueWidget(
                title="Running Drivers",
                width=12,
                height=6,
                metrics=[
                    preinit_driver_running_workers,
                    ondemand_driver_running_workers,
                ],
                sparkline=True,
            ),
        )

        dashboard.add_widgets(
            cw.SingleValueWidget(
                title="Available Workers",
                width=12,
                height=6,
                sparkline=True,
                metrics=[
                    cw.MathExpression(
                        expression="m1+m2+m5",
                        label="Pre-Initialized",
                        period=Duration.minutes(1),
                        using_metrics={
                            "m1": preinit_driver_running_workers,
                            "m2": preinit_executor_running_workers,
                            "m5": idle_workers,
                        },
                    ),
                    cw.MathExpression(
                        expression="m3+m4",
                        label="OnDemand",
                        period=Duration.minutes(1),
                        using_metrics={
                            "m3": ondemand_driver_running_workers,
                            "m4": ondemand_executor_running_workers,
                        },
                        color="#ff7f0e",
                    ),
                ],
            ),
            cw.SingleValueWidget(
                title="Running Executors",
                width=12,
                height=6,
                sparkline=True,
                metrics=[
                    preinit_executor_running_workers,
                    ondemand_executor_running_workers,
                ],
            ),
        )

        # Finally we have a whole row dedicate to job state
        job_run_states = [
            "SubmittedJobs",
            "PendingJobs",
            "ScheduledJobs",
            "RunningJobs",
            "SuccessJobs",
            "FailedJobs",
            "CancellingJobs",
            "CancelledJobs",
        ]
        dashboard.add_widgets(
            cw.SingleValueWidget(
                title="Job Runs",
                width=24,
                height=6,
                sparkline=True,
                metrics=[
                    cw.Metric(
                        metric_name=metric,
                        namespace="AWS/EMRServerless",
                        dimensions_map={
                            "ApplicationId": self.serverless_app.attr_application_id,
                        },
                        statistic="Sum",
                        label=metric,
                        period=Duration.minutes(1),
                    )
                    for metric in job_run_states
                ],
            )
        )

        ## BEGIN: APPLICATION METRICS SECTION
        dashboard.add_widgets(
            cw.TextWidget(
                markdown=f"Application Metrics\n---\nApplication metrics shows the capacity used by application **({application_id})**.",
                height=2,
                width=24,
            )
        )

        # Build up a list of metrics across different capacity metrics (cpu, memory, storage)
        capacity_metric_names = ["CPUAllocated", "MemoryAllocated", "StorageAllocated"]
        app_graph_widgets = [
            cw.GraphWidget(
                title=name,
                period=Duration.minutes(1),
                width=12,
                stacked=True,
                left=[
                    cw.MathExpression(
                        expression="m1+m2",
                        label="Pre-Initialized",
                        period=Duration.minutes(1),
                        using_metrics={
                            "m1": cw.Metric(
                                metric_name=name,
                                namespace="AWS/EMRServerless",
                                dimensions_map={
                                    "WorkerType": "Spark_Driver",
                                    "CapacityAllocationType": "PreInitCapacity",
                                    "ApplicationId": self.serverless_app.attr_application_id,
                                },
                                statistic="Sum",
                                label="Pre-Initialized Spark Driver",
                                period=Duration.minutes(1),
                            ),
                            "m2": cw.Metric(
                                metric_name=name,
                                namespace="AWS/EMRServerless",
                                dimensions_map={
                                    "WorkerType": "Spark_Executor",
                                    "CapacityAllocationType": "PreInitCapacity",
                                    "ApplicationId": self.serverless_app.attr_application_id,
                                },
                                statistic="Sum",
                                label="Pre-Initialized Spark Executor",
                                period=Duration.minutes(1),
                            ),
                        },
                    ),
                    cw.MathExpression(
                        expression="m3+m4",
                        label="OnDemand",
                        period=Duration.minutes(1),
                        color="#ff7f0e",
                        using_metrics={
                            "m3": cw.Metric(
                                metric_name=name,
                                namespace="AWS/EMRServerless",
                                dimensions_map={
                                    "WorkerType": "Spark_Driver",
                                    "CapacityAllocationType": "OnDemandCapacity",
                                    "ApplicationId": self.serverless_app.attr_application_id,
                                },
                                statistic="Sum",
                                label="OnDemand Spark Driver",
                                period=Duration.minutes(1),
                            ),
                            "m4": cw.Metric(
                                metric_name=name,
                                namespace="AWS/EMRServerless",
                                dimensions_map={
                                    "WorkerType": "Spark_Executor",
                                    "CapacityAllocationType": "OnDemandCapacity",
                                    "ApplicationId": self.serverless_app.attr_application_id,
                                },
                                statistic="Sum",
                                label="OnDemand Spark Executor",
                                period=Duration.minutes(1),
                            ),
                        },
                    ),
                ],
            )
            for name in capacity_metric_names
        ]

        dashboard.add_widgets(
            cw.GraphWidget(
                title="Running Workers",
                period=Duration.minutes(1),
                width=12,
                stacked=True,
                left=[
                    cw.MathExpression(
                        expression="m1+m2+m5",
                        label="Pre-Initialized",
                        period=Duration.minutes(1),
                        using_metrics={
                            "m1": preinit_driver_running_workers,
                            "m2": preinit_executor_running_workers,
                            "m5": idle_workers,
                        },
                    ),
                    cw.MathExpression(
                        expression="m3+m4",
                        label="OnDemand",
                        period=Duration.minutes(1),
                        using_metrics={
                            "m3": ondemand_driver_running_workers,
                            "m4": ondemand_executor_running_workers,
                        },
                        color="#ff7f0e",
                    ),
                ],
            ),
            app_graph_widgets[0],
        )
        dashboard.add_widgets(*app_graph_widgets[1:])
        ## END: APPLICATION METRICS SECTION

        ## BEGIN: PRE-INITIALIZED CAPACITY METRICS
        dashboard.add_widgets(
            cw.TextWidget(
                markdown=f"Pre-Initialized Capacity Metrics\n---\nShows you the Pre-Initialized Capacity metrics for an Application.",
                height=2,
                width=24,
            )
        )
        dashboard.add_widgets(
            cw.GraphWidget(
                title="Pre-Initialized Capacity: Total Workers",
                period=Duration.minutes(1),
                width=12,
                stacked=True,
                left=[
                    cw.MathExpression(
                        expression="m1+m2+m3",
                        label="Pre-Initialized Total Workers",
                        period=Duration.minutes(1),
                        using_metrics={
                            "m1": preinit_driver_running_workers,
                            "m2": preinit_executor_running_workers,
                            "m3": idle_workers,
                        },
                    )
                ],
            ),
            cw.GraphWidget(
                title="Pre-Initialized Capacity: Worker Utilization %",
                period=Duration.minutes(1),
                width=12,
                stacked=True,
                left=[
                    cw.MathExpression(
                        expression="100*((m1+m2)/(m1+m2+m3))",
                        label="Pre-Initialized Capacity Worker Utilization %",
                        period=Duration.minutes(1),
                        using_metrics={
                            "m1": preinit_driver_running_workers,
                            "m2": preinit_executor_running_workers,
                            "m3": idle_workers,
                        },
                    )
                ],
            ),
        )
        dashboard.add_widgets(
            cw.GraphWidget(
                title="Pre-Initialized Capacity: Idle Workers",
                period=Duration.minutes(1),
                width=12,
                stacked=True,
                left=[idle_workers],
            )
        )
        ## END: PRE-INITIALIZED CAPACITY METRICS

        ## Dynamically generate metrics broken down by drivers, executors and capacity
        for name, worker_type in zip(
            ["Driver", "Executor"], ["Spark_Driver", "Spark_Executor"]
        ):
            dashboard.add_widgets(
                cw.TextWidget(
                    markdown=f"{name} Metrics\n---\n{name} metrics shows you the capacity used by Spark {name}s for Pre-Initialized and On-Demand capacity pools.",
                    height=2,
                    width=24,
                )
            )
            for row in [
                [
                    {"metric": "RunningWorkerCount", "name": f"Running {name}s Count"},
                    {"metric": "CPUAllocated", "name": "CPU Allocated"},
                ],
                [
                    {"metric": "MemoryAllocated", "name": "Memory Allocated"},
                    {"metric": "StorageAllocated", "name": "Storage Allocated"},
                ],
            ]:
                dashboard.add_widgets(
                    cw.GraphWidget(
                        title=row[0]["name"],
                        period=Duration.minutes(1),
                        width=12,
                        stacked=True,
                        left=[
                            cw.Metric(
                                metric_name=row[0]["metric"],
                                namespace="AWS/EMRServerless",
                                dimensions_map={
                                    "WorkerType": worker_type,
                                    "CapacityAllocationType": "PreInitCapacity",
                                    "ApplicationId": self.serverless_app.attr_application_id,
                                },
                                statistic="Sum",
                                label="Pre-Initialized",
                                period=Duration.minutes(1),
                            ),
                            cw.Metric(
                                metric_name=row[0]["metric"],
                                namespace="AWS/EMRServerless",
                                dimensions_map={
                                    "WorkerType": worker_type,
                                    "CapacityAllocationType": "OnDemandCapacity",
                                    "ApplicationId": self.serverless_app.attr_application_id,
                                },
                                statistic="Sum",
                                label="OnDemand",
                                period=Duration.minutes(1),
                            ),
                        ],
                    ),
                    cw.GraphWidget(
                        title=row[1]["name"],
                        period=Duration.minutes(1),
                        width=12,
                        stacked=True,
                        left=[
                            cw.Metric(
                                metric_name=row[1]["metric"],
                                namespace="AWS/EMRServerless",
                                dimensions_map={
                                    "WorkerType": worker_type,
                                    "CapacityAllocationType": "PreInitCapacity",
                                    "ApplicationId": self.serverless_app.attr_application_id,
                                },
                                statistic="Sum",
                                label="Pre-Initialized",
                                period=Duration.minutes(1),
                            ),
                            cw.Metric(
                                metric_name=row[1]["metric"],
                                namespace="AWS/EMRServerless",
                                dimensions_map={
                                    "WorkerType": worker_type,
                                    "CapacityAllocationType": "OnDemandCapacity",
                                    "ApplicationId": self.serverless_app.attr_application_id,
                                },
                                statistic="Sum",
                                label="OnDemand",
                                period=Duration.minutes(1),
                            ),
                        ],
                    ),
                )

        ## END: DYNAMICALLY GENERATED METRICS

        ## BEGIN: JOB METRICS
        dashboard.add_widgets(
            cw.TextWidget(
                markdown=f"Job Metrics\n---\nJob metrics shows you the aggregate number of jobs in each job state.",
                height=2,
                width=24,
            )
        )

        dashboard.add_widgets(
            cw.GraphWidget(
                title="Running Jobs",
                width=12,
                height=6,
                stacked=True,
                left=[
                    cw.Metric(
                        metric_name="RunningJobs",
                        namespace="AWS/EMRServerless",
                        dimensions_map={
                            "ApplicationId": self.serverless_app.attr_application_id,
                        },
                        statistic="Sum",
                        period=Duration.minutes(1),
                    )
                ],
            ),
            cw.GraphWidget(
                title="Successful Jobs",
                width=12,
                height=6,
                stacked=True,
                left=[
                    cw.Metric(
                        metric_name="SuccessJobs",
                        namespace="AWS/EMRServerless",
                        dimensions_map={
                            "ApplicationId": self.serverless_app.attr_application_id,
                        },
                        statistic="Sum",
                        period=Duration.minutes(1),
                        color="#2ca02c",
                    )
                ],
            ),
        )

        dashboard.add_widgets(
            cw.GraphWidget(
                title="Failed Jobs",
                width=12,
                height=6,
                stacked=True,
                left=[
                    cw.Metric(
                        metric_name="FailedJobs",
                        namespace="AWS/EMRServerless",
                        dimensions_map={
                            "ApplicationId": self.serverless_app.attr_application_id,
                        },
                        statistic="Sum",
                        period=Duration.minutes(1),
                        color="#d62728",
                    )
                ],
            ),
            cw.GraphWidget(
                title="Cancelled Jobs",
                width=12,
                height=6,
                stacked=True,
                left=[
                    cw.Metric(
                        metric_name="CancelledJobs",
                        namespace="AWS/EMRServerless",
                        dimensions_map={
                            "ApplicationId": self.serverless_app.attr_application_id,
                        },
                        statistic="Sum",
                        period=Duration.minutes(1),
                        color="#c5b0d5",
                    )
                ],
            ),
        )
        ## END: JOB METRICS
        return dashboard
