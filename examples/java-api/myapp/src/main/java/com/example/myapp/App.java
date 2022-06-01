package com.example.myapp;

import java.util.concurrent.Callable;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import software.amazon.awssdk.services.emrserverless.model.ConfigurationOverrides;
import software.amazon.awssdk.services.emrserverless.model.CreateApplicationResponse;
import software.amazon.awssdk.services.emrserverless.model.JobDriver;
import software.amazon.awssdk.services.emrserverless.model.MonitoringConfiguration;
import software.amazon.awssdk.services.emrserverless.model.S3MonitoringConfiguration;
import software.amazon.awssdk.services.emrserverless.model.SparkSubmit;
import software.amazon.awssdk.services.emrserverless.model.StartJobRunRequest;
import software.amazon.awssdk.services.emrserverless.model.StartJobRunResponse;
import software.amazon.awssdk.services.emrserverless.model.SparkSubmit.Builder;

/**
 * EMR Serverless demo that starts a Spark application, runs a job, and shuts
 * the application back down.
 *
 */
@Command(name = "myapp", mixinStandardHelpOptions = true, version = "1.0")
public class App implements Callable<Integer> {
    // @formatter:off
    @Option(
        names = { "-b", "--bucket" },
        scope = CommandLine.ScopeType.INHERIT,
        description = "The bucket to use for output and logs. Example: \"emr-demo-us-east-1\"",
        required = true
    ) protected String bucket;

    @Option(
        names = { "-r", "--role-arn" },
        scope = CommandLine.ScopeType.INHERIT,
        description = "The role ARN to use when running the Spark job."
                    +
                    " Example: \"arn:aws:iam::123456789012:role/emr-serverless-job-role\"",
        required = true
    ) protected String roleArn;

    @Option(
        names = { "-sc", "--spark-conf" },
        scope = CommandLine.ScopeType.INHERIT,
        description = "The Spark configuration parameters." +
                    " Defaults to '--class org.apache.spark.examples.SparkPi --conf spark.executor.cores=1 --conf spark.executor.memory=4g --conf" +
                    " spark.driver.cores=1 --conf spark.driver.memory=4g --conf spark.executor.instances=1'."
    )
    protected String sparkParams = "--class org.apache.spark.examples.SparkPi --conf spark.executor.cores=1 --conf spark.executor.memory=4g --conf " +
            "spark.driver.cores=1 --conf spark.driver.memory=4g --conf spark.executor.instances=1";

    @Option(
        names = { "-sa", "--spark-args" },
        scope = CommandLine.ScopeType.INHERIT,
        description = "A comma-seperated list of arguments for the spark job." +
                    " Defaults to `10`"
    ) protected String sparkArgs = "10";

    @Option(
        names = { "-ep", "--entry-point" },
        scope = CommandLine.ScopeType.INHERIT,
        description = "The entrypoint for the spark job." +
                    " (Defaults to `local:///usr/lib/spark/examples/jars/spark-examples.jar`"
    ) protected String entryPoint = "local:///usr/lib/spark/examples/jars/spark-examples.jar";
    // @formatter:on

    @Override
    public Integer call() {
        System.out.println("Creating and starting EMR Serverless Spark App");

        // Create and start a Spark application on EMR 6.5.0
        EMRServerlessService emr_serverless = new EMRServerlessService();
        CreateApplicationResponse response = emr_serverless.createApplication("sample-java-app",
                "emr-6.6.0",
                EMRServerlessService.APPLICATION_TYPES.SPARK);

        String appId = response.applicationId();

        try {
            emr_serverless.startApplication(appId);
        } catch (InterruptedException e) {
            System.out.println("Unable to start application");
            e.printStackTrace();
            return 1;
        }

        // Run a sample SparkPi job
        Builder sparkSubmit = SparkSubmit.builder()
                .entryPoint(entryPoint)
                .sparkSubmitParameters(sparkParams);
        if (sparkArgs.length() > 0) {
            String[] args = sparkArgs.split(",");
            sparkSubmit.entryPointArguments(args);
        }

        StartJobRunRequest jobRunRequest = StartJobRunRequest.builder().applicationId(appId)
                .executionRoleArn(roleArn)
                .jobDriver(JobDriver.builder()
                        .sparkSubmit(sparkSubmit.build())
                        .build())
                .configurationOverrides(ConfigurationOverrides.builder()
                        .monitoringConfiguration(MonitoringConfiguration.builder()
                                .s3MonitoringConfiguration(S3MonitoringConfiguration
                                        .builder()
                                        .logUri("s3://" + bucket
                                                + "/emr-serverless/logs")
                                        .build())
                                .build())
                        .build())
                .build();

        StartJobRunResponse jobResponse = emr_serverless.startJobRun(jobRunRequest);
        try {
            emr_serverless.waitForJobToComplete(appId, jobResponse.jobRunId());
        } catch (InterruptedException e) {
            System.out.println("Unable to run job");
            e.printStackTrace();
            return 1;
        }

        // Now stop and delete the application
        emr_serverless.stopApplication(appId);
        try {
            emr_serverless.deleteApplication(appId);
        } catch (InterruptedException e) {
            System.out.println("Unable to delete application");
            e.printStackTrace();
            return 1;
        }

        return 0;
    }

    public static void main(String... args) {
        System.exit(new CommandLine(new App()).execute(args));
    }
}
