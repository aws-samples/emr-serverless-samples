package com.example.myapp;

import java.util.Set;
import java.util.logging.Logger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.emrserverless.model.Application;
import software.amazon.awssdk.services.emrserverless.model.ApplicationState;
import software.amazon.awssdk.services.emrserverless.model.CreateApplicationRequest;
import software.amazon.awssdk.services.emrserverless.model.CreateApplicationResponse;
import software.amazon.awssdk.services.emrserverless.model.DeleteApplicationRequest;
import software.amazon.awssdk.services.emrserverless.model.GetApplicationRequest;
import software.amazon.awssdk.services.emrserverless.model.GetApplicationResponse;
import software.amazon.awssdk.services.emrserverless.model.GetJobRunRequest;
import software.amazon.awssdk.services.emrserverless.model.GetJobRunResponse;
import software.amazon.awssdk.services.emrserverless.model.JobRun;
import software.amazon.awssdk.services.emrserverless.model.JobRunState;
import software.amazon.awssdk.services.emrserverless.model.StartApplicationRequest;
import software.amazon.awssdk.services.emrserverless.model.StartJobRunRequest;
import software.amazon.awssdk.services.emrserverless.model.StartJobRunResponse;
import software.amazon.awssdk.services.emrserverless.model.StopApplicationRequest;
import software.amazon.awssdk.services.emrserverless.model.StopApplicationResponse;

public class EMRServerlessService {
    private static final Logger log = Logger.getLogger(EMRServerlessService.class.getName());

    public enum APPLICATION_TYPES {
        HIVE, SPARK
    }

    private static final Set<JobRunState> terminalStates = Set.of(JobRunState.CANCELLED, JobRunState.FAILED,
            JobRunState.SUCCESS);

    private EmrServerlessClient getClient() {
        return EmrServerlessClient.builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .httpClient(UrlConnectionHttpClient.builder().build())
                .build();
    }

    public CreateApplicationResponse createApplication(String applicationName, String releaseLabel,
            APPLICATION_TYPES applicationType) {
        EmrServerlessClient client = getClient();
        CreateApplicationRequest car = CreateApplicationRequest.builder().name(applicationName)
                .releaseLabel(releaseLabel).type(applicationType.name()).build();

        return client.createApplication(car);
    }

    public void startApplication(String applicationId) throws InterruptedException {
        log.info(String.format("Starting app %s", applicationId));
        EmrServerlessClient client = getClient();
        int retries = 0;

        Application app = null;
        while (retries < 100) {
            GetApplicationRequest.Builder builder = GetApplicationRequest.builder().applicationId(applicationId);
            GetApplicationResponse getApplicationResponse = client.getApplication(builder.build());
            app = getApplicationResponse.application();
            log.info(String.format("\tCurrent Application State: %s", app.state()));

            if (app.state() == ApplicationState.CREATED || app.state() == ApplicationState.STOPPED) {
                log.info("\tCalling startApplication on the api and waiting...");
                client.startApplication(StartApplicationRequest.builder().applicationId(applicationId).build());
                // start first time we know it's slow.
                Thread.sleep(30000);
            } else {
                if (app.state() == ApplicationState.STARTED) {
                    log.info("\tApplication STARTED");
                    break;
                }

                Thread.sleep(10000);
            }

            retries++;
        }

        if (app == null || app.state() != ApplicationState.STARTED) {
            throw new RuntimeException("Application could not be started after 100 retries" + applicationId);
        }
    }

    public StartJobRunResponse startJobRun(StartJobRunRequest jobRunRequest) {
        log.info(String.format("Starting job run"));
        EmrServerlessClient client = getClient();

        return client.startJobRun(jobRunRequest);
    }

    public void waitForJobToComplete(String applicationId, String jobRunId) throws InterruptedException {
        log.info(String.format("Waiting for job %s/%s", applicationId, jobRunId));
        EmrServerlessClient client = getClient();
        int retries = 0;

        JobRun job = null;
        while (retries < 100) {
            GetJobRunRequest.Builder builder = GetJobRunRequest.builder().applicationId(applicationId)
                    .jobRunId(jobRunId);
            GetJobRunResponse getJobResponse = client.getJobRun(builder.build());
            job = getJobResponse.jobRun();
            log.info(String.format("\tCurrent Job State: %s", job.state()));

            if (!terminalStates.contains(job.state())) {
                Thread.sleep(10000);
            } else {
                break;
            }

            retries++;
        }

        if (job == null || !terminalStates.contains(job.state())) {
            throw new RuntimeException("Job was not finished after 100 retries" + jobRunId);
        }
    }

    public StopApplicationResponse stopApplication(String applicationId) {
        EmrServerlessClient client = getClient();
        StopApplicationRequest stopApp = StopApplicationRequest.builder().applicationId(applicationId).build();

        return client.stopApplication(stopApp);
    }

    public void deleteApplication(String applicationId) throws InterruptedException {
        log.info(String.format("Deleting app %s", applicationId));
        EmrServerlessClient client = getClient();
        int retries = 0;

        Application app = null;
        while (retries < 100) {
            GetApplicationRequest.Builder builder = GetApplicationRequest.builder().applicationId(applicationId);
            GetApplicationResponse getApplicationResponse = client.getApplication(builder.build());
            app = getApplicationResponse.application();
            log.info(String.format("\tCurrent Application State: %s", app.state()));

            if (app.state() != ApplicationState.STOPPED) {
                log.info("\tApplication must be in STOPPED state, waiting...");
            } else {
                log.info("\tApplication STOPPED, calling delete.");
                client.deleteApplication(DeleteApplicationRequest.builder().applicationId(applicationId).build());
                break;
            }
            Thread.sleep(10000);

            retries++;
        }

        if (app == null || app.state() != ApplicationState.STOPPED) {
            throw new RuntimeException("Application could not be stopped after 100 retries" + applicationId);
        }
    }
}
