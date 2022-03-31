package com.example.myapp;

import java.util.logging.Logger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.emrserverless.model.Application;
import software.amazon.awssdk.services.emrserverless.model.ApplicationState;
import software.amazon.awssdk.services.emrserverless.model.CreateApplicationRequest;
import software.amazon.awssdk.services.emrserverless.model.CreateApplicationResponse;
import software.amazon.awssdk.services.emrserverless.model.GetApplicationRequest;
import software.amazon.awssdk.services.emrserverless.model.GetApplicationResponse;
import software.amazon.awssdk.services.emrserverless.model.StartApplicationRequest;

public class EMRServerlessService {
    private static final Logger log = Logger.getLogger(EMRServerlessService.class.getName());

    public enum APPLICATION_TYPES {
        HIVE, SPARK
    }

    private EmrServerlessClient getClient() {
        return EmrServerlessClient.builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(Region.US_EAST_1)
                .httpClient(UrlConnectionHttpClient.builder().build())
                .build();
    }

    public CreateApplicationResponse createApplication(String applicationName, String releaseLabel, APPLICATION_TYPES applicationType) {
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
}
