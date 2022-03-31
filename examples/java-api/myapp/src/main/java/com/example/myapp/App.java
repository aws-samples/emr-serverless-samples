package com.example.myapp;

import software.amazon.awssdk.services.emrserverless.model.CreateApplicationResponse;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Creating and starting EMR Serverless Spark App");

        EMRServerlessService emr_serverless = new EMRServerlessService();
        CreateApplicationResponse response = emr_serverless.createApplication("sample-java-app", "emr-6.5.0-preview",
                EMRServerlessService.APPLICATION_TYPES.SPARK);

        try {
            emr_serverless.startApplication(response.applicationId());
        } catch (InterruptedException e) {
            System.out.println("Unable to start application");
            e.printStackTrace();
            System.exit(1);
        }

    }
}
