# EMR Serverless Custom Images

Custom images are now supported in EMR Serverless allowing you to make use of containers to create reproducible data pipelines.

In this example, we use a simple example of adding the `seaborn` library to build a weather visualization.

## Pre-requisities

> [!IMPORTANT]
> This example is intended to be run in the `us-east-1` region as it reads data from [NOAA Global Surface Summary of Day dataset](https://registry.opendata.aws/noaa-gsod/) from the Registry of Open Data.

In order to make use of custom images in EMR, you'll need to have:
- a local installation of Docker to build your image
- an ECR repository to host the resulting image.

We'll assume the user you're using has access to create and update ECR repositories, create EMR Serverless applications, and has access to the AWS CLI.

Set up some variables to be used throughout.

```bash
AWS_REGION=us-east-1
S3_BUCKET=<your-bucket-name>
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
JOB_ROLE_ARN=arn:aws:iam::${ACCOUNT_ID}:role/<your-emr-serverless-job-role>
```

## Build and publish 

We'll follow the docs for [customizing an image for EMR Serverless](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/application-custom-image.html).

- Create an ECR repository to publish to

```bash
aws ecr create-repository \
    --repository-name spark-seaborn
```

- Allow any EMR Serverless application to access the custom image

```bash
aws ecr set-repository-policy \
    --repository-name spark-seaborn \
    --policy-text '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "Emr Serverless Custom Image Support",
                "Effect": "Allow",
                "Principal": {
                    "Service": "emr-serverless.amazonaws.com"
                },
                "Action": [
                    "ecr:BatchGetImage",
                    "ecr:DescribeImages",
                    "ecr:GetDownloadUrlForLayer"
                ],
                "Condition":{
                    "StringLike": {
                        "aws:SourceArn": "arn:aws:emr-serverless:'${AWS_REGION}':'${ACCOUNT_ID}':/applications/*"
                    }
                }
            }
        ]
    }'
```

- Build the image

```bash
docker build . -t $ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/spark-seaborn:latest
```

- Login to ECR and push

```bash
# login to ECR repo
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

# push the docker image
docker push $ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/spark-seaborn:latest
```

- Now create an EMR Serverless application with that image

_Note that you can create different images for your driver and executor using the `worker-type-specifications` parameter - see the [CLI instructions in the docs](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/application-custom-image.html#create-app)._

```shell
aws emr-serverless create-application \
    --name spark-seaborn \
    --release-label emr-6.9.0 \
    --type SPARK \
    --image-configuration '{
        "imageUri": "'${ACCOUNT_ID}'.dkr.ecr.'${AWS_REGION}'.amazonaws.com/spark-seaborn:latest"
    }' \
    --initial-capacity '{
        "DRIVER": {
            "workerCount": 1,
            "workerConfiguration": {
                "cpu": "4vCPU",
                "memory": "16GB"
            }
        },
        "EXECUTOR": {
            "workerCount": 3,
            "workerConfiguration": {
                "cpu": "4vCPU",
                "memory": "16GB"
            }
        }
    }'
```

- And start the resulting application

```bash
APPLICATION_ID=00f7hdef7siki109
aws emr-serverless start-application --application-id ${APPLICATION_ID}
```

- Now let's upload our pyspark script and start a job!

```bash
aws s3 cp noaa_slugplot.py s3://${S3_BUCKET}/code/pyspark/ 
```

```bash
aws emr-serverless start-job-run \
    --name noaa-slugplot-seattle \
    --application-id $APPLICATION_ID \
    --execution-role-arn $JOB_ROLE_ARN \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://'${S3_BUCKET}'/code/pyspark/noaa_slugplot.py",
            "entryPointArguments": [ "72793524234", "2022", "'${S3_BUCKET}'", "tmp/slugplots/seattle-2022.png" ]
        }
    }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://'${S3_BUCKET}'/logs/"
            }
        }
    }'
```

- Wait for the job to finish

```bash
JOB_RUN_ID=00f7hqip55r36l09

aws emr-serverless get-job-run \  
    --application-id $APPLICATION_ID \
    --job-run-id $JOB_RUN_ID
```

- Then copy down the resulting file!

```bash
aws s3 cp s3://${S3_BUCKET}/tmp/slugplots/seattle-2022.png .
```

## Cleanup

- Stop and delete the application and ECR repository

```bash
aws emr-serverless stop-application --application-id ${APPLICATION_ID}
aws emr-serverless delete-application --application-id ${APPLICATION_ID}
aws ecr delete-repository --repository-name spark-seaborn --force
```
