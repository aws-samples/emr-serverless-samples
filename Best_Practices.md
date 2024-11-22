# EMR Serverless Best Practices Summary

## Applications

Applications are virtual cluster definitions, but unlike clusters, they are defined once and used indefinitely. You don't need to start, stop, or terminate applications as part of pipelines, unlike EMR Clusters. Once an application is defined, you can submit jobs immediately. 

* Applications startup in around a min.
* Application wake up on job submission and shutdown when idle (specified in `spark.dynamicAllocation.executorIdleTimeout`). 

## Jobs

There are multiple ways to submit jobs - SDK/CLI/API/Airflow/Step Functions etc. See Airflow operators and Step Functions integration.

## Worker sizes

Workers are drivers and executors for Spark jobs. EMR Serverless supports fine grained worker choices - from 1 vCPU/2 GB workers to 16 vCPU/120 GB workers. See link for details on Worker configurations. You specify the worker sizes in your job configuration using Spark properties. e.g. 

```properties
# driver settings
spark.emr-serverless.driver.cores=4
spark.emr-serverless.driver.memory=16G
spark.emr-serverless.driver.disk=200G

# executor settings
spark.emr-serverless.executor.cores=4
spark.emr-serverless.executor.memory=16G
spark.emr-serverless.executor.disk=200G
```
    
#### Note: 
Spark properties for worker sizes are recommended, even if you configure pre-initialized workers. This ensures that if the pre-initialized workers limit is reached, on-demand workers will be launched with the configured worker sizes, rather than default worker sizes.

#### Storage
EMR Serverless does not have HDFS as it is not a Hadoop cluster. S3 should be used as the persistent store. EMR Serverless provides ephemeral local storage (20-200GB, configurable) per worker for standard disks and 20-2000GB, configurable) per worker for shuffle optimized disks. For shuffle heavy use cases, we recommend using 200 GB disks to maximize disk size and throughput using the configurations below:

```    
--conf spark.emr-serverless.executor.disk=200g
--conf spark.emr-serverless.driver.disk=200g
```
  
Also, review EMR Serverless Shuffle Optimized Disks here, to see if it's your use-case needs a large disk per worker (till 2TB/worker) . To configure jobs to use shuffle optimized disks, use below configurations:

```    
--conf spark.emr-serverless.executor.disk.type=shuffle_optimized
--conf spark.emr-serverless.driver.disk.type=shuffle_optimized
```
 
#### Creating applications
Pick a Release and Architecture (Graviton3 or x86_64). Graviton3 has 35% better price-performance so is recommended. In rare cases, some 3rd party libraries may not be compatible with Graviton3.

#### Pre-initialized capacity: [is optional] 
Use this option if you want workers to be readily available for jobs to start up in seconds. You will incur charges for these workers even when idle until the Application goes to 'Stopped' state, which is configured through the 'idleTimeout' property during application creation. When pre-initialized capacity is enabled, application is launched in a single-AZ, which means multi-AZ functionality would not be provided out-of-the-box.

#### Enable pre-initialized capacity only if you have a strict SLA for jobs to startup in seconds
Disable pre-initialized capacity and use on-demand workers for most use cases where workers are acquired in mins.

#### Multi-AZ: 
EMR Serverless applications are multi-AZ enabled out-of-the-box when pre-initialized capacity is not enabled. A single job is however submitted in a single AZ at any one point in time hence there is no cross-AZ latency or cost. If an AZ is ever unhealthy, jobs will be submitted to healthy AZs ensuring business continuity. For applications with pre-initialized capacity enabled, you will need to restart the application to trigger an AZ failover.

#### Disaster Recovery:

##### Worker Failure - 
In case of a worker failure, EMR Serverless will automatically replace the failed worker with a new on-demand worker. In case of failure of a worker from pre-initialized capacity, EMR Serverless will replace the failed worker to meet the configured pre-initialized capacity.

##### Region failure - 
Even though EMR Serverless is multi-AZ enabled out of the box, customers looking for resiliency to region failures can create the application in another region and submit jobs to it during region outages.

#### Custom image settings: 
You can use custom images to bake in files and application dependencies in the EMR image. This feature is not needed for including python packages or including custom JARs, both of which can be stored in S3 and specified during job submission. See Customizing an EMR Serverless image. For examples, see Using custom images with EMR Serverless.

#### Networking: 
You can configure EMR Serverless to connect to resources in your VPC. Without VPC connectivity, a job can access some AWS service endpoints in the same AWS Region. These services include Amazon S3, AWS Glue, Amazon CloudWatch Logs, AWS KMS, AWS Security Token Service, Amazon DynamoDB, and AWS Secrets Manager.

When configuring VPC, multiple subnets in different AZs are recommended to make applications multi-AZ.
Each worker launched uses 1 IP address within a subnet, hence subnets should have enough free IP addresses for workers to launch.
EMR Serverless supports VPC with dual-stack option - IPv4 and IPv6 IP addresses but a subnet with only IPv6 addresses is not supported
Tags: Each application can be configured with tags that can be used for cost attribution. Note that you cannot use tags on job runs for cost attribution. To track cost of individual jobs, you can view the Billed Resource Utilization of a job run in the EMR Studio UI.

#### Monitoring:

You can use a Cloudwatch dashboard to monitor detailed application capacity usage and jobs in each state. See EMR Serverless CloudWatch Dashboard.

With EMR release 7.1 and higher, you can also monitor detailed Spark engine metrics such as JVM heap memory, GC, shuffle information and many more, at a per-job level using Prometheus and Grafana. See Monitor Spark metrics with Amazon Managed Service for Prometheus
Application Capacity Limits: An application scales automatically when jobs are submitted within the scaling boundaries defined e.g. an Application limit of 1000 vCPUs, 4000 GB RAM, 20000 GB Disk will allow the application to scale to that limit.

#### Service Quota: 
All applications in an account within a region is bound by the 'Max concurrent vCPUs per account' service quota. Most accounts start with high default values for this quota. Customers can request this quota to what they need from the Service quota console as well as track quota usage charts over time.

#### Submitting jobs
The StartJobRun API is similar to the familiar spark-submit API.

See Spark job properties for default Spark configuration.

In addition to OSS Spark properties, EMR Serverless provides additional Spark properties that you can configure:

```    
spark.emr-serverless.driver.disk
spark.emr-serverless.executor.disk
spark.emr-serverless.driverEnv.[KEY]
```

EMR Serverless does not support external shuffle service (As of Nov'2024). Make sure not to set spark.shuffle.service.enabled to true.

When submitting jobs, dependencies like script, jars etc. need to be in an S3 bucket in the same region.

To package multiple Python libraries for a PySpark job, you can create isolated Python virtual environments. See Using Python libraries with EMR Serverless.

To use different Python versions, see Using different Python versions with EMR Serverless.

Each job run can assume an IAM role to call other services. See Job runtime roles.

When specifying Spark driver and executor sizes, ensure that you consider the Spark memory overhead (specified with the properties spark.driver.memoryOverhead and spark.executor.memoryOverhead. into account and also review supported Worker configurations. The overhead has a default value of 10% of container memory, with a minimum of 384 MB. so e.g. when submitting a Spark job to use 4 vCPUs, 30 GB set Spark settings to:

```    
spark.emr-serverless.driver.cores=4
spark.emr-serverless.driver.memory=27GB
spark.emr-serverless.executor.cores=4
spark.emr-serverless.executor.memory=27GB
```
    
For Iceberg workloads, which perform table metadata lookups, we recommending using larger drivers than the default.

```    
spark.emr-serverless.driver.cores=8
spark.emr-serverless.driver.memory=60GB
```
      
It is recommended to limit what executors a job can scale to using the setting spark.dynamicAllocation.maxExecutors e.g. the following setting will set maximum executors for a job to 100 (Note: For EMR Serverless applications with release 6.10.0 or higher, the default value for the spark.dynamicAllocation.maxExecutors property is infinity. Earlier releases default to 100)

```   
spark.dynamicAllocation.maxExecutors=100
```
  
If you want a job to run with static executors, you can disable Dynamic Resource Allocation (DRA) and set static number of executors e.g.

```    
spark.dynamicAllocation.enable=false
spark.executor.instances=10
```

EMR Serverless provides one-click application UIs e.g. Spark Live UI for running jobs and Spark History Server for completed jobs, from the EMR Studio UI. Logs are stored for 30 days for free. You can store these logs in your S3 bucket as well as well as us KMS CMK keys to encrypt the logs. For more details, see Encrypting logs.

[Amazon EMR Serverless now offers larger worker sizes, to run more compute and memory-intensive workloads](https://aws.amazon.com/blogs/big-data/amazon-emr-serverless-supports-larger-worker-sizes-to-run-more-compute-and-memory-intensive-workloads/). While traditional GP2 disks with 600 IOPS are well-suited for small to medium jobs under 10TB, the introduction of GP3 "Shuffle Optimized" disks with 3000 IOPS provides the perfect solution for larger workloads demanding high IOPS and throughput.

For Spark jobs optimization recommendations, use [EMR Advisor tool](https://github.com/aws-samples/aws-emr-advisor) to identify and address performance bottlenecks and optimize resource utilization.
