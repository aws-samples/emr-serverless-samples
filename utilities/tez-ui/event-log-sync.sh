job_path=/tmp/timeline-data/$JOB_RUN_ID
mkdir -p $job_path/active
mkdir -p $job_path/done

while [[ true ]]; do
    aws s3 sync $S3_LOG_URI/applications/$APPLICATION_ID/jobs/$JOB_RUN_ID/ytslogs/active $job_path/active  --exclude '*$folder$*'
    # Hack to move done events to active folder as ATS doesnt read done path
    aws s3 sync $S3_LOG_URI/applications/$APPLICATION_ID/jobs/$JOB_RUN_ID/ytslogs/done $job_path/done_events/  --exclude '*$folder$*'
    if [ -d "$job_path/done_events/*/*/*/*" ]; then
        rsync -r $job_path/done_events/*/*/*/* $job_path/active/
    fi
    echo " `date +%s` sleeping for 30 seconds"
    sleep 30s
done