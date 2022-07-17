#!/bin/bash

#An argument specifying the environment is required for the job to be created
export SME_TARGET_ENV=${1}

source ../../../../env/smeEnvDBSetup.sh $SME_TARGET_ENV

sed "s/<instance_profile_arn_placeholder>/${JOB_INSTANCE_PROFILE_ARN}/g;s/<environment_placeholder>/${ENVIRONMENT}/g" NewsStreaming-Cassandra_template.json > NewsStreaming-Cassandra.json

#drop and recreate the job
databricks jobs list --output json | jq '.jobs[] | select(.settings.name == "SME-OmniSearch-Streaming") | .job_id' | xargs -n 1 databricks jobs delete --job-id
databricks jobs create --json-file ./NewsStreaming-Cassandra.json

#get the new job id
jobid=$(databricks jobs list --output json | jq '.jobs[] | select(.settings.name == "SME-OmniSearch-Streaming") | .job_id')
echo $jobid

#Add permissions to the job
curl --netrc-file ~/.netrc --request PATCH -H 'Content-Type: application/json' \
--data "$JOB_ACL" https://$DATABRICKS_INSTANCE/api/2.0/preview/permissions/jobs/$jobid/