#!/bin/bash
command -v aws
if [ $? -eq 0 ]; then
  aws emr create-cluster --release-label emr-5.26.0 --use-default-roles --applications Name=Spark Name=Hadoop \
          --ec2-attributes KeyName=aws-emr-key \
          --instance-fleets \
          InstanceFleetType=MASTER,TargetSpotCapacity=1,InstanceTypeConfigs=['{InstanceType=m4.large}'],LaunchSpecifications={SpotSpecification='{TimeoutDurationMinutes=10,TimeoutAction=TERMINATE_CLUSTER}'} \
          InstanceFleetType=CORE,TargetSpotCapacity=2,InstanceTypeConfigs=['{InstanceType=m4.large}'],LaunchSpecifications={SpotSpecification='{TimeoutDurationMinutes=10,TimeoutAction=TERMINATE_CLUSTER}'}
else
  echo "not an ES2 server"
  exit 2
fi
