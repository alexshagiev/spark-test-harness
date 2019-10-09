import sys
import getopt
import os
import json
import logging
import time
from tqdm import tqdm
from subprocess import run, PIPE
import generate_jsonl_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_testing = True


def show_help():
    print(
        'create-cluster-populate-data.py --spot-core-capacity 2 --hdfs-port                   # will use input stream')
    print('create-cluster-populate-data.py -i <path_to_status_file.json> # will use file location to pull the status')


def create_cluster(timeout: int, cores: int) -> str:
    cmd = 'cat ./../tests/resources/aws/create-cluster.json' if _testing else \
        "aws emr create-cluster --release-label emr-5.26.0 --use-default-roles \
--applications Name=Spark Name=Hadoop \
--ec2-attributes KeyName=aws-emr-key \
--instance-fleets \
InstanceFleetType=MASTER,TargetSpotCapacity=1,InstanceTypeConfigs=['{InstanceType=m4.large}'],LaunchSpecifications={SpotSpecification='{TimeoutDurationMinutes={timeout},TimeoutAction=TERMINATE_CLUSTER}'} \
InstanceFleetType=CORE,TargetSpotCapacity={cores},InstanceTypeConfigs=['{InstanceType=m4.large}'],LaunchSpecifications={SpotSpecification='{TimeoutDurationMinutes={timeout},TimeoutAction=TERMINATE_CLUSTER}'} \
".format(cores=cores, timeout=timeout)

    logger.info("exec: {}".format(cmd))
    result = run([cmd], check=True, shell=True, universal_newlines=True, stdout=PIPE, stderr=PIPE).stdout
    cluster_id = json.loads(result)['ClusterId']
    logger.info('ClusterId: {}, details: {}'.format(cluster_id, result.replace('\n', '').replace('  ', '')))
    return cluster_id


def describe_cluster(cluster_id: str, attempt: int = 1) -> str:
    cmd = 'cat ./../tests/resources/aws/describe-cluster-{}.json'.format(attempt) if _testing else \
        'aws emr describe-cluster --cluster-id "{}"'.format(cluster_id)
    logger.info("exec: {}".format(cmd))
    result = run([cmd], check=True, shell=True, universal_newlines=True, stdout=PIPE, stderr=PIPE)
    return result.stdout


def get_aws_emr_public_master_dns_name_on_waiting(cluster_id: str, timeout: int) -> str:
    attempt = 0
    state = ''
    t_end = time.time() + timeout * 60 + 10  # attempt for 10 seconds longer than timeout to create an AWS EMR cluster
    while time.time() < t_end:
        attempt += 1
        result = describe_cluster(cluster_id, attempt)
        j = json.loads(result)
        state = j['Cluster']['Status']['State']
        master_public_dns = j['Cluster'].get('MasterPublicDnsName')

        if state == 'WAITING':
            return master_public_dns
        else:
            logger.info(
                'Cluster Creation not complete. Timeout: {}min, Attempt: {}, State: {}, master_public_dns: {}, details: {}'.format(
                    timeout, attempt, state, master_public_dns,
                    result.replace('\n', '').replace('  ',
                                                     '')))
            for i in tqdm(range(5)):
                time.sleep(1)

    raise Exception(
        'Cluster: {} did not reach State "WAITING" after timeout: {}min. Current State: {}'.format(cluster_id,
                                                                                                   timeout, state))


def main(argv):
    # config_file = './application.conf'
    # try:
    #     opts, args = getopt.getopt(argv[1:], "i:", ["input="])
    # except getopt.GetoptError:
    #     show_help()
    #     sys.exit(2)
    #
    # for opt, arg in opts:
    #     if opt == '-h':
    #         show_help()
    #         sys.exit()
    #     elif opt in ("-c", "--config"):
    #         config_file = arg
    #     elif opt in ("-o", "--output"):
    #         output = arg
    #
    # logger.info('Loading config file:{}'.format(config_file))
    timeout = 1
    cores = 2
    cluster_id = create_cluster(timeout, cores)
    #
    host_name = get_aws_emr_public_master_dns_name_on_waiting(cluster_id, timeout)
    logger.info(host_name)
    default_fs = 'hdfs://localhost:9000' if _testing else 'hdfs://{}:{}'.format(host_name,'8020')

    generate_jsonl_data.main([sys.argv[0], '-c', './../../src/main/resources/application.conf', '-o', 'hdfs', '--default-fs', default_fs])


if __name__ == "__main__":
    main(sys.argv)