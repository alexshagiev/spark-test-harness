import sys
import getopt
import os
import json
import logging
import time
from tqdm import tqdm, trange
from subprocess import run, PIPE
import generate_jsonl_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_cluster(timeout: int, cores: int, local_test_mode: bool) -> str:
    cmd = 'cat ./tests/resources/aws/create-cluster.json' if local_test_mode else \
        "aws emr create-cluster \
        --release-label emr-5.26.0 --use-default-roles --applications Name=Spark Name=Hadoop \
        --ec2-attributes KeyName=aws-emr-key \
        --instance-fleets \
        InstanceFleetType=MASTER,TargetSpotCapacity=1,InstanceTypeConfigs=['{InstanceType=m4.large}'],LaunchSpecifications={SpotSpecification='{TimeoutDurationMinutes=" + str(
            timeout) + ",TimeoutAction=TERMINATE_CLUSTER}'} \
        InstanceFleetType=CORE,TargetSpotCapacity=" + str(
            cores) + ",InstanceTypeConfigs=['{InstanceType=m4.large}'],LaunchSpecifications={SpotSpecification='{TimeoutDurationMinutes=" + str(
            timeout) + ",TimeoutAction=TERMINATE_CLUSTER}'} \
        "

    logger.info("exec: {}".format(cmd))
    result = run([cmd], shell=True, universal_newlines=True, stdout=PIPE, stderr=PIPE)
    if result.returncode != 0:
        raise Exception(result.stderr)

    cluster_id = json.loads(result.stdout)['ClusterId']
    logger.info('ClusterId: {}, details: {}'.format(cluster_id, result.stdout.replace('\n', '').replace('  ', '')))
    return cluster_id


def describe_cluster(cluster_id: str, attempt: int, local_test_mode: bool) -> str:
    cmd = 'cat ./tests/resources/aws/describe-cluster-{}.json'.format(attempt) if local_test_mode else \
        'aws emr describe-cluster --cluster-id "{}"'.format(cluster_id)
    logger.debug("exec: {}".format(cmd))
    result = run([cmd], check=True, shell=True, universal_newlines=True, stdout=PIPE, stderr=PIPE)
    return result.stdout


def get_aws_emr_public_master_dns_name_on_waiting(cluster_id: str, timeout: int, local_test_mode: bool) -> str:
    attempt = 0
    state = ''
    t_end = time.time() + timeout * 60 + 10  # attempt for 10 seconds longer than timeout to create an AWS EMR cluster
    with tqdm(total=timeout * 60 + 10) as pbar:
        while time.time() < t_end:
            attempt += 1
            result = describe_cluster(cluster_id, attempt, local_test_mode)
            j = json.loads(result)
            state = j['Cluster']['Status']['State']
            master_public_dns = j['Cluster'].get('MasterPublicDnsName')
            core_state = j['Cluster']['InstanceFleets'][0]['Status']['State']
            master_state = j['Cluster']['InstanceFleets'][1]['Status']['State']

            logger.debug('Cluster Current Details details: {}'.format(result.replace('\n', '').replace('  ', '')))

            pbar.set_description(
                'Timeout: {}min, Check#: {}, State: {}, Master State {}, Core State {}, master_public_dns: {}'.format(
                    timeout, attempt, state, master_state, core_state, master_public_dns))
            sleep_interval_sec = 5
            pbar.update(sleep_interval_sec)

            if state == 'WAITING':
                logger.info('Cluster Details: {}'.format(result.replace('\n', '').replace('  ', '')))
                return master_public_dns

            time.sleep(sleep_interval_sec)

    raise Exception(
        'Cluster: {} did not reach State "WAITING" after timeout: {}min. Current State: {}'.format(cluster_id,
                                                                                                   timeout, state))


def create_home_dir(host_name: str, dir_name: str) -> str:
    cmd = 'ssh -i ~/aws-emr-key.pem hadoop@{} hdfs dfs -mkdir -p /user/{}'.format(host_name, dir_name)
    logger.debug("exec: {}".format(cmd))
    result = run([cmd], check=True, shell=True, universal_newlines=True, stdout=PIPE, stderr=PIPE)
    cmd = 'ssh -i ~/aws-emr-key.pem hadoop@{} hdfs dfs -chmod -R 777 /user/{}'.format(host_name, dir_name)
    return result.stdout


def show_help():
    print(
        'generate_scenarios_aws_spot_cluster.py --spot-core-capacity 2 --hdfs-port                   # will use input stream')
    print(
        'generate_scenarios_aws_spot_cluster.py -i <path_to_status_file.json> # will use file location to pull the status')


def main(argv):
    try:
        opts, args = getopt.getopt(argv[1:], "ht", ["test"])
    except getopt.GetoptError:
        show_help()
        sys.exit(2)

    local_test_mode = False

    for opt, arg in opts:
        if opt == '-h':
            show_help()
            sys.exit()
        elif opt in ("-t", "--test"):
            local_test_mode = True

    timeout = 10
    cores = 2
    cluster_id = create_cluster(timeout, cores, local_test_mode)
    #
    host_name = get_aws_emr_public_master_dns_name_on_waiting(cluster_id, timeout, local_test_mode)
    logger.info(host_name)
    default_fs = 'hdfs://localhost:9000' if local_test_mode else 'hdfs://{}:{}'.format(host_name, '8020')
    if not local_test_mode:
        output = create_home_dir(host_name, 'test-harness')

    generate_jsonl_data.main(
        [sys.argv[0], '-c', './../src/main/resources/application.conf', '-o', 'hdfs', '--default-fs', default_fs])


if __name__ == "__main__":
    main(sys.argv)
