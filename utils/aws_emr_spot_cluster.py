import sys
import getopt
import os
import json
import logging
import time
from tqdm import tqdm, trange
from pathlib import Path
from subprocess import run, PIPE
import generate_jsonl_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ssh_key_check_disable = '-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'
ssh_pem_key = '-i ~/aws-emr-key.pem'


def create_cluster(timeout: int, core_nodes: int, local_test_mode: bool) -> str:
    cmd = 'cat ./tests/resources/aws/create-cluster.json' if local_test_mode else \
        "aws emr create-cluster \
        --release-label emr-5.26.0 --use-default-roles --applications Name=Spark Name=Hadoop \
        --ec2-attributes KeyName=aws-emr-key \
        --instance-fleets \
        InstanceFleetType=MASTER,TargetSpotCapacity=1,InstanceTypeConfigs=['{InstanceType=m4.large}'],LaunchSpecifications={SpotSpecification='{TimeoutDurationMinutes=" + str(
            timeout) + ",TimeoutAction=TERMINATE_CLUSTER}'} \
        InstanceFleetType=CORE,TargetSpotCapacity=" + str(
            core_nodes) + ",InstanceTypeConfigs=['{InstanceType=m4.large}'],LaunchSpecifications={SpotSpecification='{TimeoutDurationMinutes=" + str(
            timeout) + ",TimeoutAction=TERMINATE_CLUSTER}'} \
        "

    logger.info("exec: {}".format(cmd))
    result = run([cmd], shell=True, universal_newlines=True, stdout=PIPE, stderr=PIPE)
    if result.returncode != 0:
        raise Exception(result.stderr)

    cluster_id = json.loads(result.stdout)['ClusterId']
    logger.debug('Cluster Details: {}'.format(result.stdout.replace('\n', '').replace('  ', '')))
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
    timeout_describe_addition = 10  # in seconds, to allow for aws to respond and not drop created cluster.
    t_end = time.time() + timeout * 60 + timeout_describe_addition
    with tqdm(total=timeout * 60 + timeout_describe_addition) as p_bar:
        while time.time() < t_end:
            attempt += 1
            result = describe_cluster(cluster_id, attempt, local_test_mode)
            j = json.loads(result)
            state = j['Cluster']['Status']['State']
            master_public_dns = j['Cluster'].get('MasterPublicDnsName')
            core_state = 'NEW'
            master_state = 'NEW'
            for i in range(2):
                node_state = j['Cluster']['InstanceFleets'][i]['Name']
                if node_state == 'CORE':
                    core_state = j['Cluster']['InstanceFleets'][i]['Status']['State']
                else:
                    master_state = j['Cluster']['InstanceFleets'][i]['Status']['State']

            logger.debug('Cluster Current Details details: {}'.format(result.replace('\n', '').replace('  ', '')))

            p_bar.set_description(
                'Check#: {}, State: {}, Master State {}, Core State {}'.format(
                    attempt, state, master_state, core_state))
            sleep_interval_sec = 5
            p_bar.update(sleep_interval_sec)

            if state == 'WAITING':
                logger.info('Cluster Details: {}'.format(result.replace('\n', '').replace('  ', '')))
                return 'localhost' if local_test_mode else master_public_dns

            time.sleep(sleep_interval_sec)

    raise Exception(
        'Cluster: {} did not reach State "WAITING" after timeout: {}min. Current State: {}'.format(cluster_id,
                                                                                                   timeout, state))


def create_hdfs_home_dir(host_name: str, dir_name: str, local_test_mode: bool) -> str:
    if local_test_mode:
        return 'No need to create a local directory in local mode'
    cmd = 'ssh {} {} hadoop@{} hdfs dfs -mkdir -p /user/{}'.format(ssh_key_check_disable,
                                                                   ssh_pem_key, host_name,
                                                                   dir_name)
    logger.debug("exec: {}".format(cmd))
    result = run([cmd], check=True, shell=True, universal_newlines=True, stdout=PIPE, stderr=PIPE)
    cmd = 'ssh {} {} hadoop@{} hdfs dfs -chmod -R 777 /user/{}'.format(ssh_key_check_disable,
                                                                       ssh_pem_key,
                                                                       host_name,
                                                                       dir_name)
    result = run([cmd], check=True, shell=True, universal_newlines=True, stdout=PIPE, stderr=PIPE)
    return 'usr/{}'.format(host_name)


def copy_jar_to_spot_cluster(jar_file_name: str, public_master_dns: str, local_test_mode: bool) -> str:
    if not os.path.exists(jar_file_name):
        raise Exception('Jar file does not exist: {}'.format(jar_file_name))
    dest_dir = '/mnt/var/lib/hadoop/steps'
    dest = 'hadoop@{}:{}'.format(public_master_dns, dest_dir)
    cmd = 'scp {} {} {} {}'.format(ssh_key_check_disable, ssh_pem_key, jar_file_name, dest)
    logger.debug("exec: {}".format(cmd))
    if local_test_mode:
        logger.info('Remote command: {}'.format(cmd))
        return dest

    result = run([cmd], check=True, shell=True, universal_newlines=True, stdout=PIPE, stderr=PIPE)
    return dest


def run_spark_submit(cluster_id: str, spark_submit_jar: str, local_test_mode: bool) -> str:
    cmd = 'aws emr add-steps --cluster-id ' + cluster_id + ' ' \
                                                           '--steps Type=Spark,Name="test-harness",ActionOnFailure=CONTINUE,Args=[' + spark_submit_jar + ']'
    if local_test_mode:
        logger.info('Remote command: {}'.format(cmd))
        return 'Mock run completed'
    result = run([cmd], check=True, shell=True, universal_newlines=True, stdout=PIPE, stderr=PIPE)
    return result.stdout


def running_local():
    cmd = 'type aws'
    result = run([cmd], shell=True, universal_newlines=True, stdout=PIPE, stderr=PIPE)
    return False if 0 == result.returncode else True


def show_help():
    print('aws_emr_spot_cluster.py \
          \n\tcreates a default spot cluster, populates HDFS with random data, and spark-submit test-harness job')
    print('aws_emr_spot_cluster.py --populate-hdfs=true --cluster j-2AXXXXXXGAPLF \
          \n\tconnects to cluster id provided and populate its HDFS with scenario files')
    print('aws_emr_spot_cluster.py --cluster j-2AXXXXXXGAPLF --spark-submit ./target/spark-test-harness-1.0-SNAPSHOT.jar \
          \n\tconnects to cluster id provided spark-submit test-harness job as jar')


def main(argv):
    try:
        opts, args = getopt.getopt(argv[1:], "",
                                   ["help", "populate-hdfs=", "cluster=", "core-nodes=", "spark-submit="])
    except getopt.GetoptError as e:
        show_help()
        print(e)
        sys.exit(2)

    local_test_mode = running_local()
    if local_test_mode:
        logger.info('###' * 10)
        logger.info('Running on local host not AWS')
        logger.info('###' * 10)

    script_home = str(Path(sys.argv[0]).parent)

    populate_hdfs = False
    spark_submit_jar = script_home + '/./../target/spark-test-harness-1.0-SNAPSHOT.jar'
    spark_submit = False
    cluster_id = ''
    timeout = 10
    core_nodes = 2

    for opt, arg in opts:
        if opt in "--help":
            show_help()
            sys.exit()
        elif opt in "--cluster":
            cluster_id = arg
        elif opt in "--core-nodes":
            core_nodes = arg
        elif opt in "--populate-hdfs":
            populate_hdfs = str(arg).lower() in ['true', '1', 'yes']
        elif opt in "--spark-submit":
            spark_submit_jar = arg
            spark_submit = True

    if cluster_id == '':
        populate_hdfs = True
        spark_submit = True
        cluster_id = create_cluster(timeout, core_nodes, local_test_mode)

    logger.info('Creating Cluster Timeout: {}min'.format(timeout))
    host_name = get_aws_emr_public_master_dns_name_on_waiting(cluster_id, timeout, local_test_mode)
    logger.info('Clusters Public Master DNS: {}'.format(host_name))
    port = '9000' if local_test_mode else '8020'
    default_fs = 'hdfs://{}:{}'.format(host_name, port)


    if populate_hdfs:
        logger.info("Populating Data into Cluster: {}, default-fs: {}".format(cluster_id, default_fs))
        output = create_hdfs_home_dir(host_name, 'test-harness', local_test_mode)
        logger.info("Created home dir for test-harness: {}".format(output))

        generate_jsonl_data.main(
            [sys.argv[0], '--config', script_home + '/./../src/main/resources/application.conf', '--output', 'hdfs', '--default-fs', default_fs])
        sys.exit(0)

    if spark_submit:
        logger.info("Submitting Spark Job into Cluster: {}".format(cluster_id))
        remote_uri_path = copy_jar_to_spot_cluster(spark_submit_jar, host_name, local_test_mode)
        remote_jar_path = 'file:/' + remote_uri_path.split(':')[-1]
        run_spark_submit(cluster_id, remote_jar_path, local_test_mode)


# \
# scp -i ~/aws-emr-key.pem ./target/spark-test-harness-1.0-SNAPSHOT.jar hadoop@ec2-18-223-111-193.us-east-2.compute.amazonaws.com


if __name__ == "__main__":
    main(sys.argv)
