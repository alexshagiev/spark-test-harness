from pyhocon import ConfigFactory
import sys, getopt, os
import pyarrow as pa
from itertools import product
import generator.jsonl
# from hdfs3 import HDFileSystem
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def hdfs_connect(conf) -> pa.HadoopFileSystem:
    hdfs_url = conf['hdfs.default-fs']
    logger.info(hdfs_url)
    hdfs_host = str(hdfs_url).split(':')[1].replace('/', '')
    hdfs_port = int(str(hdfs_url).split(':')[2])
    if "HADOOP_HOME" in os.environ:
        logger.info('Connecting to Haddop based on HADOOP_HOME {}'.format(os.environ.get('HADOOP_HOME')))
        return pa.hdfs.connect(driver='libhdfs3')
    else:
        logger.info('Connecting to Haddop based on config {}'.format(conf))
        return pa.hdfs.connect(hdfs_host, hdfs_port, driver='libhdfs3')


def clean_up_scenarios(hdfs: pa.HadoopFileSystem, conf: list, types: list, sizes: list):
    for type in types:
        l0_dir = conf[type]['l0-dir']
        try:
            # files = hdfs.ls(l0_dir)
            logger.info('Cleaning all files under dir={}'.format(l0_dir))
            # logger.info('hdfs -ls {} {}'.format(l0_dir, '\n'.join(files)))
        except Exception as e:
            continue
        hdfs.delete(l0_dir, recursive=True)


def create_scenarios(output: str, conf: list):
    run_types = conf['scenarios.run'].keys()

    if output == 'hdfs':
        hdfs = hdfs_connect(conf['conf'])
        base_dir = conf['conf']['hdfs']['base-dir']
        if not hdfs.exists(base_dir):
            hdfs.mkdir(base_dir)

        for run_type in run_types:
            clean_up_scenarios(hdfs, conf['scenarios'], run_types, conf['scenarios.run'][run_type])

    for run_type in run_types:
        for run_size in conf['scenarios.run'][run_type]:
            scenario = conf['scenarios'][run_type + '.' + run_size]
            rows = scenario['rows']
            columns = scenario['columns']
            rows_uniqueness_factor = int(eval(str(scenario['rows_uniqueness_factor'])))
            name = scenario['name']

            logger.info('Creating Scenario file rows={}, columns={}, name={}'.format(rows, columns, name))
            if output == 'hdfs':
                l0_dir = conf['scenarios'][run_type]['l0-dir']
                if not hdfs.isdir(l0_dir):
                    logger.info('Creating l0 directory for run_type={} dir={}'.format(run_type, l0_dir))
                    hdfs.mkdir(l0_dir)
                logger.info('Creating file: {}'.format(l0_dir + '/' + name))
                with hdfs.open(l0_dir + '/' + name, 'wb') as file:
                    generator.jsonl.write(file, columns, rows, rows_uniqueness_factor)
            else:
                full_name = output + '/' + name
                if not os.path.exists(os.path.dirname(full_name)):
                    os.makedirs(os.path.dirname(full_name), exist_ok=True)
                with open(full_name, 'w') as file:
                    generator.jsonl.write(file, columns, rows, rows_uniqueness_factor)


def show_help():
    print(
        'generate_jsonl_data.py --config <path_to_application.conf> --output [hdfs|<local_path>] --default-fs <hdfs://host:port override>')


def main(argv):

    try:
        logger.info('Starting with: argv: {}'.format(argv))
        opts, args = getopt.getopt(argv[1:], "h", ["help", "config=", "output=", "default-fs="])
    except getopt.GetoptError:
        show_help()
        sys.exit(2)

    default_fs = ''
    output = ''
    config_file = './application.conf'

    for opt, arg in opts:
        if opt == ('-h', '--help'):
            show_help()
            sys.exit()
        elif opt in "--config":
            config_file = arg
        elif opt in "--output":
            output = arg
        elif opt in "--default-fs":
            default_fs = arg

        logger.info('Loading config file:{}'.format(config_file))

        conf = ConfigFactory.parse_file(config_file)
        if not default_fs == '':
            conf['conf']['hdfs']['default-fs'] = default_fs

        logger.info("Creating scenarios output:{}, conf:{}".format(output, conf))
        create_scenarios(output, conf)


if __name__ == "__main__":
    main(sys.argv)
