from pyhocon import ConfigFactory
import sys, getopt, os
from itertools import product
import generator.jsonl
from hdfs3 import HDFileSystem
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def hdfs_connect(conf):
    hdfs_url = conf['hdfs.url']
    logger.info(hdfs_url)
    hdfs_host = str(hdfs_url).split(':')[1].replace('/', '')
    hdfs_port = int(str(hdfs_url).split(':')[2])
    return HDFileSystem(hdfs_host, hdfs_port)


def clean_up_scenarios(hdfs: HDFileSystem, conf: list, types: list, sizes: list):
    for type in types:
        l0_dir = conf[type]['l0-dir']
        try:
            files = hdfs.ls(l0_dir)
            logger.info('Cleaning all files under dir={}'.format(l0_dir))
            logger.info('hdfs -ls {} {}'.format(l0_dir, '\n'.join(files)))
        except Exception:
            continue
        hdfs.rm(l0_dir)


def create_scenarios(output: str, conf: list):
    run_types = conf['scenarios.run'].keys()

    if output == 'hdfs':
        hdfs = hdfs_connect(conf['conf'])
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
                    logger.info('Creating l0 directory for type={} dir={}'.format(type, l0_dir))
                    hdfs.mkdir(l0_dir)
                with hdfs.open(l0_dir + '/' + name, 'wb') as file:
                    generator.jsonl.write(file, columns, rows, rows_uniqueness_factor)
            else:
                full_name = output + '/' + name
                if not os.path.exists(os.path.dirname(full_name)):
                    os.makedirs(os.path.dirname(full_name), exist_ok=True)
                with open(full_name, 'w') as file:
                    generator.jsonl.write(file, columns, rows, rows_uniqueness_factor)


def show_help():
    print('generate.py -c <path_to_application.conf> -o [hdfs|<local_path>]')


def main(argv):
    config_file = './application.conf'
    try:
        opts, args = getopt.getopt(argv[1:], "hc:o:", ["config=", "output="])
    except getopt.GetoptError:
        show_help()
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            show_help()
            sys.exit()
        elif opt in ("-c", "--config"):
            config_file = arg
        elif opt in ("-o", "--output"):
            output = arg

    logger.info('Loading config file:{}'.format(config_file))

    conf = ConfigFactory.parse_file(config_file)

    create_scenarios(output, conf)


if __name__ == "__main__":
    main(sys.argv)