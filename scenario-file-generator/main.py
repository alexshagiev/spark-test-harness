from generate_jsonl_data import main
import sys

main([sys.argv[0], '-c', '../src/main/resources/application.conf', '-o', 'hdfs'])
# main([sys.argv[0], '-c', '../src/main/resources/application.conf', '-o', '../target'])