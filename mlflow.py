import yaml
import json
from dag_parser import DAGParser
from task_runner import TaskRunner
import argparse
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s/%(name)s/%(levelname)s  %(message)s',)


def run(params):
    dag = yaml.load(open(params.dag_file, "r"), Loader=yaml.FullLoader)
    dag_parser = DAGParser(dag)
    for task in dag_parser.tasks:
        task_runner = TaskRunner(task,dag)
        task_runner.execute()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('dag_file', help='the dag file')
    params = parser.parse_args()
    run(params)

