import yaml
from dag_parser import DAGParser
from task_runner import TaskRunner
import argparse
import logging


def run(params):
    if params.log_level == "info":
        logging.basicConfig(level=logging.INFO, format='%(asctime)s/%(name)s/%(levelname)s  %(message)s',)
        logging.info('setting logging to logging.INFO')
    elif params.log_level == "debug":
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s/%(name)s/%(levelname)s  %(message)s',)
        logging.info('setting logging to logging.DEBUG')

    dag = yaml.load(open(params.dag_file, "r"), Loader=yaml.FullLoader)
    dag_parser = DAGParser(dag)

    for task in dag_parser.tasks:
        task_runner = TaskRunner(task,dag, params.force)
        task_runner.execute()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', dest='log_level', default="info",
                        help='set the logger level, default info')
    parser.add_argument('-f', dest='force', default=False, action="store_true",
                        help='Force to re-run all the tasks, even it is completed.')
    parser.add_argument('dag_file', help='the dag file')
    params = parser.parse_args()
    run(params)

