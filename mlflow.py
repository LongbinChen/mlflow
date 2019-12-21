import inspect
from operators.base import Operator, InputDataType, OutputDataType, StringParam
import yaml
import json
from dag_parser import DAGParser
from task_runner import TaskRunner
import argparse
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s/%(name)s/%(levelname)s  %(message)s',)

# sample_operator = import_module('operators.sample_operator')
# operator = getattr(sample_operator, 'SampleOperator')

# w = operator(OutputDataType("s3://www/we/d/a/g.z"))
# w.execute()

# print(inspect.getfullargspec(operator.__init__))
# print(inspect.getfullargspec(operator.execute))

# from operators.sample_operator import SampleOperator




# for i in range(10):
#     sample_operator = SampleOperator(input_data="file1.txt", output_data1="file1%d.txt"%i, output_data2="file2%d.txt"%i, threshold=str(i))
#     o1, o2 = sample_operator.execute()
# print(o1)

# print(o2)


def run(params):
    dag = yaml.load(open(params.dag_file, "r"), Loader=yaml.FullLoader)
    dag_parser = DAGParser(dag)
    for task in dag_parser.tasks:
        #print("processing %s" % task['name'])
        task_runner = TaskRunner(task,dag)
        task_runner.execute()
        #print(task)
        
if __name__ == '__main__':
    #run test

    parser = argparse.ArgumentParser()
    parser.add_argument('dag_file', help='the dag file')
    params = parser.parse_args()
    run(params)

