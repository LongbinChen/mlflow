import yaml
from dag_parser import DAGParser
from task_runner import TaskRunner
import argparse
import logging


def dag_to_rete_json(dag_file):
    print(dag_file)
    dag = yaml.load(dag_file, Loader=yaml.FullLoader)
    dag_parser = DAGParser(dag)
    rete_graph = dict()
    #rete_graph['id'] = str(dag_file)
    rete_graph['id'] = "demo@0.1.0"
    rete_graph['nodes'] = dict()
    for i, task in enumerate(dag_parser.tasks):
        print("task %s" % task['name'])
        print(task['input_data_list'])
        print(task['output_data_list'])
        print(task['param_list'])
        task_node = dict()

        input_list = []
        for t in task['input_data_list']:
            input_list.append({'name': t[0], 'type': t[0]})
        output_list = []
        for t in task['output_data_list']:
            output_list.append({'name': t[0], 'type': t[0]})
        param_list = []
        for t in task['param_list']:
            param_list.append((t[0], t[1].__name__))
        
        task_node["id"] = task['id']

        #task_node["name"] = task['operator']
        task_node["name"] = "Data Processor"
        task_node['data'] = {"text": task['name'],
                      "operator": task['operator'], 
                      "input" : input_list,
                      "output" : output_list, 
                      "param" : param_list,
                      }
        for t, v in param_list:
            task_node['data'][t] = t
        task_node['inputs'] = task['inputs']
        task_node['outputs'] = task['outputs']
        task_node["position"] = [69.17975939518621 + i * 400,29.769080334239206 ]
        rete_graph['nodes']["%d" % (i + 1)] = task_node
    print(rete_graph)
    return rete_graph




if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', dest='log_level', default="info",
                        help='set the logger level, default info')
    parser.add_argument('-f', dest='force', default=False, action="store_true",
                        help='Force to re-run all the tasks, even it is completed.')
    parser.add_argument('dag_file', help='the dag file')
    params = parser.parse_args()
    run(params)

