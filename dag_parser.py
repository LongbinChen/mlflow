import data_utils
from importlib import import_module
import inspect
import json
import logging

logger = logging.getLogger("DagParser")

class DAGParser:

    def __init__(self, dag):
        self.dag = dag
        self.tasks = []
        self.parse()

    def parse(self):
        '''
        main entry of the dag parser
        '''
        logger.info("==========================================================")
        logger.info(" DAG Parseing ...")
        logger.info("==========================================================")
        # topological sort to find a sequence of tasks that can be executed sequentially
        self._sort_tasks()

        # import the operator module and find the input/output and parameters
        self._get_task_info()
        # create hashing for the task as well as the input/output data
        self._annotate_dag()


    def _get_task_info(self):
        for task in self.tasks:
            module_name, cls_name = task['operator'].rsplit(".", 1)
            operator = getattr(import_module(module_name), cls_name)
            task['operator_info'] = inspect.getfullargspec(operator.__init__)
            task['parameters_inpsect'] = inspect.getfullargspec(operator.__init__)

            # for  t, s in task['parameters_inpsect'].annotations.items():
            #     if str(s) == "<class 'operators.base.OutputDataType'>":
            #         print(t, s)


            task['input_data_list'] = list(filter(lambda m: str(m[1]) == "<class 'operators.base.InputDataType'>", 
                                        task['parameters_inpsect'].annotations.items()))
            task['output_data_list'] = list(filter(lambda m: str(m[1]) == "<class 'operators.base.OutputDataType'>", 
                                        task['parameters_inpsect'].annotations.items()))
            task['param_list'] = list(filter(lambda m: str(m[1]).endswith == "Param", 
                                        task['parameters_inpsect'].annotations.items()))
            
    def _annotate_dag(self):
        
        data_context = dict()

        for task in self.tasks:

            hash_string = "Oprator:" + task['operator']
            task['input_data_hash'] = dict()
            for k, _ in task['input_data_list']:
                v = task['parameters'][k]
                if v.startswith('s3://') or v.startswith("http://") or v.startswith("file:///"):
                    task['input_data_hash'][k] = data_utils.get_md5(v.encode('utf-8'))
                else:
                    task['input_data_hash'][k] = data_context.get(v)
            hash_string += ",".join([k + ":" + v for k, v in task['input_data_hash'].items()])
            hash_string += ",".join([k + ":" + v for k, v in task['param_list']])
            
            task_hash = data_utils.get_md5(hash_string.encode('utf-8'))
            data_context[task['name']] = task_hash
            task['hash'] = task_hash
            
            task['output_data_hash'] = dict()
            for k, v in task['output_data_list']:
                output_hash = data_utils.get_md5((hash_string + "OUTPUT:" + k).encode('utf-8'))
                data_context[task['name'] + "::" + k] = output_hash
                task['output_data_hash'][k] = output_hash


            
    def _sort_tasks(self):
        ''' given a dag, return a list of task that can run sequentially 
        if task A's input is  task B's output, B depends on A
        '''
        dag = self.dag
        dep = set()
        all_tasks = []
        for task, task_info in dag.items():
            task_info['name'] = task
            all_tasks.append(task)
            for _, data_location in task_info.get("parameters", {}).items():
                data_location = str(data_location)
                if '::' in data_location:
                    dep_task, _ = data_location.split("::", 1)
                    dep.add((dep_task,task))
        
        #top_sort(dep)

        self.tasks = []
        while len(all_tasks) > 0:
            for i in range(len(all_tasks)):
                runnable = True
                for d in dep:
                    if d[1] == all_tasks[i]:
                        runnable = False
                        break
                if runnable:
                    task_run = all_tasks[i]
                    self.tasks.append(dag[all_tasks[i]])
                    del all_tasks[i]
                    dep = [d for d in dep if d[0] != task_run]
                    break
        logger.info("\t* %d tasks found and will run as the following order " % len(self.tasks))
        for t in self.tasks:
            logger.info("\t\t- %s" % t['name'])
                
        return self.tasks

