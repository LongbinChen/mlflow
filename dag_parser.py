import data_utils
from importlib import import_module
import inspect
import logging


class DAGParser:

    def __init__(self, dag):
        self.dag = dag
        self.tasks = []

        self.logger = logging.getLogger("DagParser")
        self.logger.setLevel(logging.root.level)

        self.parse()

    def parse(self):
        """
        main entry of the dag parser
        """
        self.logger.info("==========================================================")
        self.logger.info(" DAG Parseing ...")
        self.logger.info("==========================================================")
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
            self.operator = operator
            task['operator_info'] = inspect.getfullargspec(operator.__init__)
            task['parameters_inpsect'] = inspect.getfullargspec(operator.__init__)

            task['input_data_list'] = list(filter(lambda m: str(m[1]) == "<class 'operators.base.InputDataType'>",
                                        task['parameters_inpsect'].annotations.items()))
            task['output_data_list'] = list(filter(lambda m: str(m[1]) == "<class 'operators.base.OutputDataType'>", 
                                        task['parameters_inpsect'].annotations.items()))

            task['param_list'] = []
            for param_name, param_type in task['parameters_inpsect'].annotations.items():
                self.logger.debug("param name %s, param type: %s" % (param_name, param_type.__name__ ))
                if param_type.__name__.endswith("Param"):
                    task['param_list'].append((param_name, param_type))
            # task['param_list'] = list(filter(lambda m: str(type(m[1]).__name__).endswith == "Param",
            #                             task['parameters_inpsect'].annotations.items()))
            self.logger.debug("get task info for task: %s" % module_name + "." +  cls_name)
            #self.logger.debug(" parameters_inpsect", str(task['parameters_inpsect'].annotations))
            if len(task['input_data_list']) == 0:
                self.logger.warning("input_data_list of %s is empty." % (module_name + "." + cls_name))
            self.logger.debug("input_data_list: %s " % task['input_data_list'])
            self.logger.debug("output_data_list: %s " % task['output_data_list'])
            self.logger.debug("param_list: %s " % task['param_list'])

    def _annotate_dag(self):
        """"
        calculate hash for each input and output data, also the hash for the task

        """
        
        data_context = dict()

        for task in self.tasks:

            hash_string = "Operator:" + task['operator'] + self.operator.get_version() + ":"
            task['input_data_hash'] = dict()
            for k, _ in task['input_data_list']:
                v = task['parameters'][k]
                if v.startswith('s3://') or v.startswith("http://") or v.startswith("file:///"):
                    task['input_data_hash'][k] = data_utils.get_md5(v.encode('utf-8'))
                else:
                    task['input_data_hash'][k] = data_context.get(v)
            hash_string += ",".join([k + ":" + str(v.__name__) + ":" + task['input_data_hash'][k] for k, v in task['input_data_hash'].items()])
            hash_string += ",".join([k + ":" + str(v.__name__) + ":" + str(task['parameters'][k]) for k, v in task['param_list']])
            self.logger.debug("hashing task %s" %  task)
            self.logger.debug("hashing string %s" % hash_string)
            
            task_hash = data_utils.get_md5(hash_string.encode('utf-8'))
            data_context[task['name']] = task_hash
            task['hash'] = task_hash
            task['output_data_hash'] = dict()
            for k, v in task['output_data_list']:
                output_hash = data_utils.get_md5((hash_string + "OUTPUT:" + k).encode('utf-8'))
                data_context[task['name'] + "::" + k] = output_hash
                task['output_data_hash'][k] = output_hash

    def _sort_tasks(self):
        """
        given a dag, return a list of task that can run sequentially
        if task A's input is  task B's output, B depends on A
        """
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

        self.tasks = []
        # check all dep_task are in the dag

        for d, t in dep:
            if d not in all_tasks:
                self.logger.error("Can not find task %s " % d)
                return False

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
        self.logger.info("\t* %d tasks found and will run as the following order " % len(self.tasks))
        for t in self.tasks:
            self.logger.info("\t\t- %s" % t['name'])
                
        return self.tasks

