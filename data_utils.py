import json
import hashlib
import os
import errno

def get_md5(input_str):
    m = hashlib.md5()
    m.update(input_str)
    return m.hexdigest()

def symlink_force(target, link_name):
    try:
        os.symlink(target, link_name)
    except OSError as e:
        if e.errno == errno.EEXIST:
            os.remove(link_name)
            os.symlink(target, link_name)
        else:
            raise e


# def parse_dag(dag):
#     ''' given a dag, return a list of task that can run sequentially 
#     if task A's input is  task B's output, B depends on A
#     '''

#     dep = set()
#     all_tasks = []
#     for task, task_info in dag.items():
#         all_tasks.append(task)
#         for _, data_location in task_info.get("parameters", {}).items():
#             data_location = str(data_location)
#             if '::' in data_location:
#                 dep_task, _ = data_location.split("::", 1)
#                 dep.add((dep_task,task))
    
#     #top_sort(dep)

#     sorted_tasks = []
#     print("dep", dep)
#     while len(all_tasks) > 0:
#         for i in range(len(all_tasks)):
#             runnable = True
#             for d in dep:
#                 if d[1] == all_tasks[i]:
#                     runnable = False
#                     break
#             if runnable:
#                 task_run = all_tasks[i]
#                 sorted_tasks.append(all_tasks[i])
#                 del all_tasks[i]
#                 dep = [d for d in dep if d[0] != task_run]
#                 break
#     print(sorted_tasks)
#     return sorted_tasks            
