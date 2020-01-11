import logging
import os
import subprocess

class InputDataType:
    value = None
    def __init__(self, data_name):
        self.value = data_name
    def __str__(self):
        return self.value
    type = "InputDataType"

class OutputDataType:
    value = None
    def __init__(self, data_name):
        self.value = data_name
    def __str__(self):
        return self.value
    type = "OutputDataType"
    
class StringParam:
    value = None
    def __init__(self, string_param):
        self.value = str(string_param)
    def __str__(self):
        return self.value
    type = "StringParam"
    
class BooleanParam:
    value = None
    def __init__(self, bool_param):
        if bool_param.lower() == "true":
            self.value = True
        else:
            self.value = False
    def __str__(self):
        return str(self.value)
    type = "BooleanParam"


class Operator:
    logger = logging.getLogger("BaseOperator")

    def set_param(self, execute_param):
        self._cmd_count = execute_param['cmd_count']
        self.task_dir = execute_param['task_dir']

    def _run_command(self, cmd):
        try:
            self._cmd_count += 1
        except:
            self._cmd_count = 0

        self.logger.debug("Running command %d: %s " % (self._cmd_count, cmd))
        try:
            with open(os.path.join(self.task_dir, "stdout_%d.txt" % self._cmd_count), "wb") as out:
                with open(os.path.join(self.task_dir, "stderr_%d.txt" % self._cmd_count), "wb") as err:
                    result = subprocess.Popen(cmd, shell=True, stdout=out, stderr=err, cwd=self.task_dir)
                    streamdata = result.communicate()[0]
                    text = result.returncode
            if (text != 0):
                self.logger.warning("Status : FAIL")
                self.logger.warning("\n".join(open(os.path.join(
                    self.task_dir, "stderr_%d.txt" % self._cmd_count), "r").readlines()))
        except subprocess.CalledProcessError as exc:
            self.logger.warning("Status : FAIL "  + str(exc.returncode) + str(exc.output))
