import json
import os
import subprocess
import data_utils
import mlconfig
import logging
import copy
from importlib import import_module

logger = logging.getLogger("TASK_RUNNER")

'''
format of task parameters:

[data_name]: [data_value]

'''


class TaskRunner:
    def __init__(self, task, dag):
        self.task = task
        self.dag = dag
        self.task_dir = os.path.join(mlconfig.working_dir, self.task['hash'])
        self._cmd_count = 0

    def execute(self):
        logger.info("=========================================================")
        logger.info("Running Task: %s (%s)" % (self.task['name'], self.task['hash']))
        self._prepare_data_to_working_directory()
        module_name, cls_name = self.task['operator'].rsplit(".", 1)
        operator = getattr(import_module(module_name), cls_name)
        logger.info("\t* Executing the operator : %s with parameter as follow" % self.task['operator'])
        for s in json.dumps(self.task['real_parameters'], indent=4).split("\n"):
            logger.info("\t    " + s)
        op = operator(**self.task['real_parameters'])
        logger.info("\t* Task finished successfully.")
        op.execute()

        self._copy_output_to_data()

    def _prepare_data_to_working_directory(self):
 
        if not os.path.exists(self.task_dir):
            logger.info("\t* Create working directory %s" % self.task_dir)
            os.mkdir(self.task_dir)
        else:

            #clean up output:
            for data_name, v in self.task["output_data_list"]:
                output_data_location = os.path.join(self.task_dir, data_name)
                cmd = "rm %s" % output_data_location
                self._run_command(cmd)

            logger.info("\t* Exisiting Working directory %s" % self.task_dir)

        logger.info("\t* Iterate throught all input Data")

        self.task['real_parameters'] = copy.deepcopy(self.task['parameters'])

        for data_name, _ in self.task["input_data_list"]:
            data_value = self.task['parameters'][data_name]
            if data_value.startswith("s3://"):
                data_hash = self._download_data_from_s3(data_value)
                self._create_soft_link_to_data_storage(data_name, data_hash)
            elif data_value.startswith("file://"):
                self._create_soft_link_to_localfile(data_name, data_value)
            else:
                self._create_soft_link_to_data_storage(data_name, 
                self.task['input_data_hash'][data_name])
            self.task['real_parameters'][data_name] = os.path.join(self.task_dir, data_name)
        for data_name, v in self.task["output_data_list"]:
            self.task['real_parameters'][data_name] = os.path.join(self.task_dir, data_name)

    def _create_soft_link_to_localfile(self, data_name, data_value):
        """
        create a soft link from working director + data_name to data_value, \
             which is a string in the format of file://
        """
        logger.info("\t\t- Create soft linke from %s to %s " % (data_name, data_value))
        data_location = data_value[7:]
        current_location = os.path.join(self.task_dir, data_name)
        if not os.path.exists(data_location):
            logger.error("\t\t\tIt seems the input file '%s', which will be \
                linked to %s doesn't exists. please make sure the dependent \
                    asks are completed. " % (data_location, data_name))
        else:
            data_utils.symlink_force(data_location, current_location)
            logger.info("\t\t- Softlink %s -> %s Created." % (data_name, data_location))

    def _create_soft_link_to_data_storage(self, data_name, data_hash):
        logger.info("\t\t- Create soft linke from %s to %s " % (data_hash, data_name))
        data_location = os.path.join(mlconfig.data_dir, data_hash)
        current_location = os.path.join(self.task_dir, data_name)
        if not os.path.exists(data_location):
            logger.error("\t\t\tIt seems the input file '%s', which will be \
                linked to %s doesn't exists. please make sure the dependent \
                    asks are completed. " % (data_location, data_name))
        else:
            data_utils.symlink_force(data_location, current_location)
            logger.info("%s -> %s " % (data_name, data_location))

    def _download_data_from_s3(self, s3_data_url):
        """
        download s3 data from s3_data_url, to data_dir/[md5(s3_url)]
        """
        local_data_hash = data_utils.get_md5(s3_data_url.encode('utf-8'))
        local_data_location = os.path.join(mlconfig.data_dir, local_data_hash)
        if os.path.exists(local_data_location):
            logger.info("\t\t- S3 data %s exisits. Download skipped." % local_data_location)
        else:
            logger.info("\t\t- Download s3 data from %s, to %s] " % (s3_data_url, local_data_location))
            cmd = "aws s3 cp %s %s" % (s3_data_url, local_data_location)
            self._run_command(cmd)
        return local_data_hash

    def _run_command(self, cmd):
        self._cmd_count += 1
        logger.debug("Running command %d: %s " % (self._cmd_count, cmd))
        try:
            with open(os.path.join(self.task_dir, "stdout_%d.txt" % self._cmd_count), "wb") as out:
                with open(os.path.join(self.task_dir, "stderr_%d.txt" % self._cmd_count), "wb") as err:
                    result = subprocess.Popen(cmd, shell=True, stdout=out, stderr=err)
                    streamdata = result.communicate()[0]
                    text = result.returncode
            if (text != 0):
                logger.warning("Status : FAIL")
                logger.warning("\n".join(open(os.path.join(
                    self.task_dir, "stderr_%d.txt" % self._cmd_count), "r").readlines()))
        except subprocess.CalledProcessError as exc:
            logger.warning("Status : FAIL "  + str(exc.returncode) + str(exc.output))

    def _copy_output_to_data(self):
        logger.info("\t* Copy output to global data place and soft link the output")
        for k, v in self.task['output_data_hash'].items():
            #mv data to output 
            current_output = os.path.join(self.task_dir, k)
            data_dir_output = os.path.join(mlconfig.data_dir, v)
            cmd = "mv %s %s" % (current_output, data_dir_output)
            self._run_command(cmd)
            #create soft link to output
            data_utils.symlink_force(data_dir_output, current_output)
            logger.info("\t\t- Movde data and Softlink %s -> %s Created." % (data_dir_output, current_output))

 