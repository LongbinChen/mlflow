from operators.base import Operator, OutputDataType, StringParam, BooleanParam
import os

import logging


class KaggleDownloadOperator(Operator):
    def __init__(self,
                data_file: OutputDataType,
                competition: StringParam,
                filename: StringParam,
                unzip: BooleanParam="True"):
        self.data_file = str(OutputDataType(data_file))
        self.competition = str(StringParam(competition))
        self.filename = str(StringParam(filename))
        self.unzip = bool(unzip)
        self.logger = logging.getLogger("KaggleDownload")
        self.logger.setLevel(logging.root.level)

    @staticmethod
    def get_version():
        return "v1.1"

    def execute(self):
        self.logger.info("Download %s %s from kaggle " % (self.competition, self.filename))
        cmd = "kaggle competitions download %s -f %s " % (self.competition, self.filename)
        succ, error_msg = self._run_command(cmd)
        self.logger.debug("runn command result ")
        self.logger.debug(succ)
        self.logger.debug(error_msg)
        if not succ:
            return False, error_msg

        if self.unzip and self.filename.endswith("zip"):
            self.logger.info("unzip data file %s" % self.filename)
            cmd = "unzip -u -o %s " % self.filename
            succ, error_msg = self._run_command(cmd)
            if not succ:
                return False, error_msg
            self.filename = self.filename[:-4]  #remove .gz at the end


        cmd = "mv %s %s " % (self.filename, self.data_file)
        succ, error_msg = self._run_command(cmd)
        if not succ:
            return False, error_msg
        return True, ""

