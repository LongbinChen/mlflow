import json
from operators.base import Operator, InputDataType, OutputDataType, StringParam
import os

class KaggleDownloadOperator(Operator):
    def __init__(self,
                data_file:OutputDataType,
                competition:StringParam,
                filename:StringParam):
        self.data_file = OutputDataType(data_file)
        self.competition = StringParam(competition)
        self.filename = StringParam(filename)

    def execute(self)->'OutputDataType':

        command = "kaggle competitions download %s -f %s " % (self.competition, self.filename)
        print(command)
        os.system(command)

        command = "mv %s %s " % (self.filename, self.data_file)
        print(command)
        os.system(command)


