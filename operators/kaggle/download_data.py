from operators.base import Operator, OutputDataType, StringParam
import os


class KaggleDownloadOperator(Operator):
    def __init__(self,
                data_file:OutputDataType,
                competition:StringParam,
                filename:StringParam):
        self.data_file = str(OutputDataType(data_file))
        self.competition = str(StringParam(competition))
        self.filename = str(StringParam(filename))

    def execute(self):

        command = "kaggle competitions download %s -f %s " % (self.competition, self.filename)
        os.system(command)

        command = "mv %s %s " % (self.filename, self.data_file)
        print(command)
        os.system(command)


