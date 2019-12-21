from operators.base import Operator, InputDataType, OutputDataType, StringParam
import logging
logger = logging.getLogger("operator/Cut")


class CutOperator(Operator):
    def __init__(self,
                input_data:InputDataType,
                output_data:OutputDataType,
                column_idx:StringParam="0"):
        self.input_data = InputDataType(input_data)
        self.output_data = OutputDataType(output_data)
        self.column_idx = int(str(StringParam(column_idx)))

    def execute(self):
        logger.info("input: " + str(self.input_data))
        logger.info("output: " + str(self.output_data))
        logger.info("column: " + str(self.column_idx))
        with open(self.input_data.value, "r") as fdata:
            with open(self.output_data.value, "w") as fout:
                line = fdata.readline()
                while line:
                    flds = line.strip("\n").split("\t")
                    if self.column_idx < len(flds):
                        fout.write(flds[self.column_idx] + "\n")
                    line = fdata.readline()
                    
