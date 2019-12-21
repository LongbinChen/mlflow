import argparse
import json
from .base import Operator, InputDataType, OutputDataType, StringParam
import logging
logger = logging.getLogger("Operator/SAMPLE")

class SampleOperator(Operator):
    def __init__(self,
                input_data:InputDataType,
                output_data1:OutputDataType,
                output_data2:OutputDataType,
                threshold:StringParam="20"):
        self.input_data = InputDataType(input_data)
        self.output_data1 = OutputDataType(output_data1)
        self.output_data2 = OutputDataType(output_data2)
        self.threshold = StringParam(threshold)

    def execute(self)->'OutputDataType,OutputDataType':
        logger.info("input: " + str(self.input_data))
        logger.info("output1: " + str(self.output_data1))
        logger.info("output2: " + str(self.output_data2))
        logger.info("threshold " + self.threshold.value)
        with open(self.output_data1.value, "w") as fw:
            fw.write("okok1" + self.output_data1.value + self.threshold.value + "\n")
        with open(self.output_data2.value, "w") as fw:
            fw.write("okok2" + self.output_data1.value + self.threshold.value + "\n")
        return self.output_data1, self.output_data2

