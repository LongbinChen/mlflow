import argparse
import json
from operators.base import Operator, InputDataType, OutputDataType, StringParam
import pandas as pd
import logging
logger = logging.getLogger("OP/data/CSVConverter")

class CSVConverter(Operator):
    def __init__(self,
                input_data:InputDataType,
                output_data:OutputDataType,
                input_sep: StringParam,
                input_header: StringParam,
                output_sep: StringParam,
                output_header: StringParam):
        self.input_data = str(InputDataType(input_data))
        self.output_data = str(OutputDataType(output_data))
        self.input_sep = str(input_sep)
        self.output_sep = str(output_sep)
        self.input_header = str(input_header)
        self.output_header = str(output_header)

    def execute(self):
        logger.info("Convert data from %s to %s" % (self.input_data, self.output_data))
        sep_dict = {'comman':',', "tab":'\t'}
        input_header = self.input_header.split(",")
        dataset = pd.read_csv(self.input_data, sep=sep_dict.get(self.input_sep, ","), names=input_header)
        dataset.to_csv(self.output_data, sep=sep_dict.get(self.output_sep, ","), encoding='utf-8', index=False)

