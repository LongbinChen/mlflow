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
    def __init__(self, string_param):
        self.value = bool(string_param)
    def __str__(self):
        return str(self.value)
    type = "BooleanParam"

class Operator:
    pass