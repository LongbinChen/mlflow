import json
from operators.base import Operator, InputDataType, OutputDataType, StringParam, BooleanParam
import os
import argparse
import os
import tarfile
import uuid
import hashlib
import logging

class S3UploadOperator(Operator):
    def __init__(self,
                local_file:InputDataType,
                s3_path:StringParam,
                zip_dir:StringParam=None,
                clean:BooleanParam=True
                ):
        self.local_file = InputDataType(local_file)
        self.s3_path = StringParam(s3_path)
        self.zip_dir = StringParam(zip_dir)
        self.clean = BooleanParam(clean)


    def make_tarfile(self, output_filename, source_dir):
        with tarfile.open(output_filename, "w:gz") as tar:
            tar.add(source_dir, arcname=os.path.basename(source_dir))

    def calculate_md5(self, fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def execute(self):
        if os.path.isdir(self.local_file):
            if self.zip_dir != None:
                if not os.path.exists(self.zip_dir):
                    os.mkdir (self.zip_dir)
                    _tmp_file = os.path.join(self.zip_dir, str(uuid.uuid()) + ".tar.gz")
            else:
                _tmp_file = str(uuid.uuid4()) + ".tar.gz"
            print("making tar.gz file %s " % _tmp_file)
            self.make_tarfile(_tmp_file, self.local_file)
            data_md5 = self.calculate_md5(_tmp_file)
            _tmp_file_md5 = _tmp_file + ".md5"
            
            with open(_tmp_file_md5, "w") as fmd5:
                fmd5.write(data_md5)
        
            print("uplaoding % s to %s " % (_tmp_file, self.s3_path))
            os.system("aws s3 cp %s %s" % (_tmp_file, self.s3_path))
            if self.clean: 
                os.system("rm %s" % (_tmp_file))
            return True
        if os.path.isfile(self.local_file):
            os.system("aws s3 cp -F %s %s" % (self.local_file, self.s3_path))
            return True
        return False

 
