import json
import hashlib
import os
import errno


def get_md5(input_str):
    m = hashlib.md5()
    m.update(input_str)
    return m.hexdigest()


def symlink_force(target, link_name):
    try:
        os.symlink(target, link_name)
    except OSError as e:
        if e.errno == errno.EEXIST:
            os.remove(link_name)
            os.symlink(target, link_name)
        else:
            raise e
