import os
import argparse
from importlib import import_module
import types
import operators
import logging
import sys
import inspect
import json
import re
import zipfile
from typing import Dict, List, Pattern

from importlib.machinery import SourceFileLoader

logger = logging.getLogger("OperatorLoader")



class OperatorLoader():

    COMMENT_PATTERN = re.compile(r"\s*#.*")



    def find_operators(self, directory):
        all_operators = dict()
        for f in self.list_py_file_paths(directory):

            module_name = f.strip(".py").replace("/", ".")
            _, class_name = module_name.rsplit(".", 1)
            package_name = module_name.split(".")[1]
            if package_name == "base":
                continue
            if package_name not in all_operators:
                all_operators[package_name] = dict()
            found_operators = self.process_file(f)
            for m in found_operators:
                print(module_name + "." + str(m.__name__))
                operator = getattr(import_module(module_name), m.__name__)
                inp = inspect.getfullargspec(operator.__init__)
                all_operators[package_name][class_name] = dict()
                all_operators[package_name][class_name]['module_name'] = module_name
                all_operators[package_name][class_name]['class_name'] = class_name +"." + m.__name__
                all_operators[package_name][class_name]['parameters'] = [[m, w.__name__] for m, w in inp.annotations.items()]
        with open("operators.json", "w") as fop:
            json.dump(all_operators, fop, indent=4)
        return all_operators


    def process_file(self, filepath):
        from operators.base import Operator

        found_operators = []

        mods = []

        logger.debug("Importing %s", filepath)
        mod_name, _ = os.path.splitext(os.path.split(filepath)[-1])
        if mod_name in sys.modules:
            del sys.modules[mod_name]

        try:
            m = SourceFileLoader(mod_name,  filepath).load_module()
            mods.append(m)
        except Exception as e:
            logger.exception("Failed to import: %s", filepath)

        for m in mods:
            for m_name, m_type in m.__dict__.items():
                if m_name == "Operator":
                    continue

                if inspect.isclass(m_type) and issubclass(m_type, Operator):
                    found_operators.append(m_type)
        return found_operators

    def list_py_file_paths(self, directory: str):
        """
        Traverse a directory and look for Python files.
        :param directory: the directory to traverse
        :type directory: unicode
        :return: a list of paths to Python files in the specified directory
        :rtype: list[unicode]
        """
        file_paths: List[str] = []
        if directory is None:
            return []
        elif os.path.isfile(directory):
            return [directory]
        elif os.path.isdir(directory):
            patterns_by_dir: Dict[str, List[Pattern[str]]] = {}
            for root, dirs, files in os.walk(directory, followlinks=True):
                patterns: List[Pattern[str]] = patterns_by_dir.get(root, [])
                ignore_file = os.path.join(root, '.mlflowignore')
                if os.path.isfile(ignore_file):
                    with open(ignore_file, 'r') as file:
                        # If we have new patterns create a copy so we don't change
                        # the previous list (which would affect other subdirs)
                        lines_no_comments = [self.COMMENT_PATTERN.sub("", line) for line in file.read().split("\n")]
                        patterns += [re.compile(line) for line in lines_no_comments if line]

                # If we can ignore any subdirs entirely we should - fewer paths
                # to walk is better. We have to modify the ``dirs`` array in
                # place for this to affect os.walk
                dirs[:] = [
                    subdir
                    for subdir in dirs
                    if not any(p.search(os.path.join(root, subdir)) for p in patterns)
                ]

                # We want patterns defined in a parent folder's .airflowignore to
                # apply to subdirs too
                for subdir in dirs:
                    patterns_by_dir[os.path.join(root, subdir)] = patterns.copy()

                self.find_py_file_paths(file_paths, files, patterns, root)

        return file_paths

    def find_py_file_paths(self, file_paths, files, patterns, root):
        """Finds file paths of all python files."""
        for f in files:
            # noinspection PyBroadException
            try:
                file_path = os.path.join(root, f)
                if not os.path.isfile(file_path):
                    continue
                _, file_ext = os.path.splitext(os.path.split(file_path)[-1])
                if file_ext != '.py' :
                    continue
                if any([re.findall(p, file_path) for p in patterns]):
                    continue

                file_paths.append(file_path)

            except Exception:  # pylint: disable=broad-except
                logger.error("Error while examining %s" % f)





if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    params = parser.parse_args()

    o_f = OperatorLoader()
    a = o_f.find_operators("operators")



