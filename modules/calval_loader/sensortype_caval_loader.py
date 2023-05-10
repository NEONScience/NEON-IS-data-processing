#!/usr/bin/env python3
import os
import logging
import io
from contextlib import closing
from pathlib import Path

import python_pachyderm
import environs
import shutil
import glob
import sys
import datetime
import urllib.request
import python_pachyderm
import pathlib



def load() -> None:
    env = environs.Env()
    #in_path: Path = Path("/home/NEON/vchundru/git/NEON-IS-data-processing/pipe")
    in_path: Path = env.path('IN_PATH')
    output_directory: Path = env.path('OUT_PATH')


    repo_name = os.path.split(in_path)[-1] + "_calibration"
    print("repo_name is: ", repo_name)

    try:
            for f in in_path.rglob('**/*.xml'):
                temp_file_path: Path = f
                print("source path is: ", temp_file_path)
                pathname, extension = os.path.splitext(f)
                dest_path = os.path.join(*pathname.split('/')[3:])
                dest_path = str(dest_path) +".xml"
                dest_path = Path(output_directory, dest_path)
                print("destination path is:", dest_path)
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                with open(f, 'r') as input_file:
                    data = input_file.read()
                    with open(dest_path, "w") as output_file:
                        output_file.write(data)
    except Exception:
        print(f"exception caught while putting file to pachyderm")
        exception_type, exception_obj, exception_tb = sys.exc_info()
        print("Exception at line " + str(exception_tb.tb_lineno) + ": " + str(sys.exc_info()))



if __name__ == '__main__':
    load()
