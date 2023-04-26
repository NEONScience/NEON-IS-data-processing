#!/usr/bin/env python3
from contextlib import closing
from typing import Iterator, Dict
from typing import Optional
import logging
import python_pachyderm
import sys
import os
import datetime



def put_calibration_to_pachyderm(repository_files: Dict) -> Optional[str]:

    pachyderm_client = python_pachyderm.Client(
        host=os.environ['PACHYDERM_HOST'],
        port=os.environ['PACHYDERM_PORT'],
        tls=True,
        auth_token=os.environ['PACHYDERM_AUTH_TOKEN'])
    f = pachyderm_client.who_am_i()
    # print(f)
    for repo_name in repository_files.keys():
        print(f'Repo Name is: {repo_name}')
        now = datetime.datetime.now()
        print ("Current date and time before inserting into pachyderm : ", now.strftime("%Y-%m-%d %H:%M:%S"))
        with pachyderm_client.commit(repo_name, "master") as commit:
            for file in repository_files[repo_name] :
                source_url = repository_files[repo_name][file].source_url
                dest_path = repository_files[repo_name][file].destination_path
                #print(f'source path is: {source_url}')
                #print(f'destination path is: {dest_path}')
                try:
                    pachyderm_client.put_file_url(commit, dest_path, source_url)
                except Exception :
                    #print(f"exception caught while putting file to pachyderm")
                    exception_type, exception_obj, exception_tb = sys.exc_info()
                    print("Exception at line " + str(exception_tb.tb_lineno) + ": " + str(sys.exc_info()))
    now = datetime.datetime.now()
    print ("Current date and time after inserting into pahcyderm: ", now.strftime("%Y-%m-%d %H:%M:%S"))
