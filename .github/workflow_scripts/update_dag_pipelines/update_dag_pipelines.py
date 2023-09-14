import python_pachyderm
import environs
from urllib.parse import urlparse
import yaml
from pathlib import Path
import io
import os

def setup_client(pachd_address:str, pach_token:str):
    
    # Example of how to create a robot token with the required permissions for pipeline updates
    #   pachctl auth get-robot-token testrunner
    #   pachctl auth set project default repoOwner robot:testrunner
    
    pach_url = urlparse(pachd_address)
    host = pach_url.hostname
    port = pach_url.port
    if port is None:
        port = 80
    if pach_url.scheme == "https" or pach_url.scheme == "grpcs":
        tls = True
    else:
        tls = False
    return python_pachyderm.Client(host=host, port=port, tls=tls, auth_token=pach_token) 

def pipeline_files_from_pipe_lists(paths):
    # Get the list of pipeline yamls to update
    # paths: a list of type Path or string indicating the directory that contains pipeline specifications. 
    # Note that the directory must contain a pipe_list_*.txt file listing the pipeline specs that make up the DAG. 
    # The order of paths as well as the internal ordering of the pipe_list_*.txt file should be in the order they should be loaded into pachyderm.
    pipeline_files=[]
    for path in paths:
        print(f'Looking in directory {path} for pipeline list (pipe_list_*.txt)')
        # Load the ordered pipeline list 
        for pipe_list_file in Path(path).rglob('pipe_list_*.txt'):
            print(f'Reading pipeline list {pipe_list_file}')
            with open(pipe_list_file, 'r') as file:
                for line in file:
                    line=line.rstrip('\n').strip()
                    if len(line) > 0:
                        pipeline_files.append(Path(path,line))
            break # Only read the first pipe_list file (should only be 1)
    print(f'{len(pipeline_files)} total pipelines will be updated')
    return(pipeline_files)


def create_pipeline_dict(pipeline_files):
    # Read in pipeline yaml files as a dictionary list
    # pipeline_files: a list of paths to pipeline specifications in yaml format
    pipelines = {}
    for pipe_yaml in pipeline_files:
        if os.path.isfile(pipe_yaml):
            print(f'Reading {pipe_yaml}')
            with open(pipe_yaml, 'r') as file:
                pipe = yaml.safe_load(file)
                pipe["update"] = True
                pipelines[pipe["pipeline"]["name"]] = pipe
    return pipelines

def update_dag(client, pipelines, transaction: bool):
    # Deploy pipeline updates to Pachyderm
    if transaction is True:
        print('Using transaction')
        with client.transaction() as t:
            for pipe in pipelines:
                print(f'Updating {pipe}')
                req = python_pachyderm.parse_dict_pipeline_spec(pipelines[pipe])
                client.create_pipeline_from_request(req)
    else:
        for pipe in pipelines:
            print(f'Updating {pipe}')
            req = python_pachyderm.parse_dict_pipeline_spec(pipelines[pipe])
            client.create_pipeline_from_request(req)
        

def main():
    env = environs.Env()
    pachd_address = os.environ["PACHD_ADDRESS"] # e.g. "grpcs://pachd.nonprod.gcp.neoninternal.org:443"
    pach_token = os.environ["PACH_TOKEN"] # auth token (string). Needs repoOwner roles
    paths = env.list('PATHS') # list of path strings to the directories with pipeline specs to update
    transaction = env.bool('TRANSACTION',True) # Do updates within a single transaction (recommended)
    
    # Get the list of pipeline yamls to update
    # pipeline_files must be in the desired order of loading to pachyderm. Thus, the order of paths as well as the internal ordering of the pipe_list file matters.
    pipeline_files = pipeline_files_from_pipe_lists(paths)

    # Create pipeline dictionary from pipeline files
    pipelines = create_pipeline_dict(pipeline_files)

    # Connect to pachyderm    
    client = setup_client(pachd_address,pach_token)

    # Update the pipelines
    update_dag(client,pipelines,transaction)
    


if __name__ == "__main__":
    main()


