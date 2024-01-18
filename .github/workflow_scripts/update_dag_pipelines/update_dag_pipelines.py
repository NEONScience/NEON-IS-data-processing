from pachyderm_sdk import Client
from pachyderm_sdk.api import pfs, pps, transaction
from google.protobuf import json_format
from dataclasses import fields
import environs
from urllib.parse import urlparse
import yaml
import json
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
    return Client(host=host, port=port, tls=tls, auth_token=pach_token) 

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
    return(pipeline_files)


def create_pipeline_reqs(pipeline_files):
    # Read in pipeline yaml files and convert to pipeline request
    print('Reading pipeline files and generating pipeline requests')
    pipeline_reqs = {}
    for pipe_yaml in pipeline_files:
        print(f'Reading {pipe_yaml}')
        with open(pipe_yaml, 'r') as file:
            pipe = yaml.safe_load(file)
            pipe["update"] = True
            pipe_json = json.dumps(pipe)
            pipe_req = pps.CreatePipelineRequest().from_json(pipe_json) 
            pipeline_reqs[pipe["pipeline"]["name"]] = pipe_req
    return pipeline_reqs


def update_dag(pachd_address, pach_token, pipeline_reqs, transaction: bool, txn_id:str = ""):
    # Deploy pipeline updates to Pachyderm. Note - if the pipeline does not exist, it will be created.
    for pipe in pipeline_reqs:
        print(f'Updating {pipe}')
        pipeline_req = pipeline_reqs[pipe]
        
        # Start client again
        client = setup_client(pachd_address,pach_token)
        
        # Set transaction id
        if transaction is True:
            client.transaction._set_transaction_id(txn_id)
            print(f'Adding to transaction {client.transaction._get_transaction_id()}')
        
        # Update pipeline
        client.pps.create_pipeline(
            **{f.name: getattr(pipeline_req, f.name)
            for f in fields(pipeline_req)}
        )


        
def main():
    env = environs.Env()
    pachd_address = os.environ["PACHD_ADDRESS"] # e.g. "grpcs://pachd.nonprod.gcp.neoninternal.org:443"
    pach_token = os.environ["PACH_TOKEN"] # auth token (string). Needs repoOwner roles
    paths = env.list('PATHS') # list of path strings to the directories with pipeline specs to update
    update_scope = os.getenv("UPDATE_SCOPE",default='all') # Options are 'all' or 'changed'. If not specified, all will be updated. 'changed' will update any non-existent or changed pipelines.
    changed_files = env.list('CHANGED_FILES') # Paths to files that have changed since last commit
    transaction = env.bool('TRANSACTION',True) # Do updates within a single transaction (recommended)
    print(f'Changed files list = {changed_files}')
    
    # Get the list of pipeline yamls in the dag(s)
    # pipeline_files must be in the desired order of loading to pachyderm. Thus, the order of paths as well as the internal ordering of the pipe_list file matters.
    pipeline_files = pipeline_files_from_pipe_lists(paths)
    
    # Create pipeline requests from pipeline files
    pipeline_reqs = create_pipeline_reqs(pipeline_files)
    
    # Connect to pachyderm    
    client = setup_client(pachd_address,pach_token)
    
    if update_scope == 'all':
        print('All pipelines in the selected DAGs will be updated/created')
        pipeline_reqs_update = pipeline_reqs
        
    elif update_scope == 'changed':
        print('Changed or non-existent pipelines in the selected DAGs will be updated/created')
        
        # Pipeline requests in the DAG that have changed
        changed_files = [Path(i) for i in changed_files] # Make sure changed files are Paths
        pipeline_files_changed = [value for value in pipeline_files if value in changed_files]
        pipeline_reqs_changed = create_pipeline_reqs(pipeline_files_changed)
        print(f'{len(pipeline_reqs_changed)} pipelines will be updated')
        
        # Get pipeline names
        pipelines_dag = list(pipeline_reqs.keys())
        pipelines_changed = list(pipeline_reqs_changed.keys())
    
        # Find pipelines in the DAG that do not exist in Pachyderm (these will be created)
        pipelines_dag_nexist = [value for value in pipelines_dag if not client.pps.pipeline_exists(pps.Pipeline(name=value))]
        print(f'{len(pipelines_dag_nexist)} pipelines will be newly created')
        
        # Combine the list of pipelines that have changed or that do not yet exist in Pachyderm
        pipelines_update = set(pipelines_dag_nexist+pipelines_changed)
        pipeline_reqs_update = {k:pipeline_reqs[k] for k in pipelines_dag if k in pipelines_update}
    
    else:
        print("Environment variable UPDATE_SCAPE must be 'all' or 'changed'")
        raise Exception
    
    # Quit if nothing to do
    if len(pipeline_reqs_update) == 0:
        print('No pipelines to update. Exiting.')
        return None
    else:
        print(f'{len(pipeline_reqs_update)} total pipelines will be updated/created')
    
    # Start transaction
    txn_id = ""
    if transaction is True:
        print('Using transaction')
        txn=client.transaction.start_transaction()
        txn_id = txn.id
        if client.transaction.transaction_exists(transaction=txn) is False:
            print("Could not create transaction.")
            raise Exception
        else: 
            print(f'Created transaction {txn_id}')

    try:
        # Update the pipelines
        update_dag(pachd_address,pach_token,pipeline_reqs_update,transaction,txn_id)
        
        # Finish transaction
        if transaction is True:
            print(f'Finishing transaction {txn_id}')
            client.transaction.finish_transaction(transaction=txn)
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        if transaction is True:
            client.transaction.delete_transaction(transaction=txn)
            print(f'Deleted transaction {txn_id}')
        raise



if __name__ == "__main__":
    main()


