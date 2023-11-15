from pachyderm_sdk import Client
from pachyderm_sdk.api import pfs, pps
import environs
from urllib.parse import urlparse
from pathlib import Path
import os

def setup_client(pachd_address:str, pach_token:str):
    
    # Example of how to create a robot token with the required permissions 
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


def main():
    env = environs.Env()
    pachd_address = os.environ["PACHD_ADDRESS"] # e.g. "grpcs://pachd.nonprod.gcp.neoninternal.org:443"
    pach_token = os.environ["PACH_TOKEN"] # auth token (string). Needs repoOwner roles
    repo_name = os.environ["REPO"] # The Pachyderm repo (e.g. "empty_files_prt")
    branch_name = os.environ["BRANCH"] # The branch of the pachyderm repo (e.g. "master")
    in_path = os.environ["IN_PATH"] # The local path to the folder that will be uploaded into pachyderm (e.g. "empty_files/prt"")
    out_path = os.environ["OUT_PATH"] # The path where the folder will be placed in the pachydemr repo (e.g. "prt")
    
    # Setup connection to Pachyderm
    client = setup_client(pachd_address,pach_token)
    
    # If the repo does not exist, create it and create the desired branch
    repo = pfs.Repo.from_uri(repo_name)
    try:
        client.pfs.create_repo(repo=repo)
    except:
        print(f'Did not create repo:',repo_name,' (likely already exists)')# Log the warning
        
    # Put the updated file(s) into Pachyderm
    branch = pfs.Branch.from_uri(repo_name+"@"+branch_name)
    if Path(in_path).is_dir():
        with client.pfs.commit(branch=branch) as commit:
            client.pfs.put_files(commit=commit,source=in_path,path=out_path)
    else:
        with client.pfs.commit(branch=branch) as commit:
            with open(in_path, "rb") as source:
                client.pfs.put_file_from_file(commit=commit,path=out_path,file=source,append=False)

if __name__ == "__main__":
    main()


