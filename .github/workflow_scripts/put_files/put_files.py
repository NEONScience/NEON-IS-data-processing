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
    in_paths = env.list("IN_PATHS") # The local path(s) to the file(s)/folder(s) that will be uploaded into pachyderm (e.g. "empty_files/prt""). If multiple, must match length of out_path
    out_paths = env.list("OUT_PATHS") # The path(s) where the file(s)/folder(s) will be placed in the pachydemr repo (e.g. "prt"). If multiple, must match length of in_path

    # Create dictionary of in_path:out_path pairs
    in_out_paths = {in_paths[i]: out_paths[i] for i in range(len(in_paths))}
    
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
    with client.pfs.commit(branch=branch) as commit:
      for in_path,out_path in in_out_paths.items():
        if Path(in_path).is_dir():
          client.pfs.put_files(commit=commit,source=in_path,path=out_path)
          print(f'Put path:',in_path,' into ',repo_name+"@"+branch_name+out_path)
        else:
          with open(in_path, "rb") as source:
            client.pfs.put_file_from_file(commit=commit,path=out_path,file=source,append=False)
            print(f'Put file:',in_path,' into ',repo_name+"@"+branch_name+out_path)
    

if __name__ == "__main__":
    main()


