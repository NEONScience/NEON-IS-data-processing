#  +------------------------------------------+
#  | Useful Functions for Pachyderm work in R |
#  |  Written by Robert Lee, 2020-08-05       |
#  +------------------------------------------+

# set the active context for your pachyderm work.
# NOTE! you may need to modify the permissions of your config.json file. Do this in the terminal:
# chmod a+rw- ~/.pachyderm/config.json
# This will let anyone (including RStudio), work on the file.

setActiveContext=function(context){
  library(magrittr)
  # validate paramters
  if(context %in% c("pachd1", "pachd2", "pachd3")){
    # warn if a user moves to pachd3.
    if(context=="pachd3"){warning("Warning: Switching to pachd3. pachd3 is meant for production work only.")}
    
    # modify the context
    readLines(con = "~/.pachyderm/config.json") %>%
      gsub(pattern = '"active_context": "pachd[1-3]{1}', replacement = paste0('"active_context": "', context)) %>%
      writeLines(text = ., con = "~/.pachyderm/config.json")
    # inform that all went well
    message(paste0("Active context set to ", context))
    
    # Error if bad input
  }else{stop("Input must be one of 'pachd1', 'pachd2', or 'pachd3'")}
}

#------------------------------------------------------------------------------
# list all the files in a repo, at a given subdirectory in the repo.
# repo - the pachyderm repo of interest
# subDir - accepts literal file paths, or the recursive "/**" option to list all

listPachFiles=function(repo, subDir){
  library(magrittr)
  foundFiles=system(command = paste0('pachctl list file ', repo, '@master:', subDir), intern = T) %>%
    .[!grepl(pattern = "NAME", x = .)] %>%
    gsub(pattern = " file [0-9.]*[aA-zZ]{1,} ", replacement = "") %>%
    trimws()
  
  if(length(foundFiles)>0){
    return(foundFiles)
  }else{
    message(paste0("No files found in repo: '", repo, "', sub directory: '", subDir,"'"))
  }
}

#------------------------------------------------------------------------------
# Copy a specific list of files from a pachyderm repo to a server directory of your choice
# repo - the pachyderm repo of interest
# pachFiles - the path in the repo to the files you want to get. See listPachFiles to get these paths.
# somDirectory - where files will be stored. The file paths in pachFiles will be used below the directory you specify here.

getPachFiles=function(repo, pachFiles, somDirectory){
  # Loop thru files
  for(i in 1:length(pachFiles)){
    getCmd=paste0('pachctl get file -r ', repo, '@master:', pachFiles[i],' -o ', paste0(somDirectory, "/", pachFiles[i]))
    system(command = getCmd)
  }
}

#------------------------------------------------------------------------------
# Recursively put the files from a directory into a repo (at master) in pachyderm
# repo - the pachyderm repo of interest, where files will go.
# somDirectory - where files will be stored. The file structure within this directory will be directly copied to the repo.

putPachFiles=function(repo, pachDir=NULL, somDirectory){
  if(is.null(pachDir)){
    getCmd=paste0('pachctl put file -r ', repo, '@master:/ -f ', somDirectory)
  }else{
    getCmd=paste0('pachctl put file -r ', repo, '@master:/', pachDir, ' -f ', somDirectory)
  }
  system(command = getCmd)
}
