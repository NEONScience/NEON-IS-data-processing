fileAdd <- './utilities/vignettes/schedulingSpec.yaml'

yamlAdd <- readLines(fileAdd)

# Folder with yaml files to edit
dirEdit <- './pipe/l0_data_loader'

#list all the files in the folder
file <- list.files(dirEdit,pattern = ".yaml",full.names = T, recursive = FALSE)

for(idxFile in file){
  yamlAddIdx <- yamlAdd # Reset 
  
  # Read the file and grab any pod_spec
  yaml <- readLines(idxFile)
  
  # Look for the scheduling spec block. If it's there, skip
  if(any(grepl(pattern='scheduling_spec',yaml))){next}
  
  # Look for the pod_patch block. We're going to grab the resource requests and edit the new pod_patch with them
  ptch <- grepl(pattern='pod_patch',yaml)
  linePtch <- which(ptch)
  
  if(length(linePtch) > 1){
    message(paste0('More than 1 pod_patch in yaml ', idxFile, '. Skipping.'))
    next
  } 
  
  if(length(linePtch) == 1){
    # Grab memory and cpu requests in pod_spec
    spltPtch <- strsplit(yaml[linePtch],',')[[1]]
    mem <- strsplit(spltPtch[3],'\"')[[1]][4] # Memory request
    cpu <- strsplit(spltPtch[6],'\"')[[1]][4] # CPU request
    
    # Replace values in new, readable, pod_spec block
    yamlAddIdx[22] <- sub("1G",mem,yamlAddIdx[22])
    yamlAddIdx[26] <- sub("0.5",cpu,yamlAddIdx[26])
   
    # Remove original pod_spec block
    yaml <- yaml[!ptch]
  }
  
  # Add scheduling spec block 
  yaml <- c(yaml,yamlAddIdx)
  
  # Write
  write(yaml,idxFile)
  
}

