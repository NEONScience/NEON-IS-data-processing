#### Example script, for moving specific CFGLOC files for the prt in the location_active_dates step of pipeline dev
# Author: Robert Lee, 2020-08-05
# I love pipes. Lets use them.
library(magrittr)

# pfsDir <- "/scratch/pfs"
pfsDir <- "/home/NEON/glitt/pfs"#location_active_dates/"


# Go to pachd1
setActiveContext("pachd1")

# List all data files from the last pipeline step
latestPipelineFiles=listPachFiles(repo = "prt_merge_data_by_location", subDir = "prt/2019/01/01/*/*")

# Lets parse the directories in our pachd1 data, since we need on file per directory here
dirsNeeded= latestPipelineFiles %>%
  stringr::str_extract(pattern = "/CFGLOC[0-9]{6}/[a-z_]{1,}/") %>%
  gsub(pattern = "CFGLOC[0-9]{6}|/", replacement = "") %>%
  unique()

# make a list of data files, one per directoory type. 
# We'll copy them to the som and use these files to make our empty files, 
filesNeeded=c()
for(dir in dirsNeeded){
  files = latestPipelineFiles %>%
    stringr::str_extract(pattern = paste0("[aA-zZ_0-9-/]{1,}/", dir, "/[aA-zZ_0-9-]{1,}.[a-z]{1,}$")) %>%
    unlist() %>%
    na.omit() %>%
    as.character()
  
  if(length(files)>0){
    filesNeeded=c(filesNeeded, files[1])
  }
}

# copy our data files over to the som
getPachFiles(repo = "prt_merge_data_by_location", pachFiles = filesNeeded, somDirectory = paste0(pfsDir,"/empty_files/"))


# Loop thru the files we pulled down to som
for(file in dir(paste0(pfsDir,"/empty_files/prt/"), recursive=T, full.names=T)){
  
  # We need to make a new directory for our empty files, what is the name?
  newDir=file %>%
    stringr::str_extract(pattern = "/CFGLOC[0-9]{6}/[a-z_]{1,}/") %>%
    gsub(pattern = "CFGLOC[0-9]{6}|/", replacement = "")
  
  # some empty files need special stuff on the end. decide here
  if(newDir=="flags"){
    appendName="_flagsCal"
  }else if(newDir=="uncertainty_data"){
    appendName="_uncertaintyData"
  }else if(newDir=="uncertainty_coef"){
    appendName="_uncertaintyCoef"
  }else if(newDir=="location"){
    appendName="_location"
  }else{
    appendName=""
  }
  
  # Where will our new empty file live? Build directory path
  emptyDir=paste0(pfsDir,"/empty_files/prt/", newDir)
  
  # Make the directory, if needed
  if(!dir.exists(paths = emptyDir)){
    dir.create(paste0(pfsDir,"/empty_files/prt/", newDir))
  }
  
  # handle file types differently. Parquet: read in the file, save only columns. JSON: save '{}' out, apparently.
  if(grepl(pattern = "parquet", x = file)){
    actualData = NEONprocIS.base::def.read.parq(file)
    emptyData=actualData[-c(1:nrow(actualData)),]
    
    NEONprocIS.base::def.wrte.parq(data = emptyData, NameFile = paste0(emptyDir, "/prt_location_year-month-day", appendName, ".parquet"))
  }else if(grepl(pattern = "json", x = file)){
    writeLines(text="{}", con =  paste0(emptyDir, "/prt_location_year-month-day", appendName, ".json"))
  }
}

# clean out our bad file structures, before moving
system(paste0("rm -R ",pfsDir,"/empty_files/prt/2019/"))

# move our new files over. Currently this moves all of the stuff in
putPachFiles(repo="empty_files", pachDir = "prt", somDirectory = paste0(pfsDir,"/empty_files/prt"))

# OPTIONAL - Check out if it went OK
listPachFiles(repo = "empty_files", subDir = "/**")
