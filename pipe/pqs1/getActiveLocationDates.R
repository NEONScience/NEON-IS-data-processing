#### Example script, for moving specific CFGLOC files for the pqs1 in the location_active_dates step of pipeline dev
# Author: Robert Lee, 2020-08-05
# I love pipes. Lets use them.
library(magrittr)

# (Edit GL) In the case that you don't have write access to the som directory, you'll need to save the .json files into your own user directory:
# somDirectory <- "/scratch/pfs/location_active_dates/"
somDirectory <- "/home/NEON/jroberti/pfs/location_active_dates/"
if(!dir.exists(somDirectory)){
  dir.create(somDirectory)
}

# Go to pachd1
setActiveContext("pachd1")

# Extract the CFGLOCs found in the pqs1_merge_data_by_location repo, for 2019-01
neededCFGs=listPachFiles1(repo = "pqs1_merge_data_by_location", subDir = "/pqs1/2019/01/**") %>%
  stringr::str_extract(pattern = "CFGLOC[0-9]{6}") %>%
  unlist() %>%
  unique()

#alteration for pqs1 data (2020-10-06):
#neededCFGs<-c(neededCFGs[3:5],"CFGLOC100353","CFGLOC100357","CFGLOC100361","CFGLOC100368","CFGLOC100369")

# Switch to pachd2
setActiveContext("pachd2")

#### [START] If I need to pull the data files over from a different commit than on the Master branch:
# List the files in location_active_dates for my sensor, 2019-01, THEN subset to only files that match neededCFGLOCs
# neededFiles=listPachFiles2(repo = "location_active_dates", subDir = "bff7b79c729c4ddba4b98573243a1590:/pqs1/2019/01") %>%  #/pqs1/2019/01/**
# .[grepl(pattern = paste0(neededCFGs, collapse = "|"), x=.)] #This bit subsets
neededFiles=listPachFiles2(repo = "location_active_dates", subDir = "master:/pqs1/2019/01/**") %>%  #/pqs1/2019/01/**
  .[grepl(pattern = paste0(neededCFGs, collapse = "|"), x=.)]

#### [END] If I need to pull the data files over from a different commit than on the Master branch:

# copy those files over to /scratch/pfs/location_active_dates/ on the som (if from commit):
getPachFiles(repo = "location_active_dates", pachFiles = neededFiles, somDirectory = somDirectory,commitID = "bff7b79c729c4ddba4b98573243a1590")
# If from master branch:
getPachFiles(repo = "location_active_dates", pachFiles = neededFiles, somDirectory = somDirectory)



# Switch back to pachd1
setActiveContext("pachd1")

# put the SOM server files in the repo
putPachFiles(repo = "location_active_dates", somDirectory = somDirectory)

# OPTIONAL - Check out if it went OK
listPachFiles1(repo = "location_active_dates", subDir = "/**")
