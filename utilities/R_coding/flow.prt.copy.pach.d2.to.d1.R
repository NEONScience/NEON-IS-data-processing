#### Example script, for moving specific CFGLOC files for the prt in the location_active_dates step of pipeline dev
# Author: Robert Lee, 2020-08-05
# I love pipes. Lets use them.

# somDirectory <- "/scratch/pfs/location_active_dates/"
somDirectory <- "/home/NEON/glitt/pfs/location_active_dates/"
if(!dir.exists(somDirectory)){
  dir.create(somDirectory)
}

library(magrittr)

# Go to pachd1
setActiveContext("pachd1")



# Extract the CFGLOCs found in the prt_merge_data_by_location repo, for 2019-01
neededCFGs=listPachFiles(repo = "prt_merge_data_by_location", subDir = "/prt/2019/01/**") %>%
  stringr::str_extract(pattern = "CFGLOC[0-9]{6}") %>%
  unlist() %>%
  unique()

# Switch to pachd2
setActiveContext("pachd2")

# List the files in location_active_dates for my sensor, 2019-01, THEN subset to only files that match neededCFGLOCs
neededFiles=listPachFiles(repo = "location_active_dates", subDir = "/prt/2019/01/**") %>%
  .[grepl(pattern = paste0(neededCFGs, collapse = "|"), x=.)] #This bit subsets

# copy those files over to /scratch/pfs/location_active_dates/ on the som
getPachFiles(repo = "location_active_dates", pachFiles = neededFiles, somDirectory = somDirectory)#)




# Switch back to pachd1
setActiveContext("pachd1")

# put the SOM server files in the repo
putPachFiles(repo = "location_active_dates", somDirectory = somDirectory)

# OPTIONAL - Check out if it went OK
listPachFiles(repo = "location_active_dates", subDir = "/**")
