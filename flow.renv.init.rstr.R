# This script initializes/re-initializes the renv.lock file for the chosen module or local 
# package. The renv.lock file is a listing of the exact packages used by the module/package and 
# their versions as existed in the top-facing library when renv.lock was last created/updated, 
# i.e the local packrat library or user library > system library). Adjust the options below to 
# suite your needs, e.g. to save and use and/or use the packrat library within the module or 
# package folder.
#
# If a lockfile (renv.lock) already exists, the user will be interactively asked whether to 
# restore from it or reinitialize it from the current top-facing library. Do not use this script
# to update the lockfile after installing packages in the packrat library (i.e. when using 
# install.packages when the packrat library is the top-facing library). In that case, use 
# renv::snapshot(</path/to/module/or/package/>) to update the lockfile, otherwise the packrat 
# library will be reinitialized from the system library, erasing all your recent install work.


# Module or local package directory
dirWork <- '/scratch/SOM/Github/RstudioServer/NEON-IS-data-processing/NEON-IS-data-processing/pack/NEONprocIS.base'
PackIgnr <- c('NEONprocIS.base','NEONprocIS.cal','NEONprocIS.qaqc') # These should already be in the respective docker containers

# Keep and use the local project that renv creates when creating/updating the lockfile 
# in dirWork? If TRUE, note .Rprofile and .Rproj files will be created/retained in dirWork, 
# and R will be restarted in this new or existing project. If FALSE, any resulting .Rprofile 
# and .Rproj files will be deleted. R may or may not restart depending on the options selected
# below.
KeepProj <- FALSE
  
# Keep the local library in dirWork that results from creating/updating the lockfile? If TRUE, 
# the local library is retained. If FALSE, it is deleted. Cannot be FALSE if KeepProj is TRUE.
KeepLocalLib <- TRUE

# Use the local library created/updated in dirWork after creating/updating the lockfile?
# If TRUE, any package updates or new installations will be installed in the local library. 
# Any libraries used in the R session will first be loaded from the local library. If FALSE, 
# the R session will quit and must be reloaded. 
# Cannot be TRUE if KeepLocalLib is FALSE, and cannot be FALSE if KeepProj is TRUE. 
# In other  words, you cannot use a local library you chose to delete, and you must use the 
# local library if you decided to keep the created/existing project files.
UseLocalLib <- FALSE



#----------------------- Begin Program -----------------------

# Error check the options
if(KeepProj == TRUE && KeepLocalLib == FALSE){
  stop('KeepLocalLib cannot be FALSE if KeepProj is TRUE. Check options.')
} else if (KeepLocalLib == FALSE && UseLocalLib == TRUE) {
  stop('UseLocalLib cannot be TRUE if KeepLocalLib is FALSE. Check options.')
} else if (KeepProj == TRUE && UseLocalLib == FALSE){
  stop('UseLocalLib cannot be FALSE if KeepProj is TRUE. Check options.')
}


# Ensure that package 'renv' is installed
if(!("renv" %in% rownames(installed.packages()))) install.packages("renv")

# Change the working directory to the module/package directory
# MUST DO THIS - if not the ignored packages are not actually ignored
base::setwd(dirWork)

# Initialize dependency management
renv::settings$ignored.packages(PackIgnr)
renv::init(dirWork,restart=KeepProj)


# If there is already a lockfile, you will be asked whether to restore from the lockfile, 
# discard the lockfile and re-initialize it, or to activate the project but do nothing else.
# Choose the option that suites the desired use of this script. For example, if wanting to 
# In any case, a .Rprofile file will be created, and potentially a *.Rproj file. Delete them.
if(KeepProj == FALSE){
  base::suppressWarnings(base::file.remove(base::paste0(dirWork,'/',c('*.Rproj','.Rprofile','.Rhistory'))))
}

# Remove the local library
if(KeepLocalLib == FALSE){
  base::suppressWarnings(base::unlink(base::paste0(dirWork,'/',c('*.Rproj','.Rprofile')), recursive=TRUE))
}

# Don't use the local library
if(UseLocalLib == FALSE){
  base::quit()
}