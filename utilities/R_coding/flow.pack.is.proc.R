# following http://hilaryparker.com/2014/04/29/writing-an-r-package-from-scratch/
# Run this code in sections to define the package name, create the package, and package/install it.
# The Defining section (1st) and Packaging and Installing section (3rd) can be run without the second
# section if the package already exists and you want to update it


#-----------------------------------------------------
# 1. Define working directory and name(s) of package(s)

# working directory
DirWrk00 <-
  
  # Cove
  "/home/NEON/csturtevant/R/NEON-IS-data-processing-homeDir"

  # Feel free to add your repo path! Just comment the ones not being used.


#name(s) of package(s)
namePack <- c("NEONprocIS.base",
              "NEONprocIS.cal",
              "NEONprocIS.qaqc",
              "NEONprocIS.stat",
              "NEONprocIS.wq")[4]



###
# start loop around packages
for(idxNamePack in namePack) {
###



  # Install and load required packages
  
    # install libraries in case missing
    if(!("devtools" %in% rownames(installed.packages()))) install.packages("devtools")
    if(!("roxygen2" %in% rownames(installed.packages()))) install.packages("roxygen2")

    # most current development version of roxygen2:
    # if(!("roxygen2" %in% rownames(installed.packages()))) devtools::install_github("klutometis/roxygen")
  
    # load libraries
    lapply(c("devtools","roxygen2"), library, character.only = TRUE)[[1]]

  
  
  
  #-----------------------------------------------------
  # 2. Create the Package
  
  # Set the working directory
  DirWrk <- paste0(DirWrk00, "/pack")

  setwd(DirWrk)
  
  # Test if package name exists, if so don't re-create (just update) it
  dlist <- dir(path=".",pattern=idxNamePack,full.names=FALSE)
  
  # Create package (adjust name here)
  if(length(which(idxNamePack==dlist)) == 0) usethis::create_package(idxNamePack)
  
  
  
  #-----------------------------------------------------
  # 3. Package it up & Install it
  
  # 1. adjust the contents of the DESCRIPTION file
  # 2. add files...
  #     ... functions to package subdirectory /R
  #     ... demo files to package subdirectory /demo
  #     ... vignette files to package subdirectory /vignettes
  # 3. Add comments to the beginning of each function with the format: #'  -- These
  #     will be used to create help documentation using the roxygen2 package 
  
  # change working directory to package directory
  setwd(paste0(DirWrk, "/", idxNamePack))
  
  # If the data-raw folder does not exist, create it. This is where the flow scripts to
  # create package data should be kept. This folder will be included in the .Rbuildignore
  # file of the package
  dlist <- dir(path=".",pattern="data-raw",full.names=FALSE)
  if(length(dlist) == 0) {
    
    # Create data-raw folder and add it to .Rbuidignore (default when using use_data_raw function)
    usethis::use_data_raw()
    
  } else {
    
    # If data-raw folder exists already, make sure it is included in .Rbuildignore
    usethis::use_build_ignore("data-raw", escape = TRUE)
  }
  
  # Create Roxygen documentation 
  document()
  
  # remove any existing version of package in the library location
  remove.packages(idxNamePack)
  
  # Install the package
  setwd("..")
  install(idxNamePack)
  detach(paste0("package:", idxNamePack), unload=TRUE, character.only = TRUE)
  library(paste0(idxNamePack), character.only = TRUE)



###
}
# end loop around packages
###



#test package by calling some help files that should have been generated
# ?def.cart.az
# ?fd_FootFluxKm01
# ?ersp
