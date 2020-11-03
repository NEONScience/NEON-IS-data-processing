
##############################################################################################
#' @title Bulk convert JSON pipeline spec files to YAML format

#' @author
#' Robert Lee \email{rlee@battelleecology.org} \cr

#' @description Workflow. Given a path to a directory containing JSON pipleine specification files (specPath),
#' the script will create a YAML version of that pipeline spceificationfile, and store it in the specified direcotry.
#' Note that original JSON files are kept, and should be removed by hand before merging your branch with the master
#' branch of NEON-IS-data-processing
#' @return YAML versions of pipeline specification files in the specified directory.
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

# changelog and author contributions / copyrights
#   Robert Lee (2020-09-02)
#     original creation
##############################################################################################

# ------ Choose options --------

library(magrittr)

#Point to your pipeline spec files directory
specPath = "~/GitHub/NEON-IS-data-processing/pipe/parQuantumLine/"


#----------------------- Begin Program -----------------------

# discover our Json files
inputFiles=dir(path = specPath, pattern = ".json$", full.names = TRUE)

# make new file names to use
outputFiles=gsub(pattern = ".json$", replacement = ".yaml", x = inputFiles)

# loop thru our files, performing the conversion and writing the files out.
for(i in 1:length(inputFiles)){

  jsonlite::read_json(inputFiles[i]) %>%
    yaml::as.yaml() %>%
    gsub(pattern = "yes", replacement = "true") %>%
    gsub(pattern = "no", replacement = "false") %>%
    c("---",.) %>%
    writeLines(con =  outputFiles[i])

}

# there should now be working yaml files to use in the directory you specified above
