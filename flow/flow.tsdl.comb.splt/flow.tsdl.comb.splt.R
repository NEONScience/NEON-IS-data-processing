###############################################################################
#' @title Convert Temperature at Specific Depth in Lakes from wide to individual
#' files by HOR.VER

#' @author
#' Guy Litt/Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Convert temperature at specific depth in lakes L1 
#' data from wide format to long format for each depth. 
#' 
#'
#' General code workflow:
#'    Parse input parameters
#'    Determine datums to process (set of files/folders to process as a single unit)
#'    For each datum:
#'      Create output directories and copy (by symbolic link) unmodified components
#'      Read in and combine location, stats, and quality metrics files for each input datum
#'      Rename 
#'      Organize data as a list of time variable(interval), sub-list of HOR.VER locations each containing
#'      a data.frame object. Thus each data.frame represents a unique time interval and depth combination.
#'      Write out the combined data file for each time variable and HOR.VER (depth) location
#'
#' This script is run at the command line with the following arguments. Each argument must be a string
#' in the format "Para=value", where "Para" is the intended parameter name and "value" is the value of
#' the parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the
#' parameter will be assigned from the system environment variable matching the value string.
#'
#' The arguments are:
#'
#' 1. "DirIn=value", where value is the path to the input data directory. NOTE: This path must be a
#' parent of the terminal directory where the data to be combined resides. See argument "DirComb"
#' below to indicate the terminal directory.
#'
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any
#' number of parent and child directories of any name, so long as they are not 'pfs', the same name
#' as the terminal directory indicated in argument "DirComb", or recognizable as the 'yyyy/mm/dd'
#' structure which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained
#' in the folder.
#'
#' For example: "DirIn=value", where DirIn is the Input path = /pfs/proc_group/tchain/2019/01/01
#'
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion
#' of DirIn.
#'
#' 3. "NameVarTime=value", where value is the name of the time variable(s) common across all
#' files, separated by pipes. Note that any missing timestamps among the files will be filled with NA values.
#' e.g. "001|030"
#'
#' 4. "FileSchmMapDepth=value", where value is the file path to the schema that maps named location depths
#'  to data's depth column naming convention.
#'
#' 5. "FileSchmMapCols=value" (optional), where value is the file path to the schema that maps existing
#' strings in data column names to substitute values. (e.g. WaterTemp becomes tsdWaterTemp). 
#'
#' 6. "NameFileSufxRm=value" (optional), where value is a character vector of suffix(es) to remove from the output
#' file name (before any extension). For example, if the shortest file name found in the input files is 
#' "prt_CFGLOC12345_2019-01-01_basicStats.parquet", and the input argument is "NameFileSufxRm=_basicStats", then the 
#' output file will be "prt_CFGLOC12345_2019-01-01.parquet". Default is c("basicStats","qualityMetrics") for removal.
#'  
#' 7. "MrgeCols=value" (optional), where values is the name of the columns that all data files contain 
#' for merging. Each column name is separated by pipes. Default "startDateTime|endDateTime".
#' 
#' 8. "LocDir=value" (optional), where LocDir is the subdirectory inside DirIn/CFGLOCXXXXX/ containing
#'  location file(s). Default "location".
#' 
#' 9. "StatDir=value" (optional), where StatDir is the subdirectory inside DirIn/CFGLOCXXXXX/ containing
#'  the stats data files for each time variable. Default "stats".
#' 
#' 10. "QmDir=value" (optional), where QmDir is the subdirectory inside DirIn/CFGLOCXXXXX/ containing
#'  quality metrics files for each time variable. Default "quality_metrics".
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.
#'
#' @return A single file for each HOR.VER contained the merged data in DirOut, where DirOut replaces BASE_REPO but
#' otherwise retains the child directory structure of the input path. The file name will be the same
#' as the shortest file name found in the input files, with '_HOR.VER.TMI' added as suffix prior to the
#' file extension.
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
# Sys.setenv(DIR_IN='~/pfs/tempSpecificDepthLakes_level1_group/2025/05/10/temp-specific-depths-lakes_BARC103100')
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
# arg <- c("DirIn=$DIR_IN","DirOut=~/pfs/out","DirErr=~/pfs/out/errored_datums","NameVarTime=001|030", 
#          "FileSchmMapDepth=~/R/NEON-IS-data-processing/flow/flow.tsdl.comb.splt/tests/testthat/pfs/schemas/tsdl_map_loc_names.avsc",
#          "FileSchmMapCols=~/R/NEON-IS-data-processing/flow/flow.tsdl.comb.splt/tests/testthat/pfs/schemas/tsdl_col_term_subs.avsc")
# rm(list=setdiff(ls(),c('arg','log')))

#' @seealso Currently none.

# changelog and author contributions / copyrights
#   Guy Litt (2021-04-13)
#     original creation/adapted from flow.data.comb.ts.R by CS
#   Guy Litt (2021-05-11)
#     update documentation
#   Nora Catolico (2025-06-17)
#     add logic to split into separate directories and update group files

##############################################################################################
library(dplyr)
library(data.table)
library(NEONprocIS.base)
library(stringr)

source("./wrap.file.comb.tsdl.splt.R")
source("./wrap.schm.map.char.gsub.R")
source("./def.map.char.gsub.R")
source("./def.schm.avro.pars.map.R")
source("./def.find.mtch.str.best.R")
# Start logging
log <- NEONprocIS.base::def.log.init()

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly = TRUE)

# Parse the input arguments into parameters
Para <-
  NEONprocIS.base::def.arg.pars(
    arg = arg,
    NameParaReqd = c("DirIn", "DirOut", "DirErr", "NameVarTime", "FileSchmMapDepth"),
    NameParaOptn = c("FileSchmMapCols",
                     "MrgeCols",
                     "LocDir",
                     "StatDir",
                     "QmDir",
                     "GroupDir",
                     "NameFileSufxRm",
                     "CorrColNams"),
    log = log
  )
# --------------------------------------------------------------------------- #
#               Assign default params if un-defined
# --------------------------------------------------------------------------- #
if(base::is.null(Para$MrgeCols)){
  Para$MrgeCols <- base::c("startDateTime", "endDateTime")
}
if(base::is.null(Para$LocDir)){
  Para$LocDir <- "location"
}
if(base::is.null(Para$StatDir)){
  Para$StatDir <- "stats"
}
if(base::is.null(Para$QmDir)){
  Para$QmDir <- "quality_metrics"
}
if(base::is.null(Para$NameFileSufxRm)){
  Para$NameFileSufxRm <- base::c("basicStats","qualityMetrics")
}
if(base::is.null(Para$CorrColNams)){
  Para$CorrColNams <- TRUE
}


# Combine the location, stats, and quality_metrics directory
Para$DirComb <- base::c(Para$LocDir,Para$StatDir,Para$QmDir)


# --------------------------------------------------------------------------- #
# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))

log$debug(
  base::paste0(
    'All files found in the following directories will be combined: ',
    base::paste0(Para$DirComb, collapse = ',')
  )
)

log$debug(base::paste0('Common time intervals expected in directories: ', base::paste(Para$NameVarTime, collapse = ", ") ))

# What are the expected subdirectories of each input path
log$debug(base::paste0(
  'Minimum expected subdirectories of each datum path: ',
  base::paste0(Para$DirComb, collapse = ',')
))

# --------------------------------------------------------------------------- #
# Find all the input paths (datums). We will process each one.
# --------------------------------------------------------------------------- #
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub =  Para$DirComb,
                              log = log)


# --------------------------------------------------------------------------- #
#                          Process each datum path
# --------------------------------------------------------------------------- #
for (idxDirIn in DirIn) {
  log$info(base::paste0('Processing path to datum: ', idxDirIn))
  
  # Get directory listing of input director(ies). We will combine these files.
  fileNamz <-
    base::list.files(base::paste0(idxDirIn, '/', Para$DirComb))
  filePath <-
    base::list.files(base::paste0(idxDirIn, '/', Para$DirComb), full.names =
                       TRUE)
  
  # Gather info about the input directory (including date).
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn,log=log)
  
  #find group directory
  Site <- sub(".*(temp-specific-depths-lakes_[^/]+)/.*", "\\1", idxDirIn)
  SiteDir <- sub(paste0(Site,".*"), Site, DirIn)
  log$debug(base::paste0('Site directory: ', SiteDir))
  GroupDir <- paste0(SiteDir,"/group")
  log$debug(base::paste0('Group directory: ', GroupDir))
  
  # Combine data files
  dataLs <- NULL
  dataLs <- # NEONprocIS.base::
   wrap.file.comb.tsdl.splt(filePths = filePath,
                           nameVarTime = Para$NameVarTime,
                           mrgeCols = Para$MrgeCols,
                           locDir = Para$LocDir,
                           statDir = Para$StatDir,
                           qmDir = Para$QmDir,
                           nameSchmMapDpth = Para$FileSchmMapDepth,
                           nameSchmMapCols = Para$FileSchmMapCols,
                           log = log)
  
  # Separate by timing index:
  for(nameVarTime in Para$NameVarTime){
    dataTime <- dataLs[[nameVarTime]]
    
    log$debug(base::paste0(
      'HOR.VER locations found in the combined data files: ',
      base::paste0(base::names(dataTime), collapse = ",")
    ))
    
    for(nameLoc in base::names(dataTime)){
      data <- dataTime[[nameLoc]]
      
      #create output directories
      idxDirOut <- base::paste0(Para$DirOut, InfoDirIn$dirRepo)
      idxDirOutHORVER <- base::paste0(idxDirOut, '/', nameLoc)
      NEONprocIS.base::def.dir.crea(DirBgn = idxDirOut,
                                    DirSub = nameLoc,
                                    log = log)
      
      NEONprocIS.base::def.dir.crea(DirBgn = paste0(idxDirOut,"/",nameLoc),
                                    DirSub = "data",
                                    log = log)
      NEONprocIS.base::def.dir.crea(DirBgn = paste0(idxDirOut,"/",nameLoc),
                                    DirSub = "group",
                                    log = log)
        
        
      # Take stock of the combined data
      nameCol <- base::names(data)
      log$debug(base::paste0(
        'Columns found in the combined data files: ',
        base::paste0(nameCol, collapse = ',')
      ))
      
      # ----------------------------------------------------------------------- #
      # Remove suffix strings, Take the shortest file name, insert HOR.VER.TMI
      # ----------------------------------------------------------------------- #
      # Subset to dat files that should begin with 'tchain'
      fileDat <- fileNamz[base::intersect(base::grep("tchain", fileNamz), base::grep(nameVarTime,fileNamz))] 
      
      # Remove the NameFileSufx strings to simplify output filename
      fileDats <-  base::lapply(Para$NameFileSufxRm, 
                                function(x) 
                                  base::gsub(pattern="__",replacement="_",
                                             base::gsub(pattern=x,replacement = "",
                                                        fileDat) ) )
      fileDats <- base::unlist(base::lapply(fileDats, 
                                            function(x) x[base::which.min(base::nchar(x))]))

      fileBase <-
        fileDats[base::nchar(fileDats) == base::min(base::nchar(fileDats))][1]
      # Insert the HOR.VER into the filename by replacing the nameVarTime with the standard HOR.VER.TMI
      fileBaseLoc <- base::gsub(nameVarTime,
                                base::paste0(nameLoc,".",nameVarTime),fileBase)
      
      fileOut <-
        NEONprocIS.base::def.file.name.out(nameFileIn = fileBaseLoc,
                                           sufx = "",
                                           log = log)
      log$debug(base::paste0(
        "Named output filepath as ", fileOut))
      
      nameFileOut <- base::paste0(idxDirOutHORVER, '/data/', fileOut)
      
      # ----------------------------------------------------------------------- #
      # Write out the data file.
      # ----------------------------------------------------------------------- #
      rptWrte <-
        base::try(NEONprocIS.base::def.wrte.parq(
          data = data,
          NameFile = nameFileOut,
          NameFileSchm = NULL,
          Schm = NULL,
          log=log
        ),
        silent = TRUE)
      if (base::class(rptWrte) == 'try-error') {
        log$error(base::paste0(
          'Cannot write combined file ',
          nameFileOut,
          '. ',
          attr(rptWrte, "condition")
        ))
        stop()
      } else {
        log$info(base::paste0('Combined data written successfully in file: ',
                              nameFileOut))
      }
      
      
      
      # ----------------------------------------------------------------------- #
      # Update group file
      # ----------------------------------------------------------------------- #
      
      groupFilePths <- base::list.files(GroupDir, full.names = TRUE)
      if(base::length(groupFilePths) > 1){
        log$info(base::paste0("Multiple location files exist. Using the first location file, ", groupFilePths[1]))
      }
      groupFilePth <- groupFilePths[1]
      groupData <- try(rjson::fromJSON(file=groupFilePth))
      VER <- sub(".*\\.", "", nameLoc)
      groupData$features[[1]]$VER<-VER
      
      # ----------------------------------------------------------------------- #
      # Write out group file
      # ----------------------------------------------------------------------- #
      
      groupFileName <- sub(".*/", "", groupFilePth)
      nameJsonOut <- base::paste0(idxDirOutHORVER, '/group/', groupFileName)
      
      rptWrte <-
        base::try(jsonlite::write_json(groupData, nameJsonOut, pretty = TRUE, auto_unbox = TRUE),
        silent = TRUE)
      if (base::class(rptWrte) == 'try-error') {
        log$error(base::paste0(
          'Cannot write combined file ',
          nameJsonOut,
          '. ',
          attr(rptWrte, "condition")
        ))
        stop()
      } else {
        log$info(base::paste0('Combined data written successfully in file: ',
                              nameJsonOut))
      }
      
    } # end loop on HOR.VER
  }
  
} # End loop around datum paths
