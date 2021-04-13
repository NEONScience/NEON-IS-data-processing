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
#'      Organize data as a list of time variable, list of HOR.VER locations, data.frame
#'      Write out the combined data file for each time variable and HOR.VER location
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
#' For example:
#' Input path = /pfs/proc_group/tchain/2019/01/01
#'
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion
#' of DirIn.
#'
#' 3. "NameDirCombOut=value", where value is the name of the output directory that will be created to
#' hold the combined file.
#'
#' 4. "NameVarTime=value", where value is the name of the time variable(s) common across all
#' files, separated by pipes. Note that any missing timestamps among the files will be filled with NA values.
#' e.g. "001|030"
#'
#' 5. "FileSchmMapDepth=value", where value is the file path to the schema that maps named location depths
#'  to data's depth column naming convention.
#'
#'
#' 6. "FileSchmMapCols=value" (optional), where CorrColNams is a logical value, instructing 
#' wrap.file.comb.tsdl.splt to attempt to correct column names (e.g. WaterTemp becomes tsdWaterTemp). 
#' Default TRUE.
#'
#' 7. "NameFileSufxRm=value" (optional), where value is a character vector of suffix(es) to remove from the output
#' file name (before any extension). For example, if the shortest file name found in the input files is 
#' "prt_CFGLOC12345_2019-01-01_basicStats.parquet", and the input argument is "NameFileSufxRm=_basicStats", then the 
#' output file will be "prt_CFGLOC12345_2019-01-01.parquet". Default is c("basicStats","qualityMetrics") for removal.
#'  
#' 8. "MrgeCols=value" (optional), where values is the name of the columns that all data files contain 
#' for merging. Each column name is separated by pipes. Default "startDateTime|endDateTime".
#' 
#' 9. "LocDir=value" (optional), where LocDir is the subdirectory inside DirIn/CFGLOCXXXXX/ containing
#'  location file(s). Default "location".
#' 
#' 10. "StatDir=value" (optional), where StatDir is the subdirectory inside DirIn/CFGLOCXXXXX/ containing
#'  the stats data files for each time variable. Default "stats".
#' 
#' 11. "QmDir=value" (optional), where QmDir is the subdirectory inside DirIn/CFGLOCXXXXX/ containing
#'  quality metrics files for each time variable. Default "quality_metrics".
#'
#' 
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.
#'
#' @return A single file containined the merged data in DirOut, where DirOut replaces BASE_REPO but
#' otherwise retains the child directory structure of the input path. The file name will be the same
#' as the shortest file name found in the input files, with '_combined' added as suffix prior to the
#' file extension.
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none
# setwd("~/R/NEON-IS-data-processing-glitt/flow/flow.tsdl.comb.splt/")
# Para <- base::list(DirIn = "~/pfs/tempSpecificDepthLakes_level1_group/tchain/2019/01/10/",
#                                       DirOut = "~/pfs/tsdl_comb_long/",
#                                       NameDirCombOut = "",
#                                       NameVarTime = c("001","030"),
#                                       MrgeCols =  c("startDateTime", "endDateTime"),
#                                       LocDir = "location",
#                                       StatDir = "stats",
#                                       QmDir = "quality_metrics",
#                                       FileSchmMapDepth = "./tests/testthat/pfs/schemas/tsdl_map_loc_names.avsc",
#                                       FileSchmMapCols = "./tests/testthat/pfs/schemas/tsdl_col_term_subs.avsc",
#                                       NameFileSufxRm = c("basicStats","qualityMetrics") )
#' @seealso Currently none.

# changelog and author contributions / copyrights
#   Guy Litt (2021-03-30)
#     original creation/adapted from flow.data.comb.ts.R by CS

# XTODO Rename column names in dp01/tempSpecificDepthLakes_stats_instantaneous.avsc to jive w/ pub wb:
# X 1. _UcrtExpn should be ExpUncert
# X 2. WaterTemp should be tsdWaterTemp
# X 3. Where does the extra QF come from for the QMs?
# TODO remove tsdWaterTempAlphaQF and tsdWaterTempBetaQF from instantaneous 001 data?

# FROM wrap.file.comb.tsdl.splt:
# TODO add SuspectCal to pub wb?
# TODO remove ConsistencyFail/Pass/NAQM from pub wb?
# X TODO add tsdWaterTempFinalQFSciRvw to dataset? -> NOT YET
# TODO depth11 doesn't exist yet for Mean/Minimum/Maximum/Variance stats (probably changes once CVAL files change)
# TODO add a colsKeep term or schema??

##############################################################################################
library(dplyr)
library(data.table)
library(NEONprocIS.base)

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
    NameParaReqd = c("DirIn", "DirOut", "NameDirCombOut", "NameVarTime", "FileSchmMapDepth"),
    NameParaOptn = c("ColKeep",
                     "MrgeCols",
                     "LocDir",
                     "StatDir",
                     "QmDir",
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
  base::paste0( # NameFileSufx="_basicStats_001" in tempSpecificDepthLakes_stats_instantaneous.yaml :paste0(
    'All files found in the following directories will be combined: ',
    base::paste0(Para$DirComb, collapse = ',')
  )
)
log$debug(
  base::paste0(
    'A single combined data file will be populated in the directory: ',
    Para$NameDirCombOut
  )
)

log$debug(base::paste0('Common time intervals expected in directories: ', base::paste(Para$NameVarTime, collapse = ", ") ))
# --------------------------------------------------------------------------- #
# # Read in the mapping schema
# log$debug(base::paste0(
#   'Output schema: ',
#   base::paste0(Para$FileSchmMapDepth, collapse = ',')
# ))
# 
# # TODO why parse nameSchmMapDpth here? The wrapper does this too, if nameSchmMapDpth = Para$FileSchmMapDepth
# if (base::is.null(Para$FileSchmMapDepth) || Para$FileSchmMapDepth == 'NA') {
#   # SchmComb <- NULL
#   nameSchmMapDpth <- NULL
# } else {
#   # SchmComb <-
#   #   base::paste0(base::readLines(Para$FileSchmMapDepth), collapse = '')
# 
#   # Parse the avro schema for output variable names
#   nameSchmMapDpth <- Para$FileSchmMapDepth#NEONprocIS.base::def.schm.avro.pars(Schm=SchmComb,log=log)$schmList$map  #$var$name
# }
# 
# if (base::is.null(Para$FileSchmMapCols) || Para$FileSchmMapCols == 'NA') {
#   nameSchmMapCols <- NULL
# } else {
#   nameSchmMapCols <- Para$FileSchmMapCols
# }


# Echo more arguments
log$debug(
  base::paste0(
    'Input columns (and their order) to populate in the combined output file (all if empty): ',
    base::paste0(Para$ColKeep, collapse = ',')
  )
)

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
  
  # Gather info about the input directory (including date) and create the output directory.
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  idxDirOut <- base::paste0(Para$DirOut, InfoDirIn$dirRepo)
  idxDirOutComb <- base::paste0(idxDirOut, '/', Para$NameDirCombOut)
  NEONprocIS.base::def.dir.crea(DirBgn = idxDirOut,
                                DirSub = Para$NameDirCombOut,
                                log = log)
  
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
        
        
      # Take stock of the combined data
      nameCol <- base::names(data)
      log$debug(base::paste0(
        'Columns found in the combined data files: ',
        base::paste0(nameCol, collapse = ',')
      ))
      
      # TODO add Para$ColAdd based on a file schema of expected columns that don't yet exist?
      
      
      # ----------------------------------------------------------------------- #
      # Filter and re-order the output columns
      # ----------------------------------------------------------------------- #
      if (!base::is.null(Para$ColKeep)) {
        # Check whether the desired columns to keep are found in the combined data
        chkCol <- Para$ColKeep %in% nameCol
        if (base::any(!chkCol)) {
          log$error(
            base::paste0(
              'Columns: ',
              base::paste0(nameCol[!chkCol], collapse = ','),
              'were not found in the input data. Check ColKeep input argument.'
            )
          )
          stop()
        }
        
        # Reorder and filter the output columns
        data <- data[Para$ColKeep]
        
        # Turn any periods in the column names to underscores
        base::names(data) <- base::sub(pattern='[.]',replacement='_',x=base::names(data))
        
        
        # if(base::is.null(SchmComb)){
        #   log$debug(base::paste0(
        #     'Filtered and re-ordered output columns : ',
        #     base::paste0(base::names(data), collapse = ',')
        #   ))
        # } else {
        log$debug(base::paste0(
          'Filtered and re-ordered input columns: ',
          base::paste0(base::names(data), collapse = ','),
          ' have had column names substituted using ',
          base::paste0(nameSchmMapDpth, collapse = ',')
        ))      
        # }
      } # data column filter/re-order
        
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
      
      nameFileOut <- base::paste0(idxDirOutComb, '/', fileOut)
      
      # ----------------------------------------------------------------------- #
      # Write out the file.
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
    } # end loop on HOR.VER
  }
  
} # End loop around datum paths
