##############################################################################################
#' @title  Apply custom flags for radiation sensors. 

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr

#' @description Workflow. Pull in relevant data (flags, location contexts, thresholds, schema etc), source custom functions for applying flags
#' 
#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/location-id, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The location-id is the unique identifier of the location. \cr
#'
#' Nested within this path are (at a minimum) the folders:
#'         /data
#'         /flags

#' The flags folder holds two files containing basic plausibility and calibration quality flags for 
#' the central processing day only. These files should respectively be named in the convention:
#' SOURCETYPE_LOCATIONID_YYYY-MM-DD_flagsPlausibility.parquet
#' SOURCETYPE_LOCATIONID_YYYY-MM-DD_flagsCal.parquet
#' All other files in this directory will be ignored.
#'
#' The threshold folder contains a single file named thresholds.json that holds threshold parameters
#' applicable to the smoothing algorithm. 
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#'
#' @param SchmQf (Optional). A json-formatted character string containing the schema for the custom flags being applied.
#' 
#' @param FlagsRad (Optional). A list of flags to run. If not provided it will bypass script without producing any flags 
#' 
#' @param termTest (Optional). terms to run for shading flag. If NULL and Shadow is in FlagsRad will result in errored datum.
#' 
#' @param shadowSource (Optional). Which type of shadow is expected. Options include LR Cimel Misc to distinguish between 
#'different types of shading sources from different directions. If not supplied, but shadow check is run script will fail. 
#' 
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the data/flags/threshold folders in the input path that are to be copied with a 
#' symbolic link to the output path (i.e. carried through as-is). Note that the 'stats' and 'flags' directories 
#' are automatically populated in the output and cannot be included here.
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#'
#' @return A repository in DirOutBase containing the radiation flags created in previous modules as well as a new file for custom flags
#' replaces BASE_REPO of argument \code{DirIn} but otherwise retains the child directory structure of the 
#' input path. The terminal directories for each datum include, at a minimum, 'stats' and 'flags'. The stats folder
#' contains hourly and daily output for 3 data days centered on the day indicated in the path structure, one 
#' file per day and output frequency (i.e. 6 total files), named appropriately. The output in the stats folder 
#' contains the precipitation sum, uncertainty estimates, quality flags specific to the smoothing algorithm, 
#' and final quality flag. The 'flags' folder will contain the plausibility and calibration flags aggregated
#' across the 3 strain gauges at the original measurement frequency for later processing into quality metrics.
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' # NOT RUN
#' DirIn='/scratch/pfs/cmp_analyze_pad_and_qaqc_plau/2025/03/31/'
#' DirOutBase='/scratch/pfs/out_tb'

#' wrap.rad.flags.cstm(DirIn,DirOutBase,DirSubCopy,FlagsRad,termTest)

#' @seealso Currently none

# changelog and author contributions / copyrights
#   Teresa Burlingame  (2025-09-15)
#     Initial creation

##############################################################################################
wrap.rad.flags.cstm <- function(DirIn,
                                DirOutBase,
                                SchmQf=NULL,
                                termTest=NULL,
                                DirSubCopy=NULL,
                                FlagsRad=NULL,
                                shadowSource=NULL,
                                log=NULL
){

  library(dplyr)
  
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Gather info about the input directory and create the output directory.
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn,log=log)
  dirInData <- fs::path(DirIn,'data')
  dirOut <- fs::path(DirOutBase,InfoDirIn$dirRepo)
  dirOutQf <- fs::path(dirOut,'flags')
  dirOutData <- fs::path(dirOut,'data')
  NEONprocIS.base::def.dir.crea(DirBgn = dirOut,
                                DirSub = c('flags', 'data'),
                                log = log)


  # Copy with a symbolic link the desired subfolders 
  DirSubCopy <- base::unique(DirSubCopy)
  if(base::length(DirSubCopy) > 0){

    NEONprocIS.base::def.dir.copy.symb(DirSrc=fs::path(DirIn,DirSubCopy),
                                       DirDest=dirOut,
                                       LnkSubObj=TRUE,
                                       log=log)
  } 
  
  #no custom flags were given, passing through module. 
  if (is.null(FlagsRad)){
    log$info("No custom flags specified, skipping datum")
    return()
  }
  
  # Take stock of our data files.
  fileData <- base::list.files(dirInData,pattern='.parquet',full.names=FALSE)

  # Read the datasets 
  data <- NEONprocIS.base::def.read.parq.ds(fileIn=fs::path(dirInData,fileData),
                                            VarTime='readout_time',
                                            RmvDupl=TRUE,
                                            Df=TRUE, 
                                            log=log)
  
  #initialize flagsDf to have same readout_time as data of interest
  flagDf <- data.frame(readout_time = data$readout_time)

  if("Cmp22Heater" %in% FlagsRad){
    source("./def.cmp22.heater.flags.R")
    flagDf <- def.cmp22.heater.flags(data, flagDf, log)
  }

  #run radiation shading script
  if("Shadow" %in% FlagsRad){
    source("./def.rad.shadow.flags.R")
    flagDf <- def.rad.shadow.flags(DirIn, flagDf, termTest,shadowSource, log)
  }

  # Create output filenames
  nameFileIdxSplt <- strsplit(fileData, '.', fixed = TRUE)[[1]]
  base_name <- paste0(nameFileIdxSplt[1:(length(nameFileIdxSplt) - 1)], collapse = '.')
  extension <- utils::tail(nameFileIdxSplt, 1)
  
  nameFileQfOutFlag <- paste0(base_name, "_customFlags.", extension)
  pathFileQfOutFlag <- fs::path(dirOutQf,nameFileQfOutFlag)
      
      rptWrte <-
        base::try(NEONprocIS.base::def.wrte.parq(
          data = flagDf,
          NameFile = pathFileQfOutFlag,
          log=log
        ),
        silent = TRUE)
      
      if ('try-error' %in% base::class(rptWrte)) {
        log$error(base::paste0(
          'Cannot write output to ',
          nameFileQfOutFlag,
          '. ',
          attr(rptWrte, "condition")
        ))
        stop()
      } else {
        log$info(base::paste0(
          'Wrote updated flags data to file ',
          nameFileQfOutFlag
        ))
      }

  return()
} 

