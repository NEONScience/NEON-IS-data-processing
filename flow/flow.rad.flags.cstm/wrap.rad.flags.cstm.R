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
#'  @param Shadow (defaults F). Logical whether to source custom function def.rad.shadow.flags 
#'  
#' @param Cmp22Heater (defaults F). Logical whether to source custom function def.cmp.heater.flags 
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

#' wrap.rad.flags.cstm(DirIn,DirOutBase,DirSubCopy,shadow,cmp_heat)

#' @seealso Currently none

# changelog and author contributions / copyrights
#   Teresa Burlingame  (2025-09-15)
#     Initial creation

##############################################################################################
wrap.precip.pluvio.flags<- function(DirIn,
                                    DirOutBase,
                                    SchmQf=NULL,
                                    DirSubCopy=NULL,
                                    FlagsRad=NULL,
                                    log=NULL
){
  
  library(dplyr)
  
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Gather info about the input directory and create the output directory.
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn,log=log)
  dirInData <- fs::path(DirIn,'data')
  dirInQf <- fs::path(DirIn,'flags')

  dirOut <- fs::path(DirOutBase,InfoDirIn$dirRepo)
  dirOutQf <- fs::path(dirOut,'flags')
  dirOutData <- fs::path(dirOut,'data')
  NEONprocIS.base::def.dir.crea(DirBgn = dirOut,
                                DirSub = c('flags', 'data'),
                                log = log)
  
  # Copy with a symbolic link the desired subfolders 
  DirSubCopy <- base::unique(base::setdiff(DirSubCopy,c('flags','data')))
  if(base::length(DirSubCopy) > 0){

    NEONprocIS.base::def.dir.copy.symb(DirSrc=fs::path(DirIn,DirSubCopy),
                                       DirDest=dirOut,
                                       LnkSubObj=FALSE,
                                       log=log)
  }    
  
  # Take stock of our data files.
  fileData <- base::list.files(dirInData,pattern='.parquet',full.names=FALSE)

  # Read the datasets 
  data <- NEONprocIS.base::def.read.parq.ds(fileIn=fs::path(dirInData,fileData),
                                            VarTime='readout_time',
                                            RmvDupl=TRUE,
                                            Df=TRUE, 
                                            log=log)

  #run heater flag script
  if("Cmp22Heater" %in% FlagsRad){
    data <- def.cmp22.heater.flags(data, log)
  }
  
  #run radiation shading script
  if("Shadow" %in% FlagsRad){
    data <- def.rad.shadow.flags()
  }
  
  ##drop unwanted columns
  
  ##########should be more custom
  qfCust <- data %>% dplyr::select(c(readout_time, heaterQF, shadowQF))
  
  
  
#######get these names corrected
  nameFileQfOutFlag <- fileQfPlau

  nameFileQfOutFlag <- fs::path(dirOutQf,nameFileQfOutFlag)
      
      rptWrte <-
        base::try(NEONprocIS.base::def.wrte.parq(
          data = qfCust,
          NameFile = nameFileQfOutFlag,
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

