##############################################################################################
#' @title  Assess sensor status flags for Pluvio 200L sensor on instant or aggregated levels

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr

#' @description Workflow. Compute the heater and status flags by assessing the bit rate. Only 
#' flagging alarm codes of interest.
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
#' @param SchmQF (Optional). A json-formatted character string containing the schema for the standard calibration and
#' plausibility QFs as well as the custom QFs heaterErrorQF sensorStatusQF

#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the data/flags/threshold folders in the input path that are to be copied with a 
#' symbolic link to the output path (i.e. carried through as-is). Note that the 'stats' and 'flags' directories 
#' are automatically populated in the output and cannot be included here.

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#'
#' @return A repository in DirOutBase containing the precipitation and uncertainty estimates along with 
#' quality flags (incl. the final quality flag) produced by the depth smoothing algorithm, where DirOutBase 
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
#' DirIn='/scratch/pfs/precipWeighingv2_analyze_pad_and_qaqc_plau/2025/03/31/'
#' DirOutBase='/scratch/pfs/out_tb'


#' wrap.precip.pluvio.flags(DirIn,DirOutBase,DirSubCopy)

#' @seealso Currently none

# changelog and author contributions / copyrights
#   Teresa Burlingame  (2025-04-15)
#     Initial creation
##############################################################################################
wrap.precip.pluvio.flags<- function(DirIn,
                                    DirOutBase,
                                    SchmQm=NULL,
                                    DirSubCopy=NULL,
                                    log=NULL
){
  
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
                                DirSub = c('data','flags'),
                                log = log)
  
  # Copy with a symbolic link the desired subfolders 
  DirSubCopy <- base::unique(base::setdiff(DirSubCopy,c('data','flags')))
  if(base::length(DirSubCopy) > 0){

    NEONprocIS.base::def.dir.copy.symb(DirSrc=fs::path(DirIn,DirSubCopy),
                                       DirDest=dirOut,
                                       LnkSubObj=FALSE,
                                       log=log)
  }    
  
  # Take stock of our data files.
  fileData <- base::list.files(dirInData,pattern='.parquet',full.names=FALSE)
  fileQfPlau <- base::list.files(dirInQf,pattern='Plausibility.parquet',full.names=FALSE)

  # Read the datasets 
  data <- NEONprocIS.base::def.read.parq.ds(fileIn=fs::path(dirInData,fileData),
                                            VarTime='readout_time',
                                            RmvDupl=TRUE,
                                            Df=TRUE, 
                                            log=log)
  
  qfPlau <- NEONprocIS.base::def.read.parq.ds(fileIn=fs::path(dirInQf,fileQfPlau),
                                                 VarTime='readout_time',
                                                 RmvDupl=TRUE,
                                                 Df=TRUE,
                                                 log=log)
  
  ## wipe preexisting schema TODO check with Cove
  # Remove the "schema" attribute
  #remove existing schema from plau so we can add more cols. 
  if (is.null(SchmQm)){
    base::attr(qfPlau, "schema") <- NULL
    }
  
  # if there are no heater streams add them in as NA
  if(!('heater_status' %in% names(data))){
    data$heater_status <- NA
    }

  #initialize fields 
  qfPlau$sensorErrorQF <- 0
  qfPlau$heaterErrorQF <- 0

  #bitwise calculation of flags of interest
  
  for (i in seq_along(data$sensorErrorQF)) {
    if (is.na(data$sensorStatus[i])) {
      qfPlau$sensorErrorQF[i] <- -1
    } else {
      if (data$sensorStatus[i] == 0) {
        qfPlau$sensorErrorQF[i] <- 0
      }
      
      if ((data$sensorStatus[i] / 2^6) %% 2 >= 1) { # unstable
        qfPlau$sensorErrorQF[i] <- 1
      }
      
      if ((data$sensorStatus[i] / 2^7) %% 2 >= 1) { # defective
        
        qfPlau$sensorErrorQF[i] <- 1
      }
      
      if ((data$sensorStatus[i] / 2^8) %% 2 >= 1) { # weight less minimum
        qfPlau$sensorErrorQF[i] <- 1
      }
      
      if ((data$sensorStatus[i] / 2^9) %% 2 >= 1) { # weight greater maximum
        qfPlau$sensorErrorQF[i] <- 1
      }
      
      if ((data$sensorStatus[i] / 2^10) %% 2 >= 1) { # no calibration
        qfPlau$sensorErrorQF[i] <- 1
      }
    }
  }
  
  #heater status for bit vals of interest
  for (i in seq_along(data$heater_status)) {
    if (is.na(data$heater_status[i])) {
      qfPlau$heaterErrorQF[i] <- -1
    } else {
      if (data$heater_status[i] == 0) {
        qfPlau$heaterErrorQF[i] <- 0
      }
      if ((data$heater_status[i] / 2^5) %% 2 >= 1) { #functional check failed
        qfPlau$heaterErrorQF[i] <- 1
      }
      if ((data$heater_status[i] / 2^7) %% 2 >= 1) { #heater deactivated or not present
        qfPlau$heaterErrorQF[i] <- 1
      }
    }
  }

  # "pass through" of data
  # TODO ask Cove if this is necessary? 
  # qfs added to list of flags to process through qm module. 
      
      #get file name based on date of data in directory
      nameFileOut <- fileData
      
      # Write out the time shifted dataset to file
      fileOut <- fs::path(dirOutData,nameFileOut)
      
      rptWrte <-
        base::try(NEONprocIS.base::def.wrte.parq(
          data = data,
          NameFile = fileOut,
          log=log
        ),
        silent = TRUE)
      
      if ('try-error' %in% base::class(rptWrte)) {
        log$error(base::paste0(
          'Cannot write output to ',
          fileOut,
          '. ',
          attr(rptWrte, "condition")
        ))
        stop()
      } else {
        log$info(base::paste0(
          'Data file written to file ',
          fileOut
        ))
      }
    
      nameFileQfOutFlag <- fileQfPlau

      nameFileQfOutFlag <- fs::path(dirOutQf,nameFileQfOutFlag)
      
      rptWrte <-
        base::try(NEONprocIS.base::def.wrte.parq(
          data = qfPlau,
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

