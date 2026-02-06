##############################################################################################
#' @title Wrapper for insufficient data calculations

#' @author
#' Bobby Hensley \email{hensley@battelleecology.org}
#' 
#' @description Wrapper function. Determines the number of available measurements within an
#' averaging period, and whether an insufficient data quality flag should be applied.
#' This insufficient data quality flag is then used to determine whether the final quality
#' flag should be applied.  It assumes that measurements that have failed individual
#' plausibility and sensor-specific tests have been removed and the number of remaining 
#' measurements available for averaging is the only factor determining the final data quality.    
#'
#' @param DirIn Character value. The base file path to the averaged stats and quality metrics.
#' 
#' @param numPoints Character value. The name(s) of the field(s) in the input data frame containing 
#' the number of points. 
#' 
#' @param minPoints Character value. For each set of numPoints, the corresponding minimum number of 
#' points required to not trigger the insufficient data quality flag.
#' 
#' @param insuffQFnames Character value. For each set of numPoints, the names of the corresponding 
#' insufficient data QF's in the output data frame that should be triggered if less than minPoints.
#' 
#' @param finalQFnames Character value. For each set of numPoints, the names of the corresponding 
#' final data QF's in the output data frame that should be triggered if the insufficient data QF 
#' is triggered.
#'  
#' @param DirOut Character value. The base file path for the output data. 
#' 
#' @param SchmStats (optional), A json-formatted character string containing the schema for the output averaged stats parquet.
#' Should be the same as the input. 
#' 
#' @param SchmQMs (optional), A json-formatted character string containing the schema for the output quality metrics parquet 
#' with insufficient data quality flag added. 
#' 
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. not combined but carried through as-is).
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#' 
#' @return Averaged stats file and quality metric file in daily parquets.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' 
#' @keywords Currently none
#' 
#' @examples
#' # Not run
# DirIn<-"~/pfs/nitrate_null_gap_ucrt/2025/06/24/nitrate_CRAM103100/sunav2/CFGLOC110733"
# numPoints=c("nitrateNumPts")
# minPoints=c(5)
# insuffQFnames=c("nitrateInsufficientDataQF")
# finalQFnames=c("nitrateFinalQF")
# DirOut<-"~/pfs/nitrate_null_gap_ucrt_updated/2025/06/24/nitrate_CRAM103100/sunav2/CFGLOC110733" 
# SchmStats<-base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2_stats.avsc'),collapse='')
# SchmQMs<-base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2_quality_metrics.avsc'),collapse='')
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#'
#'                                                                                                                                                                                          
#' @changelog
#' Bobby Hensley (2025-10-31)
#' Initial creation.
#' 
#' Bobby Hensley (2025-12-18)
#' Updated so that finalQF is solely determined by insufficientDataQF.
#' 
#' Bobby Hensley (2026-02-05)
#' Updated to test multiple variables. 
##############################################################################################
wrap.qf.insuff.data <- function(DirIn,
                                insuffParam,
                                DirOutBase,
                                SchmStats=NULL,
                                SchmQMs=NULL,
                                DirSubCopy=NULL,
                                log=NULL
){
  
  #' Start logging if not already.
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  DirInStats <- paste0(DirIn,"/stats")
  DirInQMs <- paste0(DirIn,"/quality_metrics")
  DirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
  DirOutStats <- base::paste0(DirOut,"/stats")
  base::dir.create(DirOutStats,recursive=TRUE)
  DirOutQMs <- base::paste0(DirOut,"/quality_metrics")
  base::dir.create(DirOutQMs,recursive=TRUE)
  
  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirSubCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(DirSrc=base::paste0(DirIn,'/',DirSubCopy),
                                       DirDest=DirOut,
                                       LnkSubObj=TRUE,
                                       log=log)
  }
  
  #' Read in parquet file of averaged stats.
  statsFileName<-base::list.files(DirInStats,full.names=FALSE)
  if(length(statsFileName)==0){
    log$error(base::paste0('Stats file not found in ', DirInStats)) 
    stop()
  } else {
    statsData<-base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInStats, '/', statsFileName),
                                                       log = log),silent = FALSE)
    log$debug(base::paste0('Successfully read in file: ',statsFileName))
  }
  
  #' Read in parquet file of quality metrics.
  qmFileName<-base::list.files(DirInQMs,full.names=FALSE)
  if(length(qmFileName)==0){
    log$error(base::paste0('Quality metrics not found in ', DirInQMs)) 
    stop()
  } else {
    qmData<-base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInQMs, '/', qmFileName),
                                                         log = log),silent = FALSE)
    log$debug(base::paste0('Successfully read in file: ',qmFileName))
  }
  

  #' Create data frame of variables, min points and flags
     testParams<-data.frame(numPoints,minPoints,insuffQFnames,finalQFnames) 
  
  #' Starts loop that performs test for each variable specified in the testParams table
    for(i in 1:nrow(testParams)){
      numPoints<-testParams[i,1]
      minPoints<-testParams[i,2]
      insuffQFnames<-testParams[i,3]
      finalQFnames<-testParams[i,4]
   
      #' If the number of points is NA, set it to 0.
      statsData[is.na(statsData[[numPoints]]), numPoints] <- 0 
    
      #' If the number of points is greater than or equal to the minimum required, 
      #' revert the insufficient data quality flag (default is to apply it).
      qmData[[insuffQFnames]]<-1
      qmData[statsData[[numPoints]] >= minPoints, insuffQFnames] <- 0 
    
      #' If insufficient data QF is applied, apply final QF.
      qmData[[finalQFnames]] <- ifelse(qmData[[insuffQFnames]] == 1, 1, 0) 
   } # Ends test loop
  
  #' Write out stats file.  
  rptOutStats <- try(NEONprocIS.base::def.wrte.parq(data = statsData,
                                                    NameFile = base::paste0(DirOutStats,'/',statsFileName),
                                                    Schm = SchmStats),silent=TRUE)
  if(class(rptOutStats)[1] == 'try-error'){
    log$error(base::paste0('Cannot write updated stats to ',base::paste0(DirOutStats,'/',statsFileName),'. ',attr(rptOutStats, "condition")))
    stop()
  } else {
    log$info(base::paste0('Updated stats written successfully in ', base::paste0(DirOutStats,'/',statsFileName)))
  }
  
  #' Write out QMs file.  
  rptOutQMs <- try(NEONprocIS.base::def.wrte.parq(data = qmData,
                                                    NameFile = base::paste0(DirOutQMs,'/',qmFileName),
                                                    Schm = SchmQMs),silent=TRUE)
  if(class(rptOutQMs)[1] == 'try-error'){
    log$error(base::paste0('Cannot write updated QMs to ',base::paste0(DirOutQMs,'/',qmFileName),'. ',attr(rptOutQMs, "condition")))
    stop()
  } else {
    log$info(base::paste0('Updated QMs written successfully in ', base::paste0(DirOutQMs,'/',qmFileName)))
  }
  
}



