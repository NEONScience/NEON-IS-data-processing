##############################################################################################
#' @title Wrapper for SUNA configuration quality flag

#' @author
#' Bobby Hensley \email{hensley@battelleecology.org}
#' 
#' @description Wrapper function. Adds a quality flag to SUNA data when more than the
#' expected number of points are present, indicating that the sensor was configured in
#' continuous rather than periodic mode. The main purpose is to flag field cleaning and 
#' calibration measurements taken in DI.  This flag will also trigger the final QF.    
#'
#' @param DirIn Character value. The base file path to the averaged stats and quality metrics.
#' 
#' @param DirOutBase Character value. The base file path for the output data. 
#' 
#' @param SchmStats (optional), A json-formatted character string containing the schema for the output averaged stats parquet.
#' Should be the same as the input. 
#' 
#' @param SchmQMs (optional), A json-formatted character string containing the schema for the output quality metrics parquet 
#' with continuous mode data quality flag added. 
#' 
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. not combined but carried through as-is).
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#' 
#' @return Averaged stats file and quality metric file with configQF added in daily parquets.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' 
#' @keywords Currently none
#' 
#' @examples
#' # Not run
# DirIn<-"~/pfs/nitrate_null_gap_ucrt/2025/06/24/nitrate-surfacewater_HOPB112100/sunav2/CFGLOC113620"
# DirOutBase<-"~/pfs/out" 
# SchmStats<-"~/pfs/nitrate_avro_schemas/sunav2_stats.avsc"
# SchmQMs<-"~/pfs/nitrate_avro_schemas/sunav2_config.avsc"
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#'
#'                                                                                                                                                                                          
#' @changelog
#' Bobby Hensley (2026-04-13)
#' Initial creation.
#' Nora Catolico (2026-04-28)
#' add in maxPts threshold option
##############################################################################################
wrap.sunav2.config.qf <- function(DirIn,
                                DirOutBase,
                                SchmStats=NULL,
                                SchmQMs=NULL,
                                DirSubCopy=NULL,
                                log=NULL
){
  
  # Start logging if not already.
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Input and output sub-directories
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  DirInStats <- paste0(DirIn,"/stats")
  DirInQMs <- paste0(DirIn,"/quality_metrics")
  DirInThresholds <- paste0(DirIn,"/threshold")
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
  
  # Read in parquet file of SUNA stats.
  statsFileName<-base::list.files(DirInStats,full.names=FALSE)
  if(length(statsFileName)==0){
    log$error(base::paste0('Data file not found in ', DirInStats)) 
    stop()
  } else {
    sunaStats<-NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInStats, '/', statsFileName),
                                                       log = log)
    log$debug(base::paste0('Successfully read in file: ',statsFileName))
  }
  
  # Read in parquet file of SUNA QM's.
  qmFileName<-base::list.files(DirInQMs,full.names=FALSE)
  if(length(qmFileName)==0){
    log$error(base::paste0('Quality metrics (QM) file not found in ', DirInQMs)) 
    stop()
  } else {
    sunaQMs<-NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInQMs, '/', qmFileName),
                                                         log = log)
    log$debug(base::paste0('Successfully read in file: ',qmFileName))
  }
  
  #optional threshold
  if(!base::dir.exists(DirInThresholds)){
    maxPts <- 41  #Older SUNA data used this configuration (50 light measurements - 9 warmup)
    log$debug(base::paste0('Threshold directory not found: ',
                          DirInThresholds,
                          '. Using default maxPts value of 41.'))
  } else {
    thresholdFileName<-base::list.files(DirInThresholds,full.names=FALSE)
    if(length(thresholdFileName)==0){
      maxPts <- 41  #Older SUNA data used this configuration (50 light measurements - 9 warmup)
      log$warn(base::paste0('No threshold file found in ',
                            DirInThresholds,
                            '. Using default maxPts value of 41.'))
    } else {
      sunaThresholds<-base::try(NEONprocIS.qaqc::def.read.thsh.qaqc.df(NameFile = base::paste0(DirInThresholds, '/', thresholdFileName)),silent = FALSE)
      if(class(sunaThresholds)[1] == 'try-error'){
        maxPts <- 41
        log$warn(base::paste0('Failed to read threshold file: ',
                              thresholdFileName,
                              '. Using default maxPts value of 41.'))
      }else{
        maxPtsThreshold <- sunaThresholds[(sunaThresholds$threshold_name=="maxPts"),]
        if(nrow(maxPtsThreshold) == 1 &&
           "number_value" %in% base::names(maxPtsThreshold) &&
           base::is.numeric(maxPtsThreshold$number_value) &&
           length(maxPtsThreshold$number_value) == 1 &&
           !base::is.na(maxPtsThreshold$number_value)){
          maxPts <- maxPtsThreshold$number_value
          log$debug(base::paste0('Successfully read in file: ',thresholdFileName))
        } else {
          maxPts <- 41  #Older SUNA data used this configuration (50 light measurements - 9 warmup)
          log$warn(base::paste0('Invalid or ambiguous maxPts threshold in file: ',
                                thresholdFileName,
                                '. Using default value of 41.'))
        }
      }
    }
  }
  
  # Sets nitrateConfigQF=1 in QM file if numPoints > maxPts in Data file 
  sunaQMs$nitrateConfigQF <- -1
  pts <- sunaStats[["surfWaterNitrateNumPts"]]
  qf  <- sunaQMs[["nitrateConfigQF"]]
  qf[!base::is.na(pts) & pts > 0]      <- 0
  qf[!base::is.na(pts) & pts > maxPts] <- 1
  sunaQMs[["nitrateConfigQF"]] <- qf

  # If nitrateConfigQF=1 set nitrateFinalQF=1
  sunaQMs[sunaQMs[["nitrateConfigQF"]] == 1, "finalQF"] <- 1
   
  #' Write out stats file.  
  rptOutStats <- try(NEONprocIS.base::def.wrte.parq(data = sunaStats,
                    NameFile = base::paste0(DirOutStats,'/',statsFileName),
                    Schm = SchmStats),silent=TRUE)
    if(class(rptOutStats)[1] == 'try-error'){
      log$error(base::paste0('Cannot write updated stats to ',base::paste0(DirOutStats,'/',statsFileName),'. ',attr(rptOutStats, "condition")))
      stop()
    } else {
      log$info(base::paste0('Updated stats written successfully in ', base::paste0(DirOutStats,'/',statsFileName)))
    }
    
  #' Write out QMs file.  
  rptOutQMs <- try(NEONprocIS.base::def.wrte.parq(data = sunaQMs,
                  NameFile = base::paste0(DirOutQMs,'/',qmFileName),
                  Schm = SchmQMs),silent=TRUE)
    if(class(rptOutQMs)[1] == 'try-error'){
      log$error(base::paste0('Cannot write updated QMs to ',base::paste0(DirOutQMs,'/',qmFileName),'. ',attr(rptOutQMs, "condition")))
      stop()
    } else {
      log$info(base::paste0('Updated QMs written successfully in ', base::paste0(DirOutQMs,'/',qmFileName)))
    }
    
  
}



