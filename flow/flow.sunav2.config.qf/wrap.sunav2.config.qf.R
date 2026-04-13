##############################################################################################
#' @title Wrapper for SUNA configuration quality flag

#' @author
#' Bobby Hensley \email{hensley@battelleecology.org}
#' 
#' @description Wrapper function. Adds a quality flag to SUNA data when more than the
#' expected number of points are present, indicating that the sensor was configured in
#' continuous rather than periodic mode). The main purpose is to flag field cleaning and 
#' calibration measurements taken in DI.  This flag will also trigger the final QF.    
#'
#' @param DirIn Character value. The base file path to the averaged stats and quality metrics.
#' 
#' @param DirOut Character value. The base file path for the output data. 
#' 
#' @param SchmStats (optional), A json-formatted character string containing the schema for the output averaged stats parquet.
#' Should be the same as the input. 
#' 
#' @param SchmQMs (optional), A json-formatted character string containing the schema for the output quality metrics parquet 
#' with continuous mode data quality flag added. 
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
# DirInBase<-"~/pfs/nitrate_null_gap_ucrt/2025/06/24/nitrate_HOPB112100/sunav2/CFGLOC113620"
# DirOutBase<-"~/pfs/out" 
# SchmStats<-"~/pfs/nitrate_avro_schemas/sunav2_stats.avsc"
# SchmQMs<-"~/pfs/nitrate_avro_schemas/sunav2_config.avsc"
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#'
#'                                                                                                                                                                                          
#' @changelog
#' Bobby Hensley (2026-04-13)
#' Initial creation.
#' 
##############################################################################################
wrap.suna.config.qf <- function(DirInBase,
                                DirOutBase,
                                SchmStats=NULL,
                                SchmQMs=NULL,
                                log=NULL
){
  
  # Start logging if not already.
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Input and output sub-directories
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirInBase)
  DirInStats <- paste0(DirInBase,"/stats")
  DirInQMs <- paste0(DirInBase,"/quality_metrics")
  DirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
  DirOutStats <- base::paste0(DirOut,"/stats")
  base::dir.create(DirOutStats,recursive=TRUE)
  DirOutQMs <- base::paste0(DirOut,"/quality_metrics")
  base::dir.create(DirOutQMs,recursive=TRUE)
  
  # Read in parquet file of SUNA stats.
  statsFileName<-base::list.files(DirInStats,full.names=FALSE)
  if(length(statsFileName)==0){
    log$error(base::paste0('Data file not found in ', DirInData)) 
    stop()
  } else {
    sunaStats<-base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInStats, '/', statsFileName),
                                                       log = log),silent = FALSE)
    log$debug(base::paste0('Successfully read in file: ',statsFileName))
  }
  
  # Read in parquet file of SUNA QM's.
  qmFileName<-base::list.files(DirInQMs,full.names=FALSE)
  if(length(qmFileName)==0){
    log$error(base::paste0('Plausibility flags not found in ', DirInQMs)) 
    stop()
  } else {
    sunaQMs<-base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInQMs, '/', qmFileName),
                                                         log = log),silent = FALSE)
    log$debug(base::paste0('Successfully read in file: ',qmFileName))
  }
  
  # Sets nitrateConfigQF=1 in QM file if numPoints > maxPts in Data file 
  maxPts= 41  #Older SUNA data used this configuration (50 light measurements - 9 warmup)
  sunaQMs$nitrateConfigQF=-1
  for(i in 1:nrow(sunaStats)){
    if(sunaStats[i,which(colnames(sunaStats)=="nitrateNumPts")]>0)
      (sunaQMs[i,which(colnames(sunaQMs)=="nitrateConfigQF")]=0)
    if(sunaStats[i,which(colnames(sunaStats)=="nitrateNumPts")]>maxPts)
      (sunaQMs[i,which(colnames(sunaQMs)=="nitrateConfigQF")]=1)
  }

  # If nitrateConfigQF=1 set nitrateFinalQF=1
  for(i in 1:nrow(sunaQMs)){
    if(sunaQMs[i,which(colnames(sunaQMs)=="nitrateConfigQF")]==1)
      (sunaQMs[i,which(colnames(sunaQMs)=="nitrateFinalQF")]=1)
  }
  
  # Arranges columns to match schema
  sunaQMs <- sunaQMs[, c(1:38, 39, 40)]
   
  #' Write out stats file.  
  rptOutStats <- try(NEONprocIS.base::def.wrte.parq(data = sunaStats,
                    NameFile = base::paste0(DirOutStats,'/',statsFileName),
                    Schm = NULL),silent=TRUE)
    if(class(rptOutStats)[1] == 'try-error'){
      log$error(base::paste0('Cannot write updated stats to ',base::paste0(DirOutStats,'/',statsFileName),'. ',attr(rptOutStats, "condition")))
      stop()
    } else {
      log$info(base::paste0('Updated stats written successfully in ', base::paste0(DirOutStats,'/',statsFileName)))
    }
    
  #' Write out QMs file.  
  rptOutQMs <- try(NEONprocIS.base::def.wrte.parq(data = sunaQMs,
                  NameFile = base::paste0(DirOutQMs,'/',qmFileName),
                  Schm = NULL),silent=TRUE)
    if(class(rptOutQMs)[1] == 'try-error'){
      log$error(base::paste0('Cannot write updated QMs to ',base::paste0(DirOutQMs,'/',qmFileName),'. ',attr(rptOutQMs, "condition")))
      stop()
    } else {
      log$info(base::paste0('Updated QMs written successfully in ', base::paste0(DirOutQMs,'/',qmFileName)))
    }
    
  
}



