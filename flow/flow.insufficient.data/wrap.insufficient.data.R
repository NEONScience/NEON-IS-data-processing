##############################################################################################
#' @title Wrapper for insufficient data calculations

#' @author
#' Bobby Hensley \email{hensley@battelleecology.org}
#' 
#' @description Wrapper function. Determines the number of available measurements within an
#' averaging period, and whether an insufficient data quality flag should be applied. 
#'
#' @param DirIn Character value. The base file path to the averaged stats and quality metrics.
#' 
#' @param minPoints Character value. The minimum number of points required to not trigger the insufficient data quality flag.
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
# minPoints=5
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
##############################################################################################
wrap.insufficient.data <- function(DirIn,
                                      minPoints,
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
  
  #' Identify the column name with the number of points and finalQF
  ptsColName<-grep("NumPts",names(statsData),value=TRUE)
  finalQfColName<-grep("FinalQF",names(qmData),value=TRUE)
  
  #' If the number of points is NA, set it to 0.
  for(i in 1:nrow(statsData)){
    if(is.na(statsData[i,which(colnames(statsData)==ptsColName)])){
      statsData[i,which(colnames(statsData)==ptsColName)]=0}}
  
  #' If the number of points is greater than or equal to the minimum required, 
  #' revert the insufficient data quality flag (default is to apply it).
  qmData$insufficientDataQF=1
  minPoints<-as.numeric(minPoints)
  for(i in 1:nrow(statsData)){
    if(statsData[i,which(colnames(statsData)==ptsColName)]>=minPoints){
      qmData[i,which(colnames(qmData)=='insufficientDataQF')]=0}}
  
  #' If the insufficient data quality flag has been applied, update the final quality flag.
  for(i in 1:nrow(qmData)){
    if(qmData[i,which(colnames(qmData)=='insufficientDataQF')]==1){
      qmData[i,which(colnames(qmData)==finalQfColName)]=1}}
  qmData <- qmData[c(setdiff(names(qmData), finalQfColName), finalQfColName)] #' Move finalQF back to the end
  
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



