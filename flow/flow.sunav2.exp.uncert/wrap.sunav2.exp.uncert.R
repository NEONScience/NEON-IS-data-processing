##############################################################################################
#' @title Wrapper for SUNA expanded uncertainty calculation

#' @author
#' Bobby Hensley \email{hensley@battelleecology.org}
#' 
#' @description Wrapper function. Calculates the expanded uncertainty for each SUNA burst.
#'
#' @param DirIn Character value. The base file path to the averaged stats and uncertainty coefficients.
#' 
#' @param DirOut Character value. The base file path for the output data. 
#' 
#' @param SchmStats (optional), A json-formatted character string containing the schema for the output averaged stats parquet.
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
# DirOut<-"~/pfs/nitrate_null_gap_ucrt_updated/2025/06/24/nitrate_CRAM103100/sunav2/CFGLOC110733" 
# SchmStats<-base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2_stats.avsc'),collapse='')
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#'
#'                                                                                                                                                                                          
#' @changelog
#' Bobby Hensley (2025-11-03)
#' Initial creation.
#' 
##############################################################################################
wrap.sunav2.exp.uncert <- function(DirIn,
                                      DirOut,
                                      SchmStats=NULL,
                                      log=NULL
){
  
  #' Start logging if not already.
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  DirInStats <- paste0(DirIn,"/stats")
  DirInCoeff <- paste0(DirIn,"/uncertainty_coef")
  DirOutStats <- base::paste0(DirOut,"/stats")
  base::dir.create(DirOutStats,recursive=TRUE)
  
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
  
  #' Read in json file of uncertainty coefficients.
  coeffileName<-base::list.files(DirInCoeff,full.names=FALSE)
  if(length(coeffileName)==0){
    log$error(base::paste0('Quality metrics not found in ', DirInCoeff)) 
    stop()
  } else {
    uncertCoeff<-base::try(NEONprocIS.cal::def.read.ucrt.coef.fdas(NameFile = base::paste0(DirInCoeff, '/', coeffileName)),
                              silent = FALSE)
    log$debug(base::paste0('Successfully read in file: ',coeffileName))
  }
  

  

  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  #' Write out updated stats file.  
  rptOutStats <- try(NEONprocIS.base::def.wrte.parq(data = statsData,
                                                    NameFile = base::paste0(DirOutStats,'/',statsFileName),
                                                    Schm = NULL),silent=TRUE)
  if(class(rptOutStats)[1] == 'try-error'){
    log$error(base::paste0('Cannot write updated stats to ',base::paste0(DirOutStats,'/',statsFileName,".parquet"),'. ',attr(rptOutStats, "condition")))
    stop()
  } else {
    log$info(base::paste0('Updated stats written successfully in ', base::paste0(DirOutStats,'/',statsFileName,".parquet")))
  }
  
  
}



