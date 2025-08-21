##############################################################################################
#' @title Wrapper for SUNA quality flagging

#' @author
#' Bobby Hensley \email{hensley@battelleecology.org}
#' 
#' @description Wrapper function. Uses thresholds to apply quality flags to SUNA data.
#'
#' @param DirIn Character value. The file path to the input data.
#'  
#' @param DirOut Character value. The file path for the output data. 
#' 
#' @param DirThresholds Character value. The file path for the quality flag thresholds. 
#' 
#' @param SchmDataOut (optional), A json-formatted character string containing the schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA.
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#' 
#' @return SUNA data with quality flags applied in daily parquets.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' 
#' @keywords Currently none
#' 
#' @examples
#' # Not run
# DirIn<-"~/pfs/sunav2_location_group_and_restructure/sunav2/2024/09/10/CFGLOC110733/data" 
# DirOut<-"~/pfs/sunav2_quality_flagged_data/sunav2/2024/09/10/CFGLOC110733" 
# DirThresholds<-"~/pfs/sunav2_thresholds" 
# SchmDataOut<-base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2_quality_flagged.avsc'),collapse='')
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#'                               
#' @changelog
#' Bobby Hensley (2025-08-30) created
#' 
##############################################################################################
wrap.sunav2.quality.flags <- function(DirIn=NULL,
                                      DirOut=NULL,
                                      SchmDataOut=NULL,
                                      log=NULL
){
  
  #' Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  #' Read in parquet file of input data
  fileName<-base::list.files(DirIn,full.names=FALSE)
  sunaData<-base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirIn, '/', fileName),
                                             log = log),silent = FALSE)
  
  #' Read in csv file of quality flag thresholds
  sunaThresholds<-read.csv(file=(base::paste0(DirThresholds,'/sunav2_thresholds.csv')))
  #############################################################################################
  #' Will be a different module that find the thresholds for the site and date range of the data
  #' This is just a temporary workaround
  siteThresholds<-sunaThresholds[(sunaThresholds$Named.Location.Name=="CRAM"),]
  #############################################################################################
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  #' Write out data file and log flags file  
  
  #write out data file
  fileOutSplt <- base::strsplit(DirInStream,'[/]')[[1]] # Separate underscore-delimited components of the file name
  asset<-tail(x=fileOutSplt,n=1)
  csv_name <-paste0('sunav2_',asset,'_',format(timeBgn,format = "%Y-%m-%d"))
  
  rptOut <- try(NEONprocIS.base::def.wrte.parq(data = dataOut,
                                               NameFile = base::paste0(DirOutData,'/',csv_name,".parquet"),
                                               Schm = SchmDataOut),silent=TRUE)
  if(class(rptOut)[1] == 'try-error'){
    log$error(base::paste0('Cannot write Data to ',base::paste0(DirOutData,'/',csv_name,".parquet"),'. ',attr(rptOut, "condition")))
    stop()
  } else {
    log$info(base::paste0('Data written successfully in ', base::paste0(DirOutData,'/',csv_name,".parquet")))
  }
  
  #write out log flags file
  csv_name_flags <-paste0('sunav2_',asset,'_',format(timeBgn,format = "%Y-%m-%d"),'_logFlags')
  
  rptOutFlags <- try(NEONprocIS.base::def.wrte.parq(data = flagsOut,
                                                    NameFile = base::paste0(DirOutFlags,'/',csv_name_flags,".parquet"),
                                                    Schm = SchmFlagsOut),silent=TRUE)
  if(class(rptOutFlags)[1] == 'try-error'){
    log$error(base::paste0('Cannot write Flags to ',base::paste0(DirOutFlags,'/',csv_name_flags,".parquet"),'. ',attr(rptOutFlags, "condition")))
    stop()
  } else {
    log$info(base::paste0('Flags written successfully in ', base::paste0(DirOutFlags,'/',csv_name_flags,".parquet")))
  }
  
}



