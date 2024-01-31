##############################################################################################
#' @title Wrapper for Troll Log File Comparison and Gap Filling

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}
#' 
#' @description Wrapper function. Compares logged data to streamed data and fills gaps.
#'
#'
#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/sensor/yyyy/mm/dd/source-id. The source-id is the unique identifier of the sensor. \cr#'
#' 
#' @param DirOut Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' 
#' @param SchmDataOut (optional), where values is the full path to the avro schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. Note that you will need to distinguish between the aquatroll200 (outputs conductivity) and the 
#' leveltroll500 (does not output conductivity) in your schema.
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#' 
#' @return Combined logged and streamed L0 data in daily parquets.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' 
#' @keywords Currently none
#' 
#' @examples
#' # Not run
DirIn<-'/home/NEON/ncatolico/pfs/logjam_clean_troll_files/leveltroll500/2022/03/10/21115' #cleaned log data
DirInStream<-'/home/NEON/ncatolico/pfs/leveltroll500_data_source_trino/leveltroll500/2022/03/10/21115' #streamed L0 data
log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' wrap.troll.logfiles <- function(DirIn=DirIn,
#'                               DirOut="~/pfs/out",
#'                               SchmDataOut=NULL,
#'                               log=log)
#'                               
#' @changelog
#   Nora Catolico (2024-01-30) original creation
#' 
##############################################################################################
wrap.troll.logfiles <- function(DirIn,
                             DirOutBase,
                             SchmDataOut=NULL,
                             SchmFlagsOut=NULL,
                             log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Gather info about the input directory (including date), and create base output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  dirInData <- fs::path(DirIn,'data')
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  DirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
  DirOutData <- base::paste0(DirOut,'/data')
  base::dir.create(DirOutData,recursive=TRUE)
  DirOutFlags <- base::paste0(DirOut,'/flags')
  base::dir.create(DirOutFlags,recursive=TRUE)
  
  # Take stock of our data files. 
  fileData <- base::list.files(dirInData,full.names=FALSE)
  log$debug(base::paste0('Files identified:', DirIn, '/', fileData))
  
  # --------- Load the data ----------
  # Load in L0 data files in parquet format into data frame 'data' if exists
  # DirInStream<-'/home/NEON/ncatolico/pfs/leveltroll500_data_source_trino/leveltroll500/2022/03/10/21115' 
  # dirInData <- fs::path(DirInStream,'data')
  # fileData<-base::list.files(dirInData,full.names=FALSE)
  # L0File <- fileData[!grepl('_log',fileData)]
  if(length(L0File)>=1){
    L0Data  <-
      base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(dirInData, '/', L0File),
                                               log = log),
                silent = FALSE)
    if (base::any(base::class(data) == 'try-error')) {
      # Generate error and stop execution
      log$error(base::paste0('File ', dirInData, '/', L0File, ' is unreadable.'))
      base::stop()
    }
  }else{
    L0Data <- NULL
  }
  
  #load in log data if exists
  LogFile <- fileData[grepl('_log',fileData)]
  if(length(LogFile)>=1){
    LogData  <-
      base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(dirInData, '/', LogFile),
                                               log = log),
                silent = FALSE)
    if (base::any(base::class(data) == 'try-error')) {
      # Generate error and stop execution
      log$error(base::paste0('File ', dirInData, '/', LogFile, ' is unreadable.'))
      base::stop()
    }
  }else{
    LogData <- NULL
  }
  # note, already binned in flow.troll.logfiles script
  
  
  ## if only one file, keep that file
  ##time to merge 
  
  
  
  

}
















