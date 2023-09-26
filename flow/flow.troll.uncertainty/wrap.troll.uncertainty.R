##############################################################################################
#' @title Level Troll 500 and Aqua Troll 200 Science Computations

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org} \cr

#' @description Wrapper function. Calculate elevation and derive uncertainty for surface and groundwater troll data products.
#'

#' @param DirInData Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/source-id/#, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The source-id is the unique identifier of the sensor. \cr
#'
#' Nested within this path are the folders:
#'         /data
#' The data folder holds 1 data file with the naming format:
#' SOURCETYPE_CFGLOC_YYYY-MM-DD.parquet
#'        /location
#' The location folder holds 2 location json files with the naming formats:
#' SOURCETYPE_SOURCEID_locations.json
#' CFGLOC.json
#' 
#' @param DirInUcrt Character value. The input path to the uncertaintydata from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/source-id/#, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The source-id is the unique identifier of the sensor. \cr
#'
#' Nested within this path are the folders:
#'        /uncertainty_coef
#' The uncertainty_coef folder holds 1 json file with the naming formats:
#' SOURCETYPE_CFGLOC_YYYY-MM-DD_uncertaintyCoef.json
#'        /uncertainty_data
#' The uncertainty_data folder holds 1 data file with the naming formats:
#' SOURCETYPE_CFGLOC_YYYY-MM-DD_uncertaintyData.parquet#' 
#' 
#' 
#' @param DirOut Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' 
#' @param Context String. Required. The value must be designated as either "surfacewater" or "groundwater".
#' 
#' @param WndwAgr (optional) where value is the aggregation interval for which to compute uncertainty. 
#' Formatted as a 3 character sequence, typically representing the number of minutes over which to compute uncertainty 
#' For example, "WndwAgr=001" refers to a 1-minute aggregation interval, while "WndwAgr=030" refers to a 
#' 30-minute aggregation interval. Multiple aggregation intervals may be specified by delimiting with a pipe 
#' (e.g. "WndwAgr=001|030|060"). Note that a separate file will be output for each aggregation interval. 
#' It is assumed that the length of the file is one day. The aggregation interval must divide one day into 
#' complete intervals. No uncertainty data will be output if both "WndwAgr" and "WndwInst" are NULL.
#' 
#' @param WndwInst (optional) set to TRUE to include instantaneous uncertainty data output. The defualt value is FALSE. 
#' No uncertainty data will be output if both "WndwAgr" and "WndwInst" are NULL.
#' 
#' @param FileSchmData String. Optional. Full path to the avro schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. Note that you will need to distinguish between the aquatroll200 (outputs conductivity) and the 
#' leveltroll500 (does not output conductivity) in your schema.
#' 
#' @param FileSchmUcrt String. Optional. Full path to the avro schema for the output uncertainty data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. Note that you will need to distinguish between the aquatroll200 (outputs conductivity) and the 
#' leveltroll500 (does not output conductivity) in your schema.
#' 
#' @param FileSchmSciStats String. Optional. Full path to the avro schema for the output science statistics
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. 
#'
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the data folder(s) in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. carried through as-is). Note that the 'data' directory is automatically
#' populated in the output and cannot be included here.
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#'
#' @return A repository in DirOutBase containing the merged and filtered Kafka output, where DirOutBase replaces BASE_REPO 
#' of argument \code{DirIn} but otherwise retains the child directory structure of the input path. 
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' # NOT RUN
#' Sys.setenv(DIR_IN_data='/home/NEON/ncatolico/pfs/groundwaterPhysical_analyze_pad_and_qaqc_plau/2020/01/05') 
#' Sys.setenv(DIR_IN_ucrt='/home/NEON/ncatolico/pfs/groundwaterPhysical_group_path/2020/01/05') #uncertainty data
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' arg <- c("DirInData=$DIR_IN_data","DirInUcrt=$DIR_IN_ucrt","DirOut=~/pfs/out","Context=groundwater","WndwInst=TRUE","WndwAgr=030")
#' wrap.troll.uncertainty(DirInData,DirInUcrt,Context,WndwInst,WndwAgr)

#' @seealso Currently none

# changelog and author contributions / copyrights
#   Cove Sturtevant (2023-03-07)
#     Initial creation
##############################################################################################
wrap.troll.uncertainty <- function(DirInData,
                                   DirInUcrt,
                                   DirOut,
                                   Context,
                                   WndwInst=FALSE,
                                   WndwAgr=NULL,
                                   FileSchmData=NULL,
                                   FileSchmSciStats=NULL,
                                   FileSchmUcrt=NULL,
                                   DirSubCopy=NULL,
                                   log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Gather info about the input directory and create the output directory.
  DirInData <- NEONprocIS.base::def.dir.splt.pach.time(DirInData,log=log)
  dirInData <- fs::path(DirInData,'data')
  InfoDirInUcrt <- NEONprocIS.base::def.dir.splt.pach.time(DirInUcrt,log=log)
  dirInUcrt <- fs::path(DirInUcrt,'ucrt')
  dirOut <- fs::path(DirOutBase,DirInData$dirRepo)
  dirOutData <- fs::path(dirOut,'data')
  NEONprocIS.base::def.dir.crea(DirBgn = dirOut,
                                DirSub = 'data',
                                log = log)
  
  # Copy with a symbolic link the desired subfolders 
  DirSubCopy <- base::unique(base::setdiff(DirSubCopy,'data'))
  if(base::length(DirSubCopy) > 0){

    NEONprocIS.base::def.dir.copy.symb(DirSrc=fs::path(DirInData,DirSubCopy),
                                       DirDest=dirOut,
                                       LnkSubObj=FALSE,
                                       log=log)
  }    
  
  # Get the fields in the L0 schema. We will include only these fields in the output
  varSchmL0 <- NEONprocIS.base::def.schm.avro.pars(FileSchm=FileSchmData,
                                                  Schm=SchmL0,
                                                  log=log
  )$var

  # Take stock of our data files. 
  fileData <- base::list.files(dirInData,full.names=FALSE)
 
  # Output filename is the input filename
  nameFileSplt <- base::strsplit(fileData[1],'_')[[1]]
  nameFileOut <- base::paste0(
    base::paste0(
      utils::head(x=nameFileSplt,n=-2),
      collapse='_'),
    '.parquet')
  
  # Read, combine, filter, and sort the dataset 
  data <- NEONprocIS.base::def.read.parq.ds(fileIn=fs::path(dirInData,fileData),
                                            Var=varSchmL0$name,
                                            VarTime='readout_time',
                                            Df=FALSE, # Retain as arrow_dplyr_query
                                            log=log)
  

  # Write out the combined dataset to file
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
      'Combined kafka output written to file ',
      fileOut
      ))
  }

  return()
} 
