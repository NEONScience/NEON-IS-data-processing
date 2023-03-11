##############################################################################################
#' @title Combine kafka output and strip unneeded data columns

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Wrapper function. Combined multiple L0 data files retrieved from kafka for a single
#' data day, strip unnecessary columns (so the data matches the L0 schema from Engineering), and 
#' remove the kafka offsets from the file name.
#' 
#' Data retrieved from Kafka may result in multiple files for a single data day depending on when the
#' data streamed to NEON HQ from the site. The file names include the start/stop kafka offsets to 
#' make them unique. Files retrieved from Kafka also include data that is not specified in the L0 
#' schema and thus potentially breaks workflows dependent on the L0 schema.
#' This module combines all kafka-retrieved files of kafka-related information so that the file 
#' name and format matches that of data generated from other sources (i.e. Trino).

#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/source-id, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The source-id is the unique identifier of the sensor. \cr
#'
#' Nested within this path is the folder:
#'         /data
#' The data folder holds any number of data files from kafka with the naming format:
#' SOURCETYPE_SOURCEID_YYYY-MM-DD_KAFKAOFFSETBEGIN_KAFKAOFFSETEND.parquet
#' 
#' For example:
#' Input path = /scratch/pfs/li191r_data_source_kafka/li191r/2023/03/01/11346/data/ with nested file:
#'    li191r_11346_2023-03-05_13275082_13534222.parquet
#'    li191r_11346_2023-03-05_13534225_13534273.parquet
#'
#' @param FileSchmL0 String. Optional. Full or relative path to L0 schema file. One of FileSchmL0 or SchmL0 must be 
#' provided.
#' @param SchmL0 String. Optional. Json formatted string of the AVRO L0 file schema. One of FileSchmL0 or SchmL0 must 
#' be provided. If both SchmL0 and FileSchmL0 are provided, SchmL0 will be ignored.
#' 
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#'
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the data folder(s) in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. carried through as-is). Note that the 'data' directory is automatically
#' populated in the output and cannot be included here.

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
#' DirIn <- '/scratch/pfs/li191r_data_source_kafka/li191r/2023/03/02/27733'
#' DirOutBase <- '/scratch/pfs/out'
#' FileSchmL0 <- '~/R/avro_schemas/schemas/li191r/li191r.avsc' # L0 schema
#' wrap.kfka.comb(DirIn,DirOutBase,FileSchmL0)

#' @seealso Currently none

# changelog and author contributions / copyrights
#   Cove Sturtevant (2023-03-07)
#     Initial creation
##############################################################################################
wrap.kfka.comb <- function(DirIn,
                           DirOutBase,
                           FileSchmL0=NULL,
                           SchmL0=NULL,
                           DirSubCopy=NULL,
                           log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Gather info about the input directory and create the output directory.
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn,log=log)
  dirInData <- fs::path(DirIn,'data')
  dirOut <- fs::path(DirOutBase,InfoDirIn$dirRepo)
  dirOutData <- fs::path(dirOut,'data')
  NEONprocIS.base::def.dir.crea(DirBgn = dirOut,
                                DirSub = 'data',
                                log = log)
  
  # Copy with a symbolic link the desired subfolders 
  DirSubCopy <- base::unique(base::setdiff(DirSubCopy,'data'))
  if(base::length(DirSubCopy) > 0){

    NEONprocIS.base::def.dir.copy.symb(DirSrc=fs::path(DirIn,DirSubCopy),
                                       DirDest=dirOut,
                                       LnkSubObj=FALSE,
                                       log=log)
  }    
  
  # Get the fields in the L0 schema. We will include only these fields in the output
  varSchmL0 <- NEONprocIS.base::def.schm.avro.pars(FileSchm=FileSchmL0,
                                                  Schm=SchmL0,
                                                  log=log
  )$var

  # Take stock of our data files. 
  fileData <- base::list.files(dirInData,full.names=FALSE)
 
  # Output filename is the input filename minus the kafka offsets
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
