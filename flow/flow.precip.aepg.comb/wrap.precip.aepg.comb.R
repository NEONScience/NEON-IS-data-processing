##############################################################################################
#' @title Compute average precipitation for the same day computed from different central days 

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr

#' @description Wrapper function. Compute average precipitation computed for the  
#' Belfort AEPG600m sensor that was generated from different central days of the smoothing algorithm
#' 

#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/location-id, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The location-id is the unique identifier of the location. \cr
#'
#' Nested within this path is the folder:
#'         /data
#'         /threshold
#' The data folder holds any number of data files from kafka with the naming format:
#' SOURCETYPE_LOCATIONID_YYYY-MM-DD.parquet
#' 
#' For example:
#' Input path = /scratch/pfs/li191r_data_source_kafka/li191r/2023/03/01/11346/data/ with nested file:
#'    li191r_11346_2023-03-05_13275082_13534222.parquet
#'    li191r_11346_2023-03-05_13534225_13534273.parquet
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
#   Teresa Burlingame & Cove Sturtevant (2024-06-25)
#     Initial creation
##############################################################################################
wrap.precip.aepg.comb <- function(DirIn,
                                    DirOutBase,
                                    SchmData=NULL,
                                    DirSubCopy=NULL,
                                    log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  

  # Gather info about the input directory and create the output directory.
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn,log=log)
  dirInStats <- fs::path(DirIn,'stats')
  dirInFlags <- fs::path(DirIn,'flags')
  dirOut <- fs::path(DirOutBase,InfoDirIn$dirRepo)
  dirOutStats <- fs::path(dirOut,'stats')
  dirOutFlags <- fs::path(dirOut,'flags')
  NEONprocIS.base::def.dir.crea(DirBgn = dirOut,
                                DirSub = c('stats','flags'),
                                log = log)
  
  # Copy with a symbolic link the desired subfolders 
  DirSubCopy <- base::unique(base::setdiff(DirSubCopy,c('stats','flags')))
  if(base::length(DirSubCopy) > 0){

    NEONprocIS.base::def.dir.copy.symb(DirSrc=fs::path(DirIn,DirSubCopy),
                                       DirDest=dirOut,
                                       LnkSubObj=FALSE,
                                       log=log)
  }    
  

  # Take stock of our stats files.
  # !! Try to make more generic, while excluding the manifest.txt file
  fileStats <- base::list.files(dirInStats,pattern='.parquet',full.names=FALSE)

  # Read all files
  stats <- base::lapply(fs::path(dirInStats,fileStats),NEONprocIS.base::def.read.parq,log=log)
    
  # Using the 1st file as a starting point, average the precip values
  statsOut <- stats[[1]]
  statsOut$bench <- as.numeric(NA)
  statsOut$precipType <- as.character(NA)
  statsOut$precipBulk <- rowMeans(do.call(cbind,
                                     base::lapply(stats,FUN=function(statsIdx){statsIdx$precipBulk})
                                     ),
                              na.rm=TRUE
  )
  statsOut$precip <- statsOut$precipBulk > 0
  statsOut$precip[is.na(statsOut$precip)] <- FALSE
  
  # Lop off the _from_<date> in the filename 
  nameFileOut <- sub(pattern='_from_[0-9]{4}-[0-9]{2}-[0-9]{2}',
                     replacement='',
                     x=fileStats[1])
  
  # Write out the stats to file
  fileOut <- fs::path(dirOutStats,nameFileOut)
      
  rptWrte <-
    base::try(NEONprocIS.base::def.wrte.parq(
      data = statsOut,
      NameFile = fileOut,
      NameFileSchm=NULL,
      Schm=NULL,
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
      'Wrote averaged precipitation to file ',
      fileOut
    ))
  }

  # Take stock of our flags files.
  # !! Try to make more generic, while excluding the manifest.txt file
  fileQf <- base::list.files(dirInFlags,pattern='.parquet',full.names=FALSE)
  
  # Read all files
  qf <- base::lapply(fs::path(dirInFlags,fileQf),NEONprocIS.base::def.read.parq,log=log)
  
  # Using the 1st file as a starting point, average the precip values
  for(idxFile in seq_len(length(qf))){
    if(idxFile == 1){
      qfOut <- qf[[idxFile]]
    } else {
      # Take the max flag value
      for (idxCol in 3:ncol(qfOut)){
        setChg <- (qf[[idxFile]][,idxCol]-qfOut[,idxCol]) > 0
        qfOut[setChg,idxCol] <- qf[[idxFile]][setChg,idxCol]
      }
    }
  }

  # Lop off the _from_<date> in the filename 
  nameFileQfOut <- sub(pattern='_from_[0-9]{4}-[0-9]{2}-[0-9]{2}',
                     replacement='',
                     x=fileQf[1])
  
  # Write out the stats to file
  fileQfOut <- fs::path(dirOutFlags,nameFileQfOut)
  
  rptWrte <-
    base::try(NEONprocIS.base::def.wrte.parq(
      data = qfOut,
      NameFile = fileQfOut,
      NameFileSchm=NULL,
      Schm=NULL,
      log=log
    ),
    silent = TRUE)
  if ('try-error' %in% base::class(rptWrte)) {
    log$error(base::paste0(
      'Cannot write output to ',
      fileQfOut,
      '. ',
      attr(rptWrte, "condition")
    ))
    stop()
  } else {
    log$info(base::paste0(
      'Wrote precipitation flags to file ',
      fileQfOut
    ))
  }
  
  return()
} 
