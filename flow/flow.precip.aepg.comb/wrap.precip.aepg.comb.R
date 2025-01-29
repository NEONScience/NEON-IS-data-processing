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
#'         /stats
#'         /flags
#' The stats and flags folders hold the output from flow.precip.aepg.smooth.R
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
#' @return A repository in DirOutBase containing the the average computed precipitation for each data day, 
#' where DirOutBase replaces BASE_REPO of argument \code{DirIn} but otherwise retains the child directory 
#' structure of the input path. Only the stats terminal directory is output. (Flags do not need averaging
#' across computation days.)
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' # NOT RUN
#' DirIn <- '/scratch/pfs/precipWeighing_ts_pad_smoother/2022/07/28'
#' DirOutBase <- '/scratch/pfs/out'
#' wrap.precip.aepg.comb(DirIn,DirOutBase)

#' @seealso Currently none

# changelog and author contributions / copyrights
#   Teresa Burlingame & Cove Sturtevant (2024-06-25)
#     Initial creation
##############################################################################################
wrap.precip.aepg.comb <- function(DirIn,
                                    DirOutBase,
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
  dirOut <- fs::path(DirOutBase,InfoDirIn$dirRepo)
  dirOutStats <- fs::path(dirOut,'stats')
  NEONprocIS.base::def.dir.crea(DirBgn = dirOut,
                                DirSub = c('stats'),
                                log = log)
  
  # Copy with a symbolic link the desired subfolders 
  DirSubCopy <- base::unique(base::setdiff(DirSubCopy,c('stats')))
  if(base::length(DirSubCopy) > 0){

    NEONprocIS.base::def.dir.copy.symb(DirSrc=fs::path(DirIn,DirSubCopy),
                                       DirDest=dirOut,
                                       LnkSubObj=FALSE,
                                       log=log)
  }    
  

  # Loop through each aggregation interval
  for (tmiIdx in c('060','01D')){
    # Take stock of our files.
    fileStats <- base::list.files(dirInStats,pattern=paste0('stats_',tmiIdx),full.names=FALSE) 

    # Read stats files
    stats <- base::lapply(fs::path(dirInStats,fileStats),NEONprocIS.base::def.read.parq,log=log)
    
    # Using the 1st file as a starting point, average the precip values and take max flag values
    statsOut <- stats[[1]]
    statsOut$precipBulk <- rowMeans(
      do.call(cbind,
              base::lapply(stats,FUN=function(statsIdx){statsIdx$precipBulk})
              ),na.rm=TRUE
    )
    statsOut$precipBulkExpUncert <- rowMeans(
      do.call(cbind,
              base::lapply(stats,FUN=function(statsIdx){statsIdx$precipBulkExpUncert})
      ),na.rm=TRUE
    )
    statsOut$insuffDataQF <- matrixStats::rowMaxs(
      do.call(cbind,
              base::lapply(stats,FUN=function(statsIdx){statsIdx$insuffDataQF})
              ), na.rm=TRUE
    )
    statsOut$dielNoiseQF <- matrixStats::rowMaxs(
      do.call(cbind,
              base::lapply(stats,FUN=function(statsIdx){statsIdx$dielNoiseQF})
      ), na.rm=TRUE
    )
    statsOut$evapDetectedQF <- matrixStats::rowMaxs(
      do.call(cbind,
              base::lapply(stats,FUN=function(statsIdx){statsIdx$evapDetectedQF})
      ), na.rm=TRUE
    )
    statsOut$finalQF <- matrixStats::rowMaxs(
      do.call(cbind,
              base::lapply(stats,FUN=function(statsIdx){statsIdx$finalQF})
      ), na.rm=TRUE
    )
    
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
    
  } # End loop over aggregation interval

  return()
} 
