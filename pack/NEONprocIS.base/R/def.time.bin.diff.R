##############################################################################################
#' @title Create sequence of time bins as a difftime object

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Create sequence of time bins as a difftime object offset from zero. This allows the 
#' time differences to be added to an arbitrary start time. 

#' @param WndwBin WndwAgr A difftime object indicating the bin size (e.g. base::as.difftime(30,units="mins")) 
#' @param WndwTime A difftime object indicating the total length of time to bin (e.g. base::as.difftime(1,units='days'))
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log 
#' output in addition to standard R error messaging. Defaults to NULL, in which no logger other than 
#' standard R error messaging will be used.
#' 
#' @return A list of:
#' timeBgnDiff A difftime object providing the start times for each bin as a time offset from zero
#' timeEndDiff A difftime object providing the end times for each bin as a time offset from zero

#' @references 
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' WndwBin <- base::as.difftime(30,units="mins")
#' WndwTime <- base::as.difftime(1,units='days')
#' 

#' @seealso \code{\link[NEONprocIS.base]{def.log.init}}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-12-19)
#     original creation
##############################################################################################
def.time.bin.diff <- function(WndwBin,
                         WndwTime,
                         log=NULL
){
  
  timeDmmyBgn <- base::as.POSIXlt('1970-01-01',tz='GMT')
  timeDmmyEnd <- timeDmmyBgn + WndwTime 
  timeBgnDiff <- list()
  timeEndDiff <- list()

    # Time series of binning windows 
    timeBgnSeq <- base::as.POSIXlt(base::seq.POSIXt(from=timeDmmyBgn,to=timeDmmyEnd,by=base::format(WndwBin)))
    timeBgnSeq <- utils::head(timeBgnSeq,n=-1) # Lop off the last one at timeEnd 
    timeEndSeq <- timeBgnSeq + WndwBin
    if(utils::tail(timeEndSeq,n=1) != timeDmmyEnd){
      msg <- (base::paste0('WndwBin must be an even divisor of WndwTime. A WndwBin of ',
                             WndwBin,' does not fit this requirement. Check inputs.'))
      if(!base::is.null(log)){
        log$fatal(msg)
      } 
      stop(msg)

    }
    
    timeBgnDiff <- timeBgnSeq - timeDmmyBgn # Add to timeBgn of each day to represent the starting time sequence
    timeEndDiff <- timeEndSeq - timeDmmyBgn # Add to timeBgn of each day to represent the end time sequence

  return(base::list(timeBgnDiff=timeBgnDiff,timeEndDiff=timeEndDiff))
}
