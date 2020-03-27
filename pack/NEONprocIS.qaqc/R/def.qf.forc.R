##############################################################################################
#' @title Force a set of quality flags to a particular value based on the value of another flag

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Force a set of quality flags to a particular value based on the value of another flag

#' @param qf Data frame of named quality flags (values of -1,0,1)
#' @param nameQfDrve Character string. The name of a single "driver" flag in \code{qf}.
#' @param valuDrve Numeric. The value of the driver flag that will cause the forced flags in
#' \code{qfForc} to be set to \code{valuForc}
#' @param nameQfForc Character vector. The names of flags in \code{qf} that are to be forced to \code{valuForc} if \code{qfDrve} equals \code{valuDrve}
#' @param valuForc Numeric. The value that the flags in \code{qfForc} are to be set to when \code{qfDrve} equals \code{valuDrve}
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return The \code{qf} data frame, after forcing any applicable values.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' qf <- data.frame(QF1=c(1,-1,1,0,-1),QF2=c(-1,1,0,0,0),stringsAsFactors=FALSE)
#' def.qf.forc(qf=qf,qfDrve='QF1',valuDrve=1,qfForc='QF2',valuForc=0)

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-03-11)
#     original creation
##############################################################################################
def.qf.forc <-
  function(qf,
           nameQfDrve,
           valuDrve,
           nameQfForc,
           valuForc,
           log = NULL) {
    # initialize logging if necessary
    if (base::is.null(log)) {
      log <- NEONprocIS.base::def.log.init()
    }
    
    # Validate the data frame
    if (!NEONprocIS.base::def.validate.dataframe(dfIn = qf,
                                                 TestNumc = FALSE,
                                                 log = log)) {
      stop()
    }
    
    # Grab the names of the quality flags
    nameQfIn <- base::names(qf)
    
    # Error check - ensure that driver and forced flags are present in the data
    exstQfDrve <- nameQfDrve %in% nameQfIn
    exstQfForc <- nameQfForc %in% nameQfIn
    
    if (!exstQfDrve) {
      log$warn(
        base::paste0(
          'Driver flag: ',
          nameQfDrve,
          ' not found in the flags files. No intended flag forcing involving this flag will occur. Check input argument(s).'
        )
      )
      return(qf)
    }
    if (!base::all(exstQfForc)) {
      log$warn(
        base::paste0(
          'Flags: ',
          base::paste0(nameQfForc[!exstQfForc], collapse = ','),
          ' were not found in the flags. Intended flag forcing involving these flags will not occur. Check input argument(s).'
        )
      )
      nameQfForc <- nameQfForc[exstQfForc]
      
      if (base::length(nameQfForc) == 0) {
        return(qf)
      }
    }
    
    # Force the flags
    setForc <- qf[[nameQfDrve]] == valuDrve
    qf[setForc, nameQfForc] <- valuForc
    log$debug(
      base::paste0(
        base::sum(setForc),
        ' value(s) in flag(s): ',
        base::paste0(nameQfForc, collapse = ','),
        ' were set to ',
        valuForc,
        ' as a result of ',
        nameQfDrve,
        ' equaling ',
        valuDrve
      )
    )
    
    return(qf)
  }
