##############################################################################################
#' @title Compute "instantaneous" alpha, beta, and final quality flags

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Aggregate quality flags to produce alpha, beta, and final quality flags
#' for each L0' (instantaneous) record. The alpha flag is 1 when any of a set of selected
#' flags have a value of 1 (fail). The beta flag is 1 when any of a set of selected
#' flags cannot be evaluated (have a value of -1). If either the alpha flag or beta flag are raised,
#' the final quality is raised (value of 1).

#' @param qf Data frame of named quality flags (values of -1,0,1)
#' @param Para (optional) A named list of:\cr
#' \code{qfAlph} A character vector of the names of quality flags in \code{qf} that are to be used
#' to compute AlphaQF. If any of these flags have a value of 1 for a given record, AlphaQF will be
#' 1 for that record. May be NULL (or not in the list), in which case all flags found in \code{qf}
#' will be used to compute AlphaQF.\cr
#' \code{qfBeta} A character vector of the names of quality flags in \code{qf} that are to be used
#' to compute BetaQF. If any of these flags have a value of -1 for a given record, BetaQF will be
#' 1 for that record. May be NULL (or not in the list), in which case all flags found in \code{qf}
#' will be used to compute BetaQF. Note that this action may be modified by the \code{qfBetaIgnr}
#' list element below\cr
#' \code{qfBetaIgnr} A character vector of the names of quality flags in \code{qf} that, if any of
#' their values equals 1 for a particular record, the betaQF flag for that record is automatically
#' set to 0 (ignores the values of all other flags). May be NULL, (or not in the list), in which
#' case this argument will be ignored.
#' Note that the entire Para argument defaults to NULL, which will follow the default actions
#' described above.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame of the alpha, beta, and final quality flags

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' qf <- data.frame(QF1=c(1,-1,1,0,-1),QF2=c(-1,1,0,0,0),stringsAsFactors=FALSE)
#' Para <- list(qfAlph=c('QF1','QF2'),qfBeta=c('QF1','QF2'),qfBetaIgnr='QF2')
#' def.qm.dp0p(qf=qf,Para=Para)

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-03-11)
#     original creation
##############################################################################################
def.qm.dp0p <- function(qf, Para = NULL, log = NULL) {
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Validate the data frame
  if (!NEONprocIS.base::def.validate.dataframe(dfIn = qf,
                                               TestNumc = TRUE,
                                               log = log)) {
    stop()
  }
  
  # Grab the names of the quality flags
  nameQfIn <- base::names(qf)
  
  # Initialize the output
  qm <- qf[base::rep(nameQfIn[1], 3)]
  base::names(qm) <- c('AlphaQF', 'BetaQF', 'FinalQF')
  qm[, ] <- 0 # intialize alpha, beta, and final QF to 0 (good)
  
  #---- Compute the alpha flag
  if (base::is.null(Para$qfAlph)) {
    nameQf <- nameQfIn # All flags used to compute alpha QF
  } else {
    nameQf <-
      Para$qfAlph # Specific set of flags used to compute alpha QF
  }
  
  # Error check - ensure that the contributing flags are included in the data
  exstQf <- nameQf %in% nameQfIn
  
  if (!base::all(exstQf)) {
    log$warn(
      base::paste0(
        'The flag(s): ',
        base::paste0(nameQf[!exstQf], collapse = ','),
        ' intended to contribute to the alpha flag were not found in the quality flags, and will not contribute to flag computation. Check input argument(s).'
      )
    )
    nameQf <- nameQf[exstQf]
    
  }
  
  # Set the alpha flag to 1 when contributing flags = 1
  setFail <- base::rowSums(qf[nameQf] == 1, na.rm = TRUE) > 0
  qm[setFail, 'AlphaQF'] <- 1
  log$debug(base::paste0(
    base::sum(setFail),
    ' value(s) in the alpha quality flag were set to 1'
  ))
  
  
  
  #---- Compute the beta flag
  
  if (base::is.null(Para$qfBeta)) {
    nameQf <- nameQfIn # All flags used to compute beta QF
  } else {
    nameQf <-
      Para$qfBeta # Specific set of flags used to compute beta QF
  }
  
  # Error check - ensure that the contributing flags are included in the data
  exstQf <- nameQf %in% nameQfIn
  
  if (!base::all(exstQf)) {
    log$warn(
      base::paste0(
        'The flag(s): ',
        base::paste0(nameQf[!exstQf], collapse = ','),
        ' intended to contribute to the beta flag were not found in the quality flags, and will not contribute to flag computation. Check input argument(s).'
      )
    )
    nameQf <- nameQf[exstQf]
    
  }
  
  # Error check - ensure that the contributing flags are included in the data
  exstQf <- Para$qfBetaIgnr %in% nameQfIn
  
  if (!base::all(exstQf)) {
    log$warn(
      base::paste0(
        'The flag(s): ',
        base::paste0(Para$qfBetaIgnr[!exstQf], collapse = ','),
        ' indicated in qfBetaIgnr (intended to set the beta QF to 0 if their value is 1) were not found in the quality flags, and will not contribute to flag computation. Check input argument(s).'
      )
    )
    Para$qfBetaIgnr <- Para$qfBetaIgnr[exstQf]
    
  }
  # Set the beta flag to 1 when contributing flags = -1
  setNa <-
    base::rowSums(qf[nameQf] == -1, na.rm = TRUE) > 0 # At least one contributing flag = -1
  setIgnr <-
    base::rowSums(qf[Para$qfBetaIgnr] == 1, na.rm = TRUE) > 0 # One of the flags in qfBetaIgnr = 1
  setFail <- setNa & !setIgnr # resultant set to fail beta QF
  qm[setFail, 'BetaQF'] <- 1
  log$debug(base::paste0(
    base::sum(setFail),
    ' value(s) in the beta quality flag were set to 1'
  ))
  
  
  #---- Compute the final quality flag
  
  setFail <-
    qm$AlphaQF == 1 | qm$BetaQF == 1 # Either alpha or beta flag raised
  qm[setFail, 'FinalQF'] <- 1
  
  return(qm)
  
}
