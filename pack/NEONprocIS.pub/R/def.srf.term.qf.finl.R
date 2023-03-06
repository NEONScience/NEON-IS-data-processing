##############################################################################################
#' @title Determine term of final quality flag given the term of the science review flag

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. The science review flag has a specific naming convention based on the 
#' final quality flag to which it applies (see below). This function back-retrieves the term name of the
#' final quality flag given the tern name of the science review flag. Standalone science review
#' flags have no corresponding final quality flag. This scenario is also covered.
#' The naming convention for science review flags that modify a final quality flag is:
#'      <final quality flag term>SciRvw
#' The naming convention for standalone science review flags is sciRvwQf or <prefix>SciRvwQF.


#' @param termSrf Character vector. Term name(s) of the science review flag(s).
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A character vector with the term names of the corresponding final quality flags. Standalone
#' SRFs will return character(NA) for the resultant final quality flag.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' termSrf <- c("finalQFSciRvw", # modifies final quality flag
#'              "gWatSCondFinalQFSciRvw", # modifies final quality flag
#'              "sciRvwQF", # standalone SRF
#'              "tempSciRvwQF") # standalone SRF


#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2023-02-22)
#     original creation
##############################################################################################
def.srf.term.qf.finl <- function(termSrf,
                                 log = NULL
  ) {
    
    # Initialize log if not input
    if (is.null(log)) {
      log <- NEONprocIS.base::def.log.init()
    }

  # Initialize
  termQfFinl <- base::as.character(NA)
  numCharTermSrf <- base::nchar(termSrf)
  
  # Discern SRFs with a corresponding final QF
  setQfFinl <- base::substr(x=termSrf,
                            start=numCharTermSrf-5,
                            stop=numCharTermSrf) == 'SciRvw'
  
  # Draw out the term name of the final quality flag
  termQfFinl[setQfFinl] <- base::substr(x=termSrf[setQfFinl],
                                        start=1,
                                        stop=numCharTermSrf[setQfFinl]-6)
  return(termQfFinl)
}
