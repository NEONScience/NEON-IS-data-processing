###################################################################################################
#' @title Validate list not empty or invalid

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' Validate a list to check it is not a list or it is not empty.
#' Returns True if it is a list and is not empty. FALSE otherwise.

#' @param listIn Input list to be validated
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return Boolean. True if the input it is a list and is not empty. FALSE otherwise. \cr

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#'
#' @export

# changelog and author contributions / copyrights
#   Mija Choi (2020-02-24)
#     original creation
##############################################################################################

def.validate.list <-
  function(listIn,
           log = NULL) {
    # Initialize logging if necessary
    if (base::is.null(log)) {
      log <- NEONprocIS.base::def.log.init()
    }
    
    b <- TRUE
    
    if (!is.list(listIn)) {
      b <- FALSE
      log$error('Input is not a list.')
    }
    
    else if (rapportools::is.empty(unlist(listIn)))  {
      b <- FALSE
      log$error('List is empty.')
      
    }
    
    return (b)
  }
