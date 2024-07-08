###################################################################################################
#' @title Validate vector 

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' Validate a vector to check that it is a vector, with optional tests for empty and numeric.
#' Returns True if passes all checks. FALSE otherwise.

#' @param vectIn Input vector to be validated
#' @param TestEmpty Boolean. TRUE to test whether the vector is empty (function will return FALSE if empty).
#' @param TestNumc Boolean. TRUE to test whether the vector is numeric or logical (function will return FALSE if not numeric or logical (which converts to 0 or 1)).

#' @return Boolean. True if the input vector is not empty. FALSE otherwise. \cr

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#'
#' @export

# changelog and author contributions / copyrights
#   Mija Choi (2020-01-07)
#     original creation
#   Cove Sturtevant (2023-02-03)
#     add logging 
#     add check for vector and options to check empty and numeric
#   Cove Sturtevant (2024-04-24)
#     Allow logical values when testing for numeric (which convert to 0 or 1)
##############################################################################################

def.validate.vector <- function(vectIn,TestEmpty=TRUE,TestNumc=TRUE,log=NULL) {

  a = TRUE

  # Initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }

  if(!is.vector(vectIn) || is.list(vectIn)) {
    a=FALSE
    log$error('Input must be a vector.')
    
  } 
  else if (TestNumc == TRUE && !is.numeric(vectIn) && !is.logical(vectIn)) {
    a=FALSE
    log$error('-------Input must be numeric or logical.')
  
  }
  else if (TestEmpty && length(vectIn) == 0) {
    a=FALSE
    log$error('Input must not be empty.')
    
  }

  return (a)
}
