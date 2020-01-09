###################################################################################################
#' @title Validate vector not empty

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' Validate a numeric vector to check it is empty.
#' Returns True if not empty. FALSE otherwise.

#' @param vectIn Input vector to be validated

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
##############################################################################################

def.validate.vector.notEmpty <- function(vectIn) {
  a = TRUE

  if (length(vectIn) == 0) {
    a = FALSE
  }

  return (a)
}
