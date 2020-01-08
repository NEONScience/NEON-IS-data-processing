###################################################################################################
#' @title Validate data frame not empty or invalid

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' Validate a data frame to check it is empty or has invalid values.
#' Returns True if not empty AND has vaild values. FALSE otherwise.

#' @param dfIn Input data frame to be validated

#' @return Boolean. True if the input vector is not empty AND does not have invalid values. FALSE otherwise. \cr

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


def.validate.dataframe <- function(dfIn) {
  b = TRUE

  if (nrow(dfIn) == 0) {

    b = FALSE
  }

  else  if (any(is.na(dfIn))) {

    b = FALSE
  }

  return (b)
}
