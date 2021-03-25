###################################################################################################
#' @title validate if the JSON content is strictly valid

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' Validate an input json to check it is valid.
#' Returns FALSE if the json is empty OR invaild . TRUE otherwise.

#' @param jsonIn Input json to be validated

#' @return Boolean. TRUE if the input json is not empty AND valid. FALSE otherwise. \cr

#' @references Currently none

#' @keywords Currently none

#' @examples NEONprocIS.base::def.validate.json (NameFileIn)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#'
#' @export

# changelog and author contributions / copyrights
#   Mija Choi (2020-01-14)
#     original creation
##############################################################################################
#library(RJSONIO)

def.validate.json <- function(jsonIn, log = NULL) {
  # Initialize log if not input
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  #set the default to FALSE
  c = FALSE
  #
  res <- try((RJSONIO::isValidJSON(jsonIn)), silent = TRUE)
  #
  # First, check the input json passed on exists
  #
  if (class(res) == "try-error") {
    log$error(base::paste0( 'In def.validate.json::::: Error, the file does not exist, ', jsonIn))
  }
  #
  # Then, check if the jsonIn is a valid json
  #
  if (class(res) != "try-error") {
    if (RJSONIO::isValidJSON(jsonIn))
    {
      c = TRUE
      log$debug(base::paste0(jsonIn, ' is valid strictly.'))
    }
    else
    {
      log$warn(base::paste0(jsonIn, ' is invalid ***** '))
    }
  }
  
  return (c)
}
