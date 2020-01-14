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


def.validate.json <- function(jsonIn) {
 
   c <- RJSONIO::isValidJSON(jsonIn)

return (c)
}

