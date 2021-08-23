###################################################################################################
#' @title validate if the JSON content is strictly valid

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description

#' @param 

#' @return String. \cr

#' @references Currently none

#' @keywords Currently none

#' @examples NEONprocIS.base::def.validate.json (NameFileIn)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#'
#' @export

# changelog and author contributions / copyrights
#   Mija Choi (2021-08-23)
#     original creation
##############################################################################################

def.generate.err.msg <- function (errmsg = NULL,
                              fun_calling,
                              fun_called,
                              lineNum){

  errmsg <- base::paste0("\"",errmsg, "\" in " , fun_called," near line ",lineNum,", called by ", fun_calling)
  
  return (errmsg)
}
