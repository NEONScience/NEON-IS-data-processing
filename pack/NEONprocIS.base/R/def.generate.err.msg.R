##################################################################################################################
#' @title Generate an error message. It includes error message, function called, line number and function calling

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' Generate an error message with error message, function that was called, line number where the error occurred, 
#' and function calling. And return the error message 

#' @param Errmsg String. Can be NULL if no specific error. 
#' 
#' @param fun_calling String.
#' @param fun_called String. 
#' @param lineNum Number. 

#' @return Message.

#' @references 
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' msg <- NEONprocIS.base::def.generate.err.msg(errmsg="Bad input", fun_calling=rlang::call_frame(n = 2)$fn_name, fun_called=rlang::call_frame(n = 1)$fn_name, lineNum=getSrcLocation(function() {}, "line"))
#' log$error(msg)
#' stop(msg)
#' An example output, Error in check(x) : "Bad input" in check near line 4, called by test 

#' @seealso Currently none

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
