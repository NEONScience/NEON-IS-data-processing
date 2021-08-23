##############################################################################################
#' @title Generate an error message by constructing the info passed into.

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' Generate a message starting with an error message. These, function which is calling, function called and line number where the error occurred, are appended. 
#' And return the message 

#' @param Errmsg String. Can be NULL if no specific error. 
#' 
#' Requires rlang and utils to run these commands
#' 
#' @param fun_calling. Can get the function name that is calling by the command, rlang::call_frame(n = 2)$fn_name
#' @param fun_called. Can get the function name that is called by the command, rlang::call_frame(n = 1)$fn_name
#' @param lineNum Number. Can get the line number by the command, getSrcLocation(function() {}, "line")

#' @return Message.

#' @references 
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' Modify the statement below by replacing "Bad Input" with your own error message and run it.
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
