##############################################################################################
#' @title Map input variable names to output variable names

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Create a data frame that maps input variable names to output variable names
#' given vectors of each. Option to add additional variable names that are the same in and out.  

#' @param nameVarIn String vector of input variable names. Defaults to zero-length character vector. 
#' @param nameVarOut String vector of output variable names matched to the same relative position 
#' of nameVarIn. Defaults to zero-length character vector. 
#' @param nameVarDfltSame String vector of variable names that are to be mapped the same in & out
#' if they are not indicated in nameVarIn. Defaults to zero-length character vector. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame of \cr
#' \code{nameVarIn} Character. Input variable names
#' \code{nameVarOut} Character. Output variable names

#' @references Currently none

#' @keywords Currently none

#' @examples 
#' NEONprocIS.base::def.var.mapp.in.out(nameVarIn=c('varIn1','varIn2'),
#'                                      nameVarOut=c('nameOut1','nameOut2'),
#'                                      nameVarDfltSame=c('nameSame1','nameSame2'))

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-13)
#     original creation
#   Cove Sturtevant (2020-07-01)
#     add error catching for non-unique input-output variable names
##############################################################################################
def.var.mapp.in.out <- function(nameVarIn=base::character(0),
                                nameVarOut=base::character(0),
                                nameVarDfltSame=base::character(0),
                                log=NULL){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  
  if(base::length(nameVarIn) != base::length(nameVarOut)){
    log$fatal(base::paste0('Number of input variable names does not match number of output variable names.'))
    stop()
  }
  
  # Error check that nameVarIn entries are unique
  if(base::length(base::unique(nameVarIn)) != length(nameVarIn)){
    log$fatal(base::paste0('Each of the values in nameVarIn must be unique.'))
    stop()
  }
  
  # Error check that output names nameVarOut are unique and not overlapping with namveVarDfltSame
  nameVarOutChk <- c(nameVarOut,nameVarDfltSame[!(nameVarDfltSame %in% nameVarIn)])
  if(base::length(base::unique(nameVarOutChk)) != length(nameVarOutChk)){
    log$fatal(base::paste0('Output variable names must be unique and/or you cannot rename a variable to one of the values in nameVarDfltSame unless the value in nameVarDfltSame is also in nameVarIn.'))
    stop()
  }
  
  mappNameVar <- base::data.frame(nameVarIn=nameVarIn,nameVarOut=nameVarOut,stringsAsFactors=FALSE)
  newVar <- nameVarDfltSame[!(nameVarDfltSame %in% nameVarIn)] 
  mappNameVar <- base::rbind(mappNameVar,base::data.frame(nameVarIn=newVar,nameVarOut=newVar,stringsAsFactors = FALSE))
  
  return(mappNameVar)
}
