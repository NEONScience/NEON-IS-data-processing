##############################################################################################
#' @title Parse vectors into key:multi-value sets

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Parse the elements of one or more vectors into key:multiple value sets  

#' @param listVect A list of vectors of class numeric or character. Each vector must be length 2 or greater.
#' @param NameList Character vector of length 2 indicating first the name to call the 
#' key in each output list, and then the name to call the value in each output list
#' Names default to key and value if not input.
#' @param Type A 2-element vector indicating the respective R data classes of the key and 
#' value. Defaults to the class of listVect. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A list of lists numbering the length of  with two elements, the key and value(s), except named according to NameList 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Vector input represents key:multi-value set
#' listVect <- list(c('key1','1.5','3','4','5'),c('key2','3'))
#' NameList <- c('MyKeys','MyValues')
#' Type <- c('character','numeric')
#' NEONprocIS.base::def.vect.pars.one.many(listVect=listVect,NameList=NameList,Type=Type)


#' @seealso \code{\link[NEONprocIS.base]{def.arg.pars}}
#' @seealso \code{\link[NEONprocIS.base]{def.vect.pars.pair}}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-03-11)
#     original creation
##############################################################################################
def.vect.pars.one.many <- function(listVect,
                                   NameList=c('key','value'),
                                   Type=rep(base::class(listVect[[1]]),2),
                                   log=NULL){
  
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Error check
  if(!base::is.list(listVect)){
    log$fatal(base::paste0('listVect must be a list.')) 
    base::stop()
    
  }
  
  numVect <- base::unlist(base::lapply(listVect,base::length))
  if(base::any(numVect < 2)){
    log$fatal(base::paste0('Length of each vector in listVect must be 2 or greater.')) 
    base::stop()
  }   
  
  if(base::length(NameList) != 2){
    log$fatal('Length of NameList must be 2.')
  }
  
  if(length(Type) != 2){
    log$fatal('Length of Type must be 2.')
    base::stop()
  }
 
  # Parse the key-value sets
  rpt <- base::lapply(
    listVect,
    FUN = function(idxVect) {
      rptIdx <- base::list(
                key = idxVect[1],
                value = utils::tail(x = idxVect, n = -1)
      )
      
      # Assign classes
      base::class(rptIdx$key) <- Type[1]
      base::class(rptIdx$value) <- Type[2]
      
      return(rptIdx)
    }
  )
  
  return(rpt)
}
