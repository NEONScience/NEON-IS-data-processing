##############################################################################################
#' @title Parse vector into key:value pairs

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Parse the elements of a vector into key:value pairs.  

#' @param vect Vector of class numeric or character. If length of vect is 1, all expected 
#' keys indicated in KeyExpc will be assigned this value. If length of vect is in multiples
#' of 2, the first value of each pair is assigned the key and the second value of each pair
#' is assigned the value. 
#' @param KeyExpc A vector of expected keys. If these are missing in vect, they will be created.
#' Each missing key will be assigned the value of vect if it is length 1, otherwise the value in
#' ValuDflt.  Defaults to NULL, in which case no checking for missing keys will be done. Must not
#' be NULL if vect is a single value. 
#' @param ValuDflt A single value (character or numeric) of the default value to assign any keys 
#' in KeyExpc that are not found in vect. Note that if vect is length 1, any missing keys will 
#' be assigned the value in vect rather than ValuDflt. Defaults to NA.
#' @param NameCol Character vector of length 2 indicating the names of the output columns.
#' Column names default to key and value.
#' @param Type A 2-element vector indicating the respective R data classes of the keys and 
#' values. Defaults to the class of vect. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A data frame with parsed key:value pairs. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Vector input represents key:value pairs
#' vect <- c('key1','1.5')
#' KeyExpc <- c('key1','key2','key3')
#' ValuDflt <- 3
#' NameCol <- c('MyKey','MyValue')
#' Type <- c('character','numeric')
#' NEONprocIS.base::def.vect.pars.pair(vect=vect,KeyExp=KeyExpc,ValuDflt=ValuDflt,NameCol=NameCol,Type=Type)
#' 
#' # Single value input for vect
#' vect <- 5
#' KeyExpc <- c('key1','key2','key3')
#' NameCol <- c('MyKey','MyValue')
#' Type <- c('character','numeric')
#' NEONprocIS.base::def.vect.pars.pair(vect=vect,KeyExp=KeyExpc,NameCol=NameCol,Type=Type)

#' @seealso \code{\link[NEONprocIS.base]{def.arg.pars}}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-13)
#     original creation
##############################################################################################
def.vect.pars.pair <- function(vect,
                               KeyExpc=NULL,
                               ValuDflt=NA,
                               NameCol=c('key','value'),
                               Type=rep(base::class(vect),2),
                               log=NULL){
  
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Error check
  numVect <- base::length(vect)
  if(numVect!=1 && numVect %% 2 > 0){
    log$fatal(base::paste0('Length of vect must be 1 or a multiple of 2.')) 
    base::stop()
  }   
  
  if(base::length(NameCol) != 2){
    log$fatal('Length of NameCol must be 2.')
    stop()
  }
  
  if(base::length(ValuDflt) != 1){
    log$fatal('Length of ValuDflt must be 1.')
    base::stop()
  }
  
  if(length(Type) != 2){
    log$fatal('Length of Type must be 2.')
    base::stop()
  }
  
  if(numVect == 1 && base::length(KeyExpc) == 0){
    log$fatal('Length of KeyExpc must be greater than 0 if length of vect is 1.')
    base::stop()
  }
  
  # Assign key:value pairs
  numVect <- base::length(vect)
  if(numVect > 1){
    rpt <- base::data.frame(key=vect[base::seq.int(from=1,to=numVect,by=2)],
                            value=vect[base::seq.int(from=2,to=numVect,by=2)],
                            stringsAsFactors = FALSE)
  } else {
    rpt <- base::data.frame(key=KeyExpc,value=vect,stringsAsFactors = FALSE)
  }
  
  # Fill in missing keys
  keyMiss <- KeyExpc[!(KeyExpc %in% rpt$key)] # missing keys
  if(base::length(keyMiss) > 0){
    # Fill missing keys with default value
    rpt <- base::rbind(rpt,base::data.frame(key=keyMiss,value=ValuDflt,stringsAsFactors = FALSE)) # Fill in with default
  }

  # Assign classes
  base::class(rpt$key) <- Type[1]
  base::class(rpt$value) <- Type[2]
  
  # Assign column names
  base::names(rpt) <- NameCol
  
  return(rpt)
}
