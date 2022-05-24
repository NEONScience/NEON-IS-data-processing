##############################################################################################
#' @title Read event data in json format

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Read event data in json format into data frame. Converts timestamps to POSIX.
#' NOTE: The json records must have uniform format.

#' @param NameFile String. Name (including relative or absolute path) of event json file.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log 
#' output in addition to standard R error messaging. Defaults to NULL, in which no logger other than 
#' standard R error messaging will be used.

#' @return A data frame of the data contained in the json file. 

#' @references 
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' myEventData <- NEONprocIS.base::def.read.evnt.json(NameFile='/scratch/test/myFile.json')

#' @seealso \code{\link[NEONprocIS.base]{def.log.init}}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-12-19)
#     original creation
#   Mija Choi (2022-04-12)
#     added log initialization and modified to catch errors
##############################################################################################
def.read.evnt.json <- function(NameFile,log=NULL){
  
  # Initialize log if not input
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  }
  
  listEvnt <- base::try(rjson::fromJSON(file=NameFile,simplify=TRUE)[[1]],silent=FALSE)
  if(base::class(listEvnt) == 'try-error'){
    msg <- base::paste0('File ', NameFile,' is unreadable.')
    log$fatal(msg)
    stop(msg)        
  }
  
  # Turn all the NULLs into NAs
  listEvnt <- base::lapply(listEvnt,function(list){
    base::lapply(list,function(valu){
      if(base::is.null(valu) || base::length(valu)==0){
        return(NA)
      } else {
        return(valu)
      }
    })
  })
  
  listEvnt <- base::lapply(listEvnt,base::as.data.frame,stringsAsFactors=FALSE)
  evnt <- base::do.call(base::rbind,listEvnt)

  # Assign timestamps to POSIXct
  evnt$timestamp <- base::strptime(evnt$timestamp,format='%Y-%d-%mT%H:%M:%OSZ',tz='GMT')
  
  return(evnt)
}
