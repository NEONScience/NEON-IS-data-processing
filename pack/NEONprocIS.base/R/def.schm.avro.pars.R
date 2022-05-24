##############################################################################################
#' @title Parse AVRO schema to list

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Turn the JSON formatted AVRO schema (either from file or a json string)
#' into a formatted list. 

#' @param FileSchm String. Optional. Full or relative path to schema file. One of FileSchm or Schm must be 
#' provided.
#' @param Schm String. Optional. Json formatted string of the AVRO file schema. One of FileSchm or Schm must 
#' be provided. If both Schm and FileSchm are provided, Schm will be ignored.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log 
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be 
#' created for use within this function. 

#' @return A list of:\cr
#' \code{schmJson}: the avro schema in json format\cr
#' \code{schmList}: a list of avro schema properties and fields
#' \code{var}: a data frame of variables/fields in the schema, their data type(s), and documentation string

#' @references 
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' NEONprocIS.base::def.schm.avro.pars(FileSchm='/pfs/avro_schemas/prt_calibrated.avsc')

#' @seealso \link[NEONprocIS.base]{def.read.avro.deve}
#' @seealso \link[NEONprocIS.base]{def.log.init}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-10-25)
#     original creation
##############################################################################################
def.schm.avro.pars <- function(FileSchm=NULL,
                               Schm=NULL,
                               log=NULL
){
  
  # Initialize log if not input
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Error check
  if(base::is.null(FileSchm) && base::is.null(Schm)){
    # Generate error and stop execution
    msg <- base::paste0('One of FileSchm or Schm must be provided')
    log$fatal(msg)
    stop(msg)
  }
  
  # Read in the schema file
  if(!base::is.null(FileSchm)){
    
    Schm <- base::try(base::paste0(base::readLines(FileSchm),collapse=''),silent=TRUE)
    if(base::class(Schm) == 'try-error'){
      
      # Generate error and stop execution
      msg <- base::paste0('Avro schema file ', FileSchm, ' is unreadable. Error text:',attr(Schm,"condition"))
      log$fatal(msg)
      stop(msg)
    }    
  }
  
  # Interpret as list
  schmList <- rjson::fromJSON(json_str=Schm,simplify=FALSE)
  
  # Turn field list into a data frame
  var <- base::lapply(schmList$fields,FUN=function(idx){
    if(base::is.null(idx$name)){
      idx$name <- NA
    }
    if(base::is.null(idx$type)){
      idx$type <- NA
    }
    if(base::is.null(idx$doc)){
      idx$doc <- NA
    }
    
    base::data.frame(name=idx$name,type=base::paste0(base::unlist(idx$type),collapse='|'),doc=idx$doc,stringsAsFactors=FALSE)
  })
  var <- base::do.call(base::rbind,var)
  
  # Output
  rpt <- base::list()
  rpt$schmJson <- Schm
  rpt$schmList <- schmList
  rpt$var <- var
  
  return(rpt)
  
}
