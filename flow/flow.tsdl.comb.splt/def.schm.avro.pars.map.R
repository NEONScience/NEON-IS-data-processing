##############################################################################################
#' @title Parse AVRO mapping schema to list

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Turn the JSON formatted AVRO schema (either from file or a json string)
#' into a formatted list. 

#' @param FileSchm String. Optional. Full or relative path to schema file containing a map. 
#' One of FileSchm or Schm must be provided.
#' @param Schm String. Optional. Json formatted string of the AVRO file schema containing a map.
#'  One of FileSchm or Schm must be provided. If both Schm and FileSchm are provided, Schm will be ignored.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log 
#' output in addition to standard R error messaging. Defaults to NULL, in which no logger other than 
#' standard R error messaging will be used.


#' @return A list of:\cr
#' \code{schmJson}: the avro schema in json format\cr
#' \code{schmList}: a list of avro schema properties and fields
#' \code{map}: a data frame of variables/term mappings in the schema, their data type(s), and documentation string

#' @references 
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' NEONprocIS.base::def.schm.avro.pars(FileSchm='/pfs/avro_schemas/prt_calibrated.avsc')

#' @seealso \link[NEONprocIS.base]{def.read.avro.deve}
#' @seealso \link[NEONprocIS.base]{def.log.init}
#' @seealso wrap.schm.map.char.gsub

#' @export

# changelog and author contributions / copyrights
#   Guy Litt (2021-04-01)
#     adapted for avro mappings from def.schm.avro.pars.R by Cove Sturtevant
##############################################################################################
def.schm.avro.pars.map <- function(FileSchm=NULL,
                               Schm=NULL,
                               log=NULL
){
  
  # Error check
  if(base::is.null(FileSchm) && base::is.null(Schm)){
    # Generate error and stop execution
    msg <- base::paste0('One of FileSchm or Schm must be provided')
    if(!base::is.null(log)){
      log$fatal(msg)
    } 
    stop(msg)
  }
  
  # Read in the schema file
  if(!base::is.null(FileSchm)){
    
    Schm <- base::try(base::paste0(base::readLines(FileSchm),collapse=''),silent=true)
    if(base::class(Schm) == 'try-error'){
      
      # Generate error and stop execution
      msg <- base::paste0('Avro schema file ', FileSchm, ' is unreadable. Error text:',attr(Schm,"condition"))
      if(!base::is.null(log)){
        log$fatal(msg)
      } 
      stop(msg)
    }    
  }
  
  # Interpret as list
  schmList <- rjson::fromJSON(json_str=Schm,simplify=FALSE)
  
  if(base::length(schmList$map$values) == 0){
    msg <- base::paste0('Avro schema does not follow expected map format.')
    if(!base::is.null(log)){
      log$fatal(msg)
    } 
    stop(msg)
  }
  
  # Turn field list into a data frame
  var <- base::lapply(schmList$map$values, function(v) base::unlist(v))
  map <- base::data.frame(term1 = base::names(var), term2 = base::unlist(var),row.names = NULL,stringsAsFactors = FALSE)
  
  if("description" %in% base::names(schmList$map)){
    map$desc <- schmList$map$description
  }
  
  if("type" %in% base::names(schmList$map)){
    map$type <- schmList$map$type
  }  
  
  # Output
  rpt <- base::list()
  rpt$schmJson <- Schm
  rpt$schmList <- schmList
  rpt$map <- map
  
  return(rpt)
  
}
