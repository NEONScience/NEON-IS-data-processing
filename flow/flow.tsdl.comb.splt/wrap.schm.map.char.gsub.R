##############################################################################################
#' @title Substitute character names in object after parsing AVRO map schema

#' @author 
#' Guy Litt \email{glitt@battelleecology.org}

#' @description 
#' Wrapper function. Turn the JSON formatted AVRO schema (either from file or a json string)
#' into a list containing a formatted dataframe. Then substitute matching terms in the provided
#'  object, changing anything matching the first term to the corresponding second term.
#' 
#' @param obj Character class. A character vector containing terms that should be matched.
#' @param FileSchm String. Optional. Full or relative path to schema file containing a mapping.
#'  One of FileSchm or Schm must be provided.
#' @param Schm String. Optional. Json formatted string of the AVRO file schema containing a mapping. 
#' One of FileSchm or Schm must be provided. If both Schm and FileSchm are provided, Schm will be ignored.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log 
#' output in addition to standard R error messaging. Defaults to NULL, in which no logger other than 
#' standard R error messaging will be used.


#' @return A list of:\cr
#' \code{obj}: the character object with newly substituted terms\cr
#' \code{map}: a data frame of variables/term mappings in the schema, their data type(s), and documentation string

#' @references 
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' NEONprocIS.base::def.schm.avro.pars(FileSchm='/pfs/avro_schemas/prt_calibrated.avsc')

#' @seealso \link[NEONprocIS.base]{def.read.avro.deve}
#' @seealso \link[NEONprocIS.base]{def.log.init}

#' @export

# changelog and author contributions / copyrights
#   Guy Litt (2021-04-01)
#     Originallly created
##############################################################################################


wrap.schm.map.char.gsub <- function(obj,
                                    FileSchm=NULL,
                                    Schm=NULL,
                                    log = NULL){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  if(base::is.null(FileSchm) && base::is.null(Schm)){
    # Generate error and stop execution
    msg <- base::paste0('One of FileSchm or Schm must be provided')
    log$fatal(msg)
    stop()
  }
  
  if(base::is.null(FileSchm)){
    mapRpt <- def.schm.avro.pars.map(Schm = Schm, log = log)
  } else {
    mapRpt <- def.schm.avro.pars.map(FileSchm = FileSchm, log = log)
  }
  
  map <- mapRpt$map
  
  if(!"term1" %in% base::names(map)){
    msg <- "The map object does not have term1 in the column name. Choosing first column."
    log$warn(msg)
    warning(msg)
    term1 <- map[,1]
  } else {
    term1 <- map$term1
  }
  
  if(!"term2" %in% base::names(map)){
    msg <- "The map object does not have term2 in the column name. Choosing second column."
    log$warn(msg)
    warning(msg)
    term2 <- map[,2]
  } else {
    term2 <- map$term2
  }
  # Perform text substituion based on term mappings
  objSub <- def.map.char.gsub(pattFind = term1, replStr = term2, obj = obj, log = log)
  
  # Check how many terms were substituted
  objDiff <- base::setdiff(obj,objSub)
  msgDiff <- base::paste0("Given ", base::length(obj), " character names, substituted ", base::length(objDiff), " total names based on the mapping schema.")
  log$debug(msgDiff)
  
  mapRpt <- base::list(obj = objSub, map = map)
  
  return(mapRpt)
}