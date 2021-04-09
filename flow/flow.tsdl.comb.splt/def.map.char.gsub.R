##############################################################################################
#' @title Substitute character names in object based on provided mapping

#' @author 
#' Guy Litt \email{glitt@battelleecology.org}

#' @description 
#' Definition function. Find a character term match and replace with the provided replacement
#' string
#' 
#' @param obj Character class. A character vector containing terms that should be matched.
#' @param pattFind Character class. A character vector containing terms that sould be 
#' substituted in \code{obj}.
#' @param strRepl Character class. A character vector contains the terms that should substitute
#' any instances of \code{pattFind} in \code{obj}.
#' @param Schm String. Optional. Json formatted string of the AVRO file schema containing a mapping. 
#' One of FileSchm or Schm must be provided. If both Schm and FileSchm are provided, Schm will be ignored.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log 
#' output in addition to standard R error messaging. Defaults to NULL, in which no logger other than 
#' standard R error messaging will be used.


#' @return 
#' \code{obj}: the character object with newly substituted terms\cr

#' @references 
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' 
#' def.map.char.gsub(pattFind =  base::c("ThermistorDepth501","ThermistorDepth502","ThermistorDepth503",
#'                     "ThermistorDepth504","ThermistorDepth505","ThermistorDepth506",
#'                     "ThermistorDepth507","ThermistorDepth508","ThermistorDepth509",
#'                     "ThermistorDepth510","ThermistorDepth511"),
#'                   replStr = base::c("depth0","depth1","depth2","depth3","depth4","depth5","depth6",
#'                     "depth7","depth8","depth9","depth10"),
#'                   obj = thrmDpthDf$depthLocs)
                      
#' @seealso \link[NEONprocIS.base]{def.read.avro.deve}
#' @seealso \link[NEONprocIS.base]{def.log.init}

#' @export

# changelog and author contributions / copyrights
#   Guy Litt (2021-04-01)
#     Originallly created

def.map.char.gsub <- function(pattFind, replStr, obj){
  
  if(base::length(pattFind) != base::length(replStr)){
    stop("pattFind and replStr must have lengths.")
  }
  
  if(!"character" %in% base::class(pattFind)){
    stop("pattFind must be a character class.")
  }
  
  if(!"character" %in% base::class(replStr)){
    stop("replStr must be a character class.")
  }
  
  if(!"character" %in% base::class(obj)){
    stop("obj must be a character class.")
  }
  
  for(idx in 1:base::length(pattFind)){
    patt <- pattFind[idx]
    repl <- replStr[idx]
    obj <- base::gsub(pattern = patt, replacement = repl, x = obj)
  }
  
  return(obj)
}
