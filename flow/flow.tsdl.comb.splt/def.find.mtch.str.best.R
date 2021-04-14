#' @title Search for the best string match
#' @author Guy Litt
#' @description Given a character vector and match string(s),
#' search for the best match as defined by the following criteria:
#' 1. the most characters that match,
#'     (e.g. depth10 is a better match to xxxdepth10 than depth1 to xxxdepth10)
#' 2. the best match if a \code{subFind} ends with a number, 
#'     (e.g. depth1 is a better match for xxxdepth1 rather than xxxdepth12)
#' 
#' @param obj the character vector of strings to search, consisting of at least two strings.
#' @param subFind a character vector of substrings to search for in obj
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log.
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A dataframe with the following columns:
#' obj - the remaining objects that were matched to a \code{subFind} string.
#' idxMax - the index corresponding to the original \code{obj}
#' mtchGrp - the corresponding matching string from \code{subFind} found in \code{obj}

#' @references Currently none

#' @keywords Currently none
#' @export

# changelog and author contributions / copyrights
#   Guy Litt (2021-04-13)
#     original creation

def.find.mtch.str.best <- function(obj, subFind, log=NULL){
  require(stringr)
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  if(!base::is.character(obj) || !base::is.character(subFind)){
    log$error("Must provide character class objects, obj & subFind.")
    stop()
  }
  
  if(base::length(obj) <=1){
    log$error("The obj must be a character vector of at least length 2.")
    stop()
  }

  # Create matrix of string matches
  matRslt <- base::sapply(subFind, function(x) stringr::str_match(string = obj, pattern = x) )

  # Anything found in this matrix should be ignored
  matRsltAddNum <- base::sapply(subFind, function(x) stringr::str_match(string = obj, pattern = base::paste0(x,"\\d")) )
  
  # Remove the matches corresponding to more numbers in the match:
  matRslt[base::which(!base::is.na(matRsltAddNum), arr.ind = TRUE)] <- NA
  
  # Test - there should be no more than one non-NA value in each row:
  if(base::any(base::rowSums(!base::is.na(matRslt)) > 1)){
    log$error("No more than 1 match should happen for any given subFind and obj.")
    stop()
  }
  
  bestMtch <- base::unlist(base::lapply(1:base::nrow(matRslt), function(i) 
    if(length(base::which(!base::is.na(matRslt[i,]))) == 0) NA 
    else matRslt[i,base::which(!base::is.na(matRslt[i,]))] ) )
  
  dfMtch <- base::data.frame(obj = obj, mtchGrp = bestMtch, stringsAsFactors = FALSE)
  idxsNa <- base::which(base::is.na(dfMtch$mtchGrp))
  if(base::length(idxsNa)>0){
    dfMtch <- dfMtch[-idxsNa,]
  }
  
  dfMtch$idxMtch <- unlist(lapply(1:nrow(dfMtch), function(i) which(subFind == dfMtch[i,"mtchGrp"])))
  
  
  return(dfMtch)
}



