#' @title Search for the best string match
#' @author Guy Litt
#' @description Given a character object and multiple match strings,
#' search for the best match as defined by the following criteria:
#' 1. the most characters that match,
#'     (e.g. depth10 is a better match to xxxdepth10 than depth1 to xxxdepth10)
#' 2. the best match if a \code{subFind} ends with a number, 
#'     (e.g. depth1 is a better match for xxxdepth1 rather than xxxdepth12)
#' 
#' @param obj the character vector of strings to search
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
#   Guy Litt (2021-04-3)
#     original creation

# cols <- base::colnames(cmboStatQm)
# 
# # Identify the columns without any depth term & columns w/ depth terms:
# idxsMapDpthAll <- base::lapply(thrmDpthDf$depthName, function(x) base::grep(x, cols)) 
# colsNonDpth <- cols[-base::unlist(idxsMapDpthAll)]
# colsData <- cols[-base::which(cols %in% colsNonDpth)]
# 
# # Note that just colsData and colsMrge will be kept, but also removing horizontalPosition/verticalPosition at end of loop
# obj <- colsData
# 
# obj <- paste0("depth11",names(dataLs$`001`$`103.503`))
# subFind <- c("depth0","depth1","depth11")

def.find.mtch.str.best <- function(obj, subFind, log=NULL){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  if(!base::is.character(obj) || !base::is.character(subFind)){
    log$error("Must provide character class objects, obj & subFind.")
    stop()
  }
  
  # Search for a pattern across all search terms, and choose whichever string match is longest. 
  # e.g. depth1 matches depth10 w/ 6 chars, but depth10 matches depth10 w/ 7 chars.
  matSnglDigMl <- base::sapply(obj, function(col) base::sapply(subFind, 
                                                                    function(x) base::attributes(base::gregexpr(pattern=x,text=col)[[1]])$match.length ) ) 
  # The indices corresponding to the maximum match length:
  idxMax <- base::sapply(1:base::ncol(matSnglDigMl), function(i) base::as.integer(base::which.max(matSnglDigMl[,i]) ) )
  
  # create the full column name - depth mapping df:
  dfMtch <- base::data.frame(obj=obj, idxMtch=idxMax, mtchGrp=subFind[idxMax], stringsAsFactors=FALSE)
  
  idxsEndNum <- base::which(!base::is.na(base::as.numeric(base::substring(text=subFind,first=base::nchar(subFind),
                                                      last=base::nchar(subFind)))))
  
  # IF a match string ends with a number - make sure other numbers don't follow
  if(base::length(idxsEndNum) > 0){
    log$debug("At least some match strings from subFind end with a number, so making sure matches don't happen with more numbers after the specific string.")
    # Check for faulty match situation based on an extra digit at the end of the match pattern, e.g. depth1 matches both depth10 & depth1
    matLogMoreDigt <-  base::sapply(obj, function(col) base::sapply(subFind, 
                                  function(x) (base::regexec(pattern=paste0(x,"\\d"), text=col)[[1]][1]) == 1))
    # Only the idxsEndNum indices matter for matLogMoreDigt, so set all others as FALSE
    idxIgnr <- which(base::is.na(as.numeric(substring(text=subFind,first=base::nchar(subFind),
                                                      last=base::nchar(subFind) )) ) )
    if(base::length(idxIgnr) > 0){
      matLogMoreDigt[idxIgnr,] <- FALSE
    }
    
    if(!base::identical(base::dim(matLogMoreDigt), base::dim(matSnglDigMl))){
      log$error("Expect matrices of the same dimension when identifying name matches.")
      stop()
    }
    
    # identify the indices where matLogMoreDigt is TRUE 
    idxsMoreDigt <- base::unlist(base::lapply(1:base::nrow(matLogMoreDigt),
                                              function(i) 
                                              {x <- base::which(matLogMoreDigt[i,] == TRUE);
                                              return(x)}))
    
    if(base::length(idxsMoreDigt) >0){
      # remove match columns corresponding to excess digits
      dfMtch <- dfMtch[-base::as.integer(idxsMoreDigt),]
    }
  }
  return(dfMtch)
}