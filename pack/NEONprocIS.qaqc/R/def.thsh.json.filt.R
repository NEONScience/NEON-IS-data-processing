##############################################################################################
#' @title Filter NEON instrumented systems QC JSON-formatted thresholds 

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Filter QA/QC threshold information in JSON format (file or text) based
#' on matching field values.

#' @param NameFile Filename (including relative or absolute path). Must be json format. 
#' @param strJson Json-formatted text. Only one of NameFile or strJson must be entered.
#' @param NameFileOut Filename (optional) for writing the filtered output (in json format). Default is 
#' NULL, in which case the result is output to the environment
#' @param Term String vector of terms (field 'term_name' in the thresholds file/text) to search for. 
#' Retrieves matches to any Term. Enter NULL (default) if not filtering based on term.
#' @param Ctxt # string vector of contexts (field 'context'). Enter NULL (default) if not filtered based 
#' on context. NOTE: If either \code{Term} or \code{Ctxt} (not both) are populated, the filter will 
#' return matches to any entry in these inputs, treated individually. If both \code{Term} and \code{Ctxt} 
#' are populated, AND logic is applied, meaning that the \code{Term} must match one of the Term values 
#' and \code{Ctxt} must match ALL Context values.

#' @return If NameFileOut is entered, a filtered json file. Otherwise, a list output of the filtered 
#' thresholds in json format

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-05-16)
#     original creation
##############################################################################################
def.thsh.json.filt <- function(NameFile=NULL,
                               strJson=NULL,
                               NameFileOut=NULL,
                               Term=NULL,
                               Ctxt=NULL
){
  
  # Read in the thresholds json file
  if(!is.null(NameFile)){
    thsh <- rjson::fromJSON(file=NameFile,simplify=TRUE)$thresholds
  } else if (!is.null(strJson)){
    thsh <- rjson::fromJSON(json_str=strJson,simplify=TRUE)$thresholds
  } else {
    stop('Either NameFile or strJson must be supplied in the inputs (but not both).')
  }
  numThsh <- base::length(thsh)
  
  # Find all matching records for selected term(s)
  numCtxt <- base::length(Ctxt)
  if (!is.null(Term) && !is.null(Ctxt)){
    setUse <- base::unlist(base::lapply(thsh,function(list){list$term_name %in% Term && 
        base::sum(Ctxt %in% base::unlist(list$context))==numCtxt}))
  } else if (is.null(Term) && !is.null(Ctxt)){
    setUse <- base::unlist(base::lapply(thsh,function(list){base::sum(Ctxt %in% base::unlist(list$context))>0}))
  } else if (!is.null(Term) && is.null(Ctxt)){
    setUse <- base::unlist(base::lapply(thsh,function(list){list$term_name %in% Term}))
  } else {
    setUse <- base::rep(TRUE,numThsh)
  }
  
  # Filter
  rpt <- thsh[setUse]
  
  # Reinstate the 'thresholds' list
  rpt <- base::list(thresholds=rpt)
  
  # Either return the output or write to file
  if(base::is.null(NameFileOut)){
    return(rpt)
  } else {
    base::write(rjson::toJSON(rpt,indent=3),file=NameFileOut)
  }
    
}
