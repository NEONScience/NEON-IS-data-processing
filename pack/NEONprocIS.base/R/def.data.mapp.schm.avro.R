##############################################################################################
#' @title Map data to an output avro schema 

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Rearrange and/or create dummy columns in a data frame to match the column
#' order of an output avro schema. Optionally convert data type to match output schema.

#' @param data Data frame with named columns. 
#' @param schm json string of the output schema 
#' @param ConvType Logical TRUE to attempt to convert the data type to that indicated in the json
#' schema. FALSE to attempt no type conversion. Default is FALSE. Note: POSIX conversion to
#' numeric will result in seconds since epoc 1970-01-01 00:00:00.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame that (hopefully) matches the output schema

#' @references Currently none

#' @keywords Currently none

#' @examples 
#' NEONprocIS.base::def.data.mapp.schma.avro(

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-20)
#     original creation
##############################################################################################
def.data.mapp.schm.avro <- function(data,schm,ConvType=FALSE,log=NULL){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Parse the schema to a list
  schmList <- rjson::fromJSON(json_str=schm,simplify=FALSE)
  
  # Get the field names
  nameVarOut <- base::unlist(base::lapply(schmList$fields,FUN=function(idxFld){idxFld$name}))
    
  # Data columns may be missing  and/or out of order. Add dummy columns for those that we're missing 
  nameVarAdd <- nameVarOut[!(nameVarOut %in% base::names(data))]
  numData <- base::nrow(data)
  for(idxVarAdd in nameVarAdd){
    data[[idxVarAdd]] <- base::rep(x=base::as.character(NA),times=numData)
  }
  # Rearrange to the original columns order & get rid of variables not in the schema
  data <- data[,nameVarOut]
 
  if(ConvType == TRUE){
    # Assign the data type for each column from the schema
    for(idxVar in base::seq_len(base::length(nameVarOut))){
      
      # type indicated by schema
      typeIdx <- base::unlist(schmList$fields[[idxVar]]$type)
      
      # Assign R class from schema-indicated data type
      if(base::sum(typeIdx == "string") > 0){
        base::class(data[[idxVar]]) <- "character"
      } else if (base::sum(typeIdx == "boolean") > 0){
        base::class(data[[idxVar]]) <- "logical"
      } else if (base::sum(typeIdx %in% c("int","long")) > 0){
        base::class(data[[idxVar]]) <- "integer"
      } else if (base::sum(typeIdx %in% c("float","double")) > 0){
        base::class(data[[idxVar]]) <- "numeric"
      }
      
    }
    
  }
  
  return(data)
}
