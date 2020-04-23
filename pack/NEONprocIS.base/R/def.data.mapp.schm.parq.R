##############################################################################################
#' @title Map data to a parquet schema

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Rearrange and/or create dummy columns in a data frame to match the column
#' order of a parquet schema. Optionally convert data type to match the schema.

#' @param data Data frame with named columns. 
#' @param schm A Parquet schema of class ArrowObject
#' @param ConvType Logical TRUE to attempt to convert the data type to that indicated in the 
#' schema. FALSE to attempt no type conversion. Default is FALSE. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame that (hopefully) matches the output schema

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-04-16)
#     original creation
##############################################################################################
def.data.mapp.schm.parq <- function(data,schm,ConvType=FALSE,log=NULL){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Parse the schema to get the columns and data types
  infoSchm <- NEONprocIS.base::def.schm.parq.pars(schm)
  nameVarOut <- infoSchm$name
    
  # Data columns may be missing  and/or out of order. Add dummy columns for those that we're missing 
  nameVarAdd <- nameVarOut[!(nameVarOut %in% base::names(data))]
  numData <- base::nrow(data)
  for(idxVarAdd in nameVarAdd){
    data[[idxVarAdd]] <- base::rep(x=base::as.character(NA),times=numData)
  }
  
  # Rearrange to the original columns order & get rid of variables not in the schema
  data <- data[,nameVarOut]
 
  # Convert data types
  if(ConvType == TRUE){
    data <- NEONprocIS.base::def.data.conv.type.parq(data=data,type=infoSchm,log=log)
    
  }
  
  return(data)
}
