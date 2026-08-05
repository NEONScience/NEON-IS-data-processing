##############################################################################################
#' @title Cast Arrow Table Columns to Match Schema

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Cast Arrow table columns to match the types specified in a target schema.
#' Columns that cannot be cast are left unchanged and a warning is logged.
#' @param data An Arrow table object
#' @param schm Target Arrow schema
#' @param log Optional logger object. Defaults to NULL.

#' @return The Arrow table with columns cast to match the schema

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2026-06-22)
#     original creation

##############################################################################################
def.wrte.parq.cast.cols <- function(data, schm, log=NULL){
    
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Iterate through schema fields and cast columns as needed
  for(idx in base::seq_len(schm$num_fields)){
    field <- schm$field(idx - 1)  # Arrow uses 0-based indexing
    colName <- field$name
    targetType <- field$type
    
    # Get the current column
    currentCol <- data$GetColumnByName(colName)
    if (base::is.null(currentCol)) {
      log$warn(base::paste0('Column "', colName, '" not found in input Arrow table; skipping cast.'))
      next
    }
    currentType <- currentCol$type
    
    # Convert types to strings before error handling
    currentTypeStr <- currentType$ToString()
    targetTypeStr <- targetType$ToString()
    
    # Check if casting is needed
    if(!identical(currentType, targetType)){
      # Try to cast the column
      tryCatch({
        data[[colName]] <- currentCol$cast(targetType)
        log$debug(base::paste0('Cast column "', colName, '" from ', currentTypeStr, ' to ', targetTypeStr))
      }, error = function(e){
        log$warn(base::paste0('Could not cast column "', colName, '" from ', currentTypeStr, ' to ', targetTypeStr, '. Error: ', conditionMessage(e)))
      })
    }
  }
  
  return(data)
}
