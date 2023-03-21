##############################################################################################
#' @title Convert data types in data frame to be compatible with parquet or avro schema data types

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Convert data types in data frame to match parquet or avro schema types.

#' @param data Data frame of data to be converted. 
#' @param type A data frame of: \cr
#' \code{name} Character. Name of data column
#' \code{type} Character. Data type. There may be multiple types, delimited by pipes (|)
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return The input data frame with data types converted as specified

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' data <- data.frame(x=c(1,2,3),y=c('one','two','three'),stringsAsFactors=FALSE)
#' type <- data.frame(name=c('x'),type=c('string|utf8'),stringsAsFactors=FALSE)
#' dataOut <- NEONprocIS.base::def.data.conv.type.parq(data=data,type=type)

#' @seealso \link[NEONprocIS.base]{def.write.parq}
#' @seealso \link[arrow]{data-type}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-04-16)
#     original creation
#   Cove Sturtevant (2023-02-21)
#     refined arrow data types
##############################################################################################
def.data.conv.type.parq <- function(data,
                                    type,
                                    log=NULL
){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Pull the variable names in data
  nameVar <- base::names(data)
    
  # Assign the data type for each column 
  for(idx in base::seq_len(base::nrow(type))){
    
    # Get column name and type 
    nameIdx <- type$name[idx]
    typeIdx <- strsplit(type$type[idx],'[|]')[[1]]
    
    if(!(nameIdx %in% nameVar)){
      log$warn(base::paste0('Variable: ', nameIdx, ' not found in input data. No type conversion will be performed for this variable.'))
      next
    }
    
    # Assign R class from schema-indicated data type
    if(base::any(base::grepl('time',typeIdx)) || base::any(base::grepl('date',typeIdx))){
      # Convert to POSIXct. Parquet writer handles this well.
      data[[nameIdx]] <- base::as.POSIXct(data[[nameIdx]],tz='GMT')
      
    } else if(base::any(base::grepl('dictionary<values=string',typeIdx))){
      # Use as.character rather than class(..) <- 'character'. Converts factors to character without factors as attributes.
      data[[nameIdx]] <- base::as.factor(data[[nameIdx]])
      
    } else if(base::any(typeIdx %in% c('string','utf8','large_utf8','character'))){
      # Use as.character rather than class(..) <- 'character'. Converts factors to character without factors as attributes.
      data[[nameIdx]] <- base::as.character(data[[nameIdx]])
      
    } else if (base::any(typeIdx %in% c('bool','boolean','logical'))){
      base::class(data[[nameIdx]]) <- "logical"
      
    } else if (base::any(base::grepl('int',typeIdx)) || base::any(typeIdx %in% c("long","short"))){
      base::class(data[[nameIdx]]) <- "integer"
      
    } else if (base::any(base::grepl('float',typeIdx)) || base::any(typeIdx %in% c("numeric","decimal"))){
      base::class(data[[nameIdx]]) <- "numeric"
      
    } else if ("double" %in% typeIdx){
      base::class(data[[nameIdx]]) <- "double"
      
    } else {
      log$warn(base::paste0("Don't know what to do with data type: ",typeIdx,' intended for variable: ',nameIdx,'. No type conversion will be attempted.'))
    }
    
  }
  
  return(data)
}
