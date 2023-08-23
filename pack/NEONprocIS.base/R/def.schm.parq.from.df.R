##############################################################################################
#' @title Create Parquet schema auto-detected from data frame

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Create an arrow schema object auto-detected from data frame

#' @param df Data frame
#' @param log Optional. A logger object as produced by NEONprocIS.base::def.log.init to produce structured log 
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be 
#' created for use within this function. 
#' 
#' @return An Apache Arrow Schema object for the input data frame

#' @references 
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[arrow]{data-type}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2023-08-14)
#     original creation
##############################################################################################
def.schm.parq.from.df <- function(df,
                                  log=NULL
){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Parse 
  typeVar <- base::lapply(df,base::class)
  numVar <- base::length(typeVar)
  nameVar <- base::names(df)
  
  # Create each field in the schema
  #Arrow timestamps are stored as a 64-bit integer with column metadata to associate a time unit (e.g. milliseconds, microseconds, or nanoseconds), and an optional time zone.
  fldSchm <- base::vector(numVar,mode='list') # Initialize list of schema fields
  for(idx in base::seq_len(numVar)){
      
    nameField <- nameVar[idx]
    typeData <- typeVar[idx]

    # See documentation on arrow::data-type
    if (base::any(base::grepl('POSIX', typeData) | grepl('Date',typeData))) {
      tz <- base::attr(df[[nameField]],'tzone')
      if(is.null(tz)){
        tz=""
      }
      typeArrw <- arrow::timestamp(unit = "ms", timezone = tz)

    } else if (base::any(typeData == 'numeric')) {
      typeArrw <- arrow::float()
      
    } else if (base::any(typeData == 'integer')) {
      typeArrw <- arrow::int32()
      
    } else if (base::any(typeData == 'character')) {
      typeArrw <- arrow::string()
      
    } else if (base::any(typeData == 'logical')) {
      typeArrw <- arrow::boolean()

    } else {
      typeArrw <- arrow::null()
      log$warn(base::paste0('Data type(s) [',base::paste0(typeData,collapse=','),'] not recognized for field name [',nameField,']. Setting parquet schema type to null, which will likely result in NA data.'))
    }
    
    # Place in the list
    # if (base::any(typeData == 'null')){
    nullable = TRUE
    # } else {
      # nullable = FALSE
    # }
    fldSchm[[idx]] <- arrow::field(nameField, typeArrw, nullable = nullable)
  }
    
  # Create schema
  # Example
  # arrow::schema(
  #   arrow::field("c", arrow::bool(),nullable = TRUE),
  # )
  schm <- base::do.call(arrow::schema, fldSchm)

  
  return(schm)
}
