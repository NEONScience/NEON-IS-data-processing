##############################################################################################
#' @title Create Parquet schema from publication workbook

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Create a arrow schema object from a publication workbook. The fields in the
#' schema for each table will be crafted in the order the terms are found in the pub workbook, 
#' with any duplicates removed. Note that the rank column in the pub workbook is not heeded in 
#' order to avoid issues when multiple pub workbooks are combined.

#' @param NameFile String. Optional (either one of NameFile or PubWb must be entered). 
#' Name (including relative or absolute path) of publication workbook file.
#' @param pubWb Data frame. Optional (either one of NameFile or PubWb must be entered). A data frame
#' of the pub workbook, as read in by def.read.pub.wb.
#' @param TablPub Character vector. The table(s) in the pub workbook(s) to produce. By default all of them 
#' are produced. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#' 
#' @return An Apache Arrow Schema object containing a parquet schema representing the table(s) in the 
#' publication workbook.

#' @references 
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[arrow]{data-type}
#' @seealso \link[NEONprocIS.pub]{def.read.pub.wb}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2023-02-20)
#     original creation
##############################################################################################
def.schm.parq.from.pub.wb <- function(NameFile=NULL,
                                      pubWb=NULL,
                                      TablPub=NULL,
                                      log=NULL
){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  if(base::length(NameFile) > 0){
    pubWb <- NEONprocIS.pub::def.read.pub.wb(NameFile)
  } else if (base::length(pubWb) == 0){
    log$error('No publication workbook supplied. Either NameFile or pubWb must be entered.')
    stop()    
  }
  
  # Constrain to the desired pub tables
  if(base::is.null(TablPub)){
    TablPub <- base::unique(pubWb$table)
  }
  pubWb <- pubWb[pubWb$table %in% TablPub,]
  
  # Create the schema for each pub table 
  schm <- base::vector(base::length(TablPub),mode="list")
  names(schm) <- TablPub
  for(tablPubIdx in TablPub){
    
    # Rows of interest in pub wb
    setRow <- base::which(pubWb$table==tablPubIdx)

    # Remove duplicated fields for this table (this can happen when multiple pub workbooks are included with the same table)
    setRow <- setRow[!base::duplicated(pubWb$fieldName[setRow])]
    numRow <- base::length(setRow)
    
    # Create each field in the schema
    fldSchm <- base::vector(numRow,mode='list') # Initialize list of schema fields
    for(idx in base::seq_len(numRow)){
      
      nameField <- pubWb$fieldName[setRow[idx]]
      typeData <- pubWb$dataType[setRow[idx]]
      fmt <- pubWb$pubFormat[setRow[idx]]
      
      # See documentation on arrow::data-type
      if (typeData == 'dateTime') {
        typeArrw <- arrow::timestamp(unit = "ms")
      } else if (typeData == 'real' && base::grepl(pattern='.#',fmt)) {
        # Assume here that float is sufficient to represent any decimal field. Implement logic for double() later if needed.
        typeArrw <- arrow::float()
      } else if (typeData == 'real' && fmt == 'integer') {
        typeArrw <- arrow::int32()
      } else if ((typeData == 'integer' || typeData == 'signed integer') && base::grepl('QF',nameField)) {
        # Quality flags have very few values
        typeArrw <- arrow::int8()
      } else if (typeData == 'unsigned integer' && base::grepl('QF',nameField)) {
        # Quality flags have very few values
        typeArrw <- arrow::uint8()
      } else if (typeData == 'integer' || typeData == 'signed integer' ) {
        typeArrw <- arrow::int32()
      }  else if (typeData == 'unsigned integer') {
        typeArrw <- arrow::uint32()
      } else if (typeData == 'string') {
        typeArrw <- arrow::string()
      } else if (typeData == 'boolean') {
        typeArrw <- arrow::boolean()
      } else {
        typeArrw <- arrow::null()
      }
      
      # Place in the list
      fldSchm[[idx]] <- arrow::field(nameField, typeArrw, nullable = TRUE)
    }
    
    # Create schema
    # Example
    # arrow::schema(
    #   arrow::field("c", arrow::bool(),nullable = TRUE),
    # )
    schm[[tablPubIdx]] <- base::do.call(arrow::schema, fldSchm)
    
  }
  
  return(schm)
}
