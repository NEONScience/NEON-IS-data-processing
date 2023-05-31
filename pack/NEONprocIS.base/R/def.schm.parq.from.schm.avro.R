##############################################################################################
#' @title Create Parquet schema from Avro schema

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Create a arrow schema object from an avro schema file or json string. 

#' @param FileSchm String. Optional. Full or relative path to schema file. One of FileSchm or Schm must be 
#' provided.
#' @param Schm String. Optional. Json formatted string of the AVRO file schema. One of FileSchm or Schm must 
#' be provided. If both Schm and FileSchm are provided, Schm will be ignored.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log 
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be 
#' created for use within this function. 
#' 
#' @return An Apache Arrow Schema object containing a parquet schema representation of the avro schema.

#' @references 
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[arrow]{data-type}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2023-02-20)
#     original creation
##############################################################################################
def.schm.parq.from.schm.avro <- function(FileSchm=NULL,
                                         Schm=NULL,
                                         log=NULL
){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Parse the avro schema
  typeVar <- NEONprocIS.base::def.schm.avro.pars(FileSchm=FileSchm,
                                                 Schm=Schm,
                                                 log=log
                                                 )$var
  numRow <- base::nrow(typeVar)
  
    # Create each field in the schema
  fldSchm <- base::vector(numRow,mode='list') # Initialize list of schema fields
  for(idx in base::seq_len(numRow)){
      
    nameField <- typeVar$name[idx]
    typeData <- typeVar$type[idx]
    typeData <- base::strsplit(typeData,'|',fixed=TRUE)[[1]]

    # See documentation on arrow::data-type
    if (base::any(base::grepl('timestamp-millis', typeData))) {
      typeArrw <- arrow::timestamp(unit = "ms")
      
    } else if (base::any(base::grepl('timestamp-micro', typeData))) {
      typeArrw <- arrow::timestamp(unit = "us")
      
    } else if (base::any(base::grepl('timestamp-nano', typeData))) {
      typeArrw <- arrow::timestamp(unit = "ns")
      
    } else if (base::any(base::grepl('time', typeData) || base::grepl('date', typeData))) {
      typeArrw <- arrow::timestamp(unit = "s")
    
    } else if (base::any(base::grepl('float', typeData))) {
      typeArrw <- arrow::float()
      
    } else if (base::any(typeData == 'double')) {
      typeArrw <- arrow::double()
      
    } else if (base::any(typeData == 'uint8')) {
      typeArrw <- arrow::uint8()
      
    } else if (base::any(typeData == 'uint16')) {
      typeArrw <- arrow::uint16()
      
    } else if (base::any(typeData == 'uint32')) {
      typeArrw <- arrow::uint32()
      
    } else if (base::any(typeData == 'uint64')) {
      typeArrw <- arrow::uint64()
      
    } else if (base::any(typeData == 'int8')) {
      typeArrw <- arrow::int8()
      
    } else if (base::any(typeData == 'int16')) {
      typeArrw <- arrow::int16()
      
    } else if (base::any(typeData == 'int32')) {
      typeArrw <- arrow::int32()
      
    } else if (base::any(typeData == 'int64') || base::any(typeData == 'long')) {
      typeArrw <- arrow::int64()
      
    } else if (base::any(base::grepl('uint', typeData))) {
      typeArrw <- arrow::uint32()
      
    } else if (base::any(base::grepl('int', typeData))) {
      typeArrw <- arrow::int32()
      
    } else if (base::any(typeData == 'string')) {
      typeArrw <- arrow::string()
      
    } else if (base::any(typeData == 'boolean')) {
      typeArrw <- arrow::boolean()
      
    } else if (base::any(typeData == 'fixed')) {
      # Fixed size binary
      size <- typeData[which(typeData == 'fixed')+1]
      typeArrw <- arrow::fixed_size_binary(base::as.integer(size))
      
    } else if (base::any(typeData == 'bytes')) {
      # Binary
      typeArrw <- arrow::binary()
      
    } else {
      typeArrw <- arrow::null()
      log$warn(base::paste0('Data type(s) [',base::paste0(typeData,collapse=','),'] not recognized for field name [',nameField,']. Setting parquet schema type to null, which will likely result in NA data.'))
    }
    
    # Place in the list
    if (base::any(typeData == 'null')){
      nullable = TRUE
    } else {
      nullable = FALSE
    }
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
