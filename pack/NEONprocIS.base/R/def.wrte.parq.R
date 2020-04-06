##############################################################################################
#' @title Write Parquet file

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Write Parquet file from data frame. Optionally input a parquet or
#' avro schema to convert column names and/or data types. Any variables of class factor will
#' be written as class character.

#' @param data Data frame. Data to write to file.
#' @param NameFile String. Name (including relative or absolute path) of AVRO file.
#' @param Schm Optional. Either a Parquet schema of class ArrowObject, or a Json formatted string 
#' with an AVRO file schema. Example:\cr
#' "{\"type\" : \"record\",\"name\" : \"ST\",\"namespace\" : \"org.neonscience.schema.device\",\"fields\" : [ {\"name\" :\"readout_time\",\"type\" : {\"type\" : \"long\",\"logicalType\" : \"timestamp-millis\"},\"doc\" : \"Timestamp of readout expressed in milliseconds since epoch\"}, {\"name\" : \"soilPRTResistance\",\"type\" : [ \"null\", \"float\" ],\"doc\" : \"DPID: DP0.00041.001 TermID: 1728 Units: ohm Description: Soil Temperature, Level 0\",\"default\" : null} ],\"__fastavro_parsed\" : true}"\cr
#' Defaults to NULL, in which case the schema will be constructed using the argument NameFileSchm 
#' (if not NULL) or auto-generated from the data frame.
#' @param NameFileSchm String. Optional. A filename (include relative or aboslute path) of an avro 
#' schema file (.avsc format). Defaults to NULL, in which case the schema will be constructed using 
#' the Schm argument (if not NULL) or auto-generated from the data frame
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return The data frame as written to the output.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' data <- data.frame(x=c(1,2,3),y=c('one','two','three'),stringsAsFactors=FALSE)
#' dataOut <- NEONprocIS.base::def.wrte.parq(data,NameFile='out.parquet')

#' @seealso \link[NEONprocIS.base]{def.read.avro.deve}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-04-06)
#     original creation
##############################################################################################
def.wrte.parq <- function(data,
                          NameFile,
                          Schm=NULL,
                          NameFileSchm=NULL,
                          log=NULL
){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }

  # Convert data according to schema
  if(base::is.null(Schm) && base::is.null(NameFileSchm)){
    # Schema to be constructed from data frame. We done.
    log$debug('Auto-creating schema using data frame.')
    rpt <- arrow::write_parquet(x=data,sink=NameFile) 
  } else {
    # Schema specified in inputs
    nameVarIn <- base::names(data)
    numVarIn <- base::length(nameVarIn)
    
    # Parquet Schema
    if('ArrowObject' %in% base::class(Schm)){
      
      log$debug('Using parquet schema from input argument Schm.')
      
      # Pull the col names and data types from the schema
      typeVar <- NEONprocIS.base::def.schm.parq.pars(Schm,log=log)
      

    } else if (!base::is.null(Schm)) {
      
      log$debug('Using avro schema from input argument Schm.')
  
      # Pull the col names and data types from the schema
      typeVar <- NEONprocIS.base::def.schm.avro.pars(Schm=Schm,log=log)$var
    
    } else {
  
      log$debug('Reading avro schema from file.')
      
      # Read in avro schema file
      con <- base::file(NameFileSchm,open='r')
      Schm <- base::paste0(base::readLines(con),collapse='')
      base::close(con)
      typeVar <- NEONprocIS.base::def.schm.avro.pars(Schm=Schm,log=log)$var
    }
    
    # Rename the variables to match the schema
    if(numVarIn != base::length(typeVar$name)){
      log$error('Number of variables in the data does not match number of variables in the schema.')
      stop()
    } else {
      base::names(data) <- typeVar$name
    }
    
    # Assign the data type for each column from the schema
    for(idxVar in base::seq_len(base::length(typeVar$name))){
      
      # type indicated by schema
      typeIdx <- base::strsplit(typeVar$type[idxVar],",")[[1]]
      
      # Assign R class from schema-indicated data type
      if(base::any(c('timestamp-millis','timestamp[ms]') %in% typeIdx)){
        # Convert to POSIXct. Parquet writer handles this well.
        data[[idxVar]] <- base::as.POSIXct(data[[idxVar]],tz='GMT')
      } else if('string' %in% typeIdx){
        # Use as.character rather than class(..) <- 'character'. Converts factors to character without factors as attributes.
        data[[idxVar]] <- base::as.character(data[[idxVar]])
      } else if ('boolean' %in% typeIdx){
        base::class(data[[idxVar]]) <- "logical"
      } else if (base::any(c("int","long") %in% typeIdx)){
        base::class(data[[idxVar]]) <- "integer"
      } else if ("float" %in% typeIdx){
        base::class(data[[idxVar]]) <- "numeric"
      } else if ("double" %in% typeIdx){
        base::class(data[[idxVar]]) <- "double"
      } else {
        log$warn(base::paste0("Don't know what to do with data type: ",typeVar$type[idxVar],'. No type conversion will be attempted.'))
      }
      
    }
    
    # Write the data
    rpt <- arrow::write_parquet(x=data,sink=NameFile) 
  }
  
  log$info(base::paste0('Wrote parquet file: ',NameFile))
  return(rpt)
}
