##############################################################################################
#' @title Write Parquet file

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Write Parquet file from data frame. Optionally input a parquet or
#' avro schema to convert column names and/or data types. Any variables of class factor will
#' be written as class character.

#' @param data Data frame. Data to write to file.
#' @param NameFile String. Name (including relative or absolute path) of output parquet file.
#' @param Schm Optional. Either a Parquet schema of class ArrowObject, or a Json formatted string 
#' with an AVRO file schema. Example:\cr
#' "{\"type\" : \"record\",\"name\" : \"ST\",\"namespace\" : \"org.neonscience.schema.device\",\"fields\" : [ {\"name\" :\"readout_time\",\"type\" : {\"type\" : \"long\",\"logicalType\" : \"timestamp-millis\"},\"doc\" : \"Timestamp of readout expressed in milliseconds since epoch\"}, {\"name\" : \"soilPRTResistance\",\"type\" : [ \"null\", \"float\" ],\"doc\" : \"DPID: DP0.00041.001 TermID: 1728 Units: ohm Description: Soil Temperature, Level 0\",\"default\" : null} ],\"__fastavro_parsed\" : true}"\cr
#' Defaults to NULL, in which case the schema will be constructed using the argument NameFileSchm 
#' (if not NULL) or auto-generated from the data frame.
#' @param NameFileSchm String. Optional. A filename (include relative or aboslute path) of an avro 
#' schema file (.avsc format). Defaults to NULL, in which case the schema will be constructed using 
#' the Schm argument (if not NULL) or auto-generated from the input data.
#' @param CompType String. Compression algorithm. Defaults to 'gzip'. Can also be NULL, in which case
#' the arrow::write_parquet default will be used. 
#' @param CompLvl Numeric. Compression level. See documentation for arrow::write_parquet for details.
#' @param Dict Logical. Vector either length 1 or the same length as the number of columns in \code{data} 
#' representing  whether to apply dictionary encoding to each respective data column. If length 1, the 
#' choice is applied to all data columns. Defaults to NULL, in which case dictionary enconding is 
#' determined automatically based on the prevalence of repeated values.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return The data frame as written to the output.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' data <- data.frame(x=c(1,2,3),y=c('one','two','three'),stringsAsFactors=FALSE)
#' dataOut <- NEONprocIS.base::def.wrte.parq(data,NameFile='out.parquet')

#' @seealso \link[NEONprocIS.base]{def.read.parq}
#' @seealso \link[arrow]{write_parquet}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-04-06)
#     original creation
#   Cove Sturtevant (2023-02-21)
#     Convert avro schema to parquet schema and convert to arrow table before writing parquet file. 
#     This retains the data types (and likely space savings) upon write
##############################################################################################
def.wrte.parq <- function(data,
                          NameFile,
                          Schm=NULL,
                          NameFileSchm=NULL,
                          CompType='gzip',
                          CompLvl=5,
                          Dict=NULL,
                          log=NULL
){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  numVar <- base::length(data)
  numRow <- base::nrow(data)
  
  if(base::length(Dict) == 1){
    Dict <- base::rep(Dict,numVar)
  }
  
  # Error check
  if(!base::is.null(Dict) && (base::length(Dict) != numVar || !is.logical(Dict))){
    log$error(base::paste0('Input argument Dict must be NULL or a logical vector the same length as the number of columns in data.'))
    stop()
  }
  
  # Convert data according to schema
  if(base::is.null(Schm) && base::is.null(NameFileSchm)){
    # No schema was input.
    # Look for a schema attached as an attribute of the data (as produced by NEONprocIS.base::def.read.parq)
    schmData = base::attr(data,'schema')
    
    if(!base::is.null(schmData) && 'ArrowObject' %in% base::class(schmData)){
      log$debug('Using parquet schema attached as an attribute to the data frame.')
    } else {
      schmData <- NULL
      log$debug('Auto-creating schema using data frame.')
    }
  
    # Use schema attached as attribute to the data, or auto-construct it from the data frame.
    data <- arrow::arrow_table(data, schema=schmData)

  } else {
    
    if (!base::is.null(Schm) && 'ArrowObject' %in% base::class(Schm)){
      # Schema specified in inputs as a parquet schema.
      log$debug('Using parquet schema from input argument Schm.')
      
      # Pull the col names and data types from the schema
      typeVar <- NEONprocIS.base::def.schm.parq.pars(Schm,log=log)
      
    } else if (!base::is.null(Schm) || !base::is.null(NameFileSchm)) {
        
      log$debug('Creating parquet schema from input avro schema.')
    
      # Create parquet schema from the avro schema
      Schm <- NEONprocIS.base::def.schm.parq.from.schm.avro(FileSchm=NameFileSchm,Schm=Schm,log=log)    
      
      # Pull the col names and data types from the schema
      typeVar <- NEONprocIS.base::def.schm.parq.pars(Schm,log=log)
      
    }
      
    # Rename the variables to match the schema
    if(numVar != base::length(typeVar$name)){
      log$error('Number of variables in the data does not match number of variables in the schema.')
      stop()
    } else {
      base::names(data) <- typeVar$name
    }
    # In order to reliably apply the schema, we need to do some type conversion of the data first.
    data <- NEONprocIS.base::def.data.conv.type.parq(data=data,type=typeVar,log=log)
    
    # Apply the arrow schema
    data <- arrow::arrow_table(data, schema=Schm)
    
  }
  
  # Determine whether to use dictionary encoding for each variable
  if(base::is.null(Dict)){
    Dict <- base::rep(FALSE,numVar)
    
    # Run through each var. If the values repeat often, use dictionary encoding
    for(idxVar in base::seq_len(numVar)){
      enblDict <- try(base::length(base::unique(data[[idxVar]])) < 0.7*numRow,silent=TRUE) # Fails for some arrow types
      if(base::all(base::class(enblDict) != 'try-error')){
        if(!("POSIXt" %in% class(data[[idxVar]]))){
          Dict[idxVar] <- TRUE
        }
        
      }
    }    
  }

  # Write the data
  rpt <- base::try(
    arrow::write_parquet(x=data,
                         sink=NameFile,
                         compression=CompType,
                         compression_level=CompLvl,
                         use_dictionary=Dict, 
                         coerce_timestamps='ms', 
                         allow_truncated_timestamps=TRUE
                         ),
    silent=FALSE
  )
  
  if('try-error' %in% base::class(rpt)){
    log$error(base::paste0('Could not write parquet file ',NameFile))
    stop()
  } else {
    log$debug(base::paste0('Wrote parquet file: ',NameFile))
    return(rpt)
  }
}
