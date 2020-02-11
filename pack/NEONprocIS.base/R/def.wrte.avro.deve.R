##############################################################################################
#' @title Write AVRO file (development library)

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Write AVRO file from data frame. Uses a super developmental version of the library. The 
#' requisite dependent libraries must be installed on the host system.

#' @param data Data frame. Data to write to file.
#' @param NameFile String. Name (including relative or absolute path) of AVRO file.
#' @param NameSchm String. Optional. Schema name. 
#' @param NameSpceSchm String. Optional. Schema namepace. 
#' @param Schm String. Optional. Json formatted string of the AVRO file schema. Example:\cr
#' "{\"type\" : \"record\",\"name\" : \"ST\",\"namespace\" : \"org.neonscience.schema.device\",\"fields\" : [ {\"name\" :\"readout_time\",\"type\" : {\"type\" : \"long\",\"logicalType\" : \"timestamp-millis\"},\"doc\" : \"Timestamp of readout expressed in milliseconds since epoch\"}, {\"name\" : \"soilPRTResistance\",\"type\" : [ \"null\", \"float\" ],\"doc\" : \"DPID: DP0.00041.001 TermID: 1728 Units: ohm Description: Soil Temperature, Level 0\",\"default\" : null} ],\"__fastavro_parsed\" : true}"\cr
#' Defaults to NULL, at which point the schema will be constructed using a different input or the data frame
#' @param NameFileSchm String. Optional. A filename (include relative or aboslute path) of an avro 
#' schema file (.avsc format). Defaults to NULL, at which point the schema will be constructed using a different 
#' input or from the data frame
#' @param NameLib String. Name (including relative or absolute path) of AVRO library. Defaults to ./ravro.so.

#' @return Numeric. 0 = successful write.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.base]{def.read.avro.deve}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-03-21)
#     original creation
#   Cove Sturtevant (2019-04-15)
#     adjusting for new ravro library that auto-creates schema from data frame
#   Cove Sturtevant (2019-04-24)
#     adjusting for new ravro library that writes class character
#   Cove Sturtevant (2019-06-17)
#     adding option to read output schema from file. 
#     Fix bug in call to write with schema.
#   Cove Sturtevant (2019-10-09)
#     fix bug not allowing conversion of multiple POSIX variables to numeric
##############################################################################################
def.wrte.avro.deve <- function(data,
                               NameFile,
                               NameSchm=NULL,
                               NameSpceSchm=NULL,
                               Schm=NULL,
                               NameFileSchm=NULL,
                               NameLib='ravro.so'
){
  
  # Load the library
  base::dyn.load(NameLib)
  
  # Are any variables in POSIX format? Convert to milliseconds since epoc 1970-01-01 00:00:00
  clssVar <- base::lapply(X=data,FUN=base::class)
  idxTime <- base::unlist(base::lapply(X=clssVar,FUN=function(idxClss){base::sum(base::grepl(pattern='POSIX',x=idxClss))>0}))
  data[,idxTime] <- base::as.data.frame(base::lapply(base::subset(data,select=idxTime),base::as.numeric))*1000 # Convert
    
  # Are there any variables as factor?
  if(base::sum(c('factor') %in% clssVar) > 0){
    stop('Cannot write data of class factor.')
  }

  # Assign schema name and namespace
  if(!base::is.null(NameSchm)){
    base::attr(data, "avroname") <- "myschemaname"
  }
  if(!base::is.null(NameSpceSchm)){
    base::attr(data, "avronamespace") <- "myschemanamespace"
  }
  
  # Write the data. 
  # Note: The PACKAGE argument is needed to include this as a function in the NEONprocIS.base package
  if(base::is.null(Schm) && base::is.null(NameFileSchm)){
    # Schema to be constructed from data frame
    rpt <- base::.Call('writeavro',data,NameFile,PACKAGE='ravro') 
  } else {
    # Schema specified in inputs
    
    # Read in schema file
    if(!base::is.null(NameFileSchm)){
      con <- base::file(NameFileSchm,open='r')
      Schm <- base::paste0(base::readLines(con),collapse='')
      base::close(con)
    }
    rpt <- base::.Call('writeavro_withschema',data,Schm,NameFile,PACKAGE='ravro') 
  }
 
  return(rpt)
}
