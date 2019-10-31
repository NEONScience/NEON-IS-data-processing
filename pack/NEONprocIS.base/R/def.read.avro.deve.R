##############################################################################################
#' @title Read AVRO file (development library)

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Read in AVRO file. Uses a super developmental version of the library. The 
#' requisite dependent libraries must be installed on the host system.

#' @param NameFile String. Name (including relative or absolute path) of AVRO file.
#' @param NameLib String. Name (including relative or absolute path) of AVRO library.

#' @return A data frame of the data contained in the AVRO file. The schema is included in attribute 'schema'

#' @references 
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' myData <- NEONprocIS.base::def.read.avro.deve(NameFile='/scratch/test/myFile.avro',NameLib='/ravro.so')
#' attr(myData,'schema') # Returns the schema

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-03-13)
#     original creation
#   Cove Sturtevant (2019-09-30)
#     assign R classes to empty data frames if they are NULL
#   Cove Sturtevant (2019-10-09)
#     generalized the conversion to POSIX for variables of type "timestamp-millis"
##############################################################################################
def.read.avro.deve <- function(NameFile,
                               NameLib='ravro.so'
){
  
  # Load the library
  base::dyn.load(NameLib)
  
  # Read the data. 
  # Note: The package argument is needed to include this as a function in the NEONprocIS.base package
  rpt <- base::.Call('readavro',NameFile,PACKAGE='ravro') 
  
  # If any of the column classes are NULL, read the data type from the schema and assign them
  typeNull <- base::unlist(base::lapply(X=rpt,FUN=function(var){base::sum(base::class(var)=="NULL")==1}))
  
  # Parse the schema to a list
  Schm <- rjson::fromJSON(json_str=base::attr(rpt,"schema"),simplify=FALSE)
  
  # Assign the data type for each column from the schema
  for(idxVarNull in base::names(typeNull)[typeNull]){
    
    # Which schema field pertains to this column
    idxSchm <- base::which(base::unlist(base::lapply(Schm$fields,FUN=function(idx){idx$name})) == idxVarNull)
    
    # type indicated by schema
    typeIdx <- base::unlist(Schm$fields[[idxSchm]]$type)
    
    # Assign R class from schema-indicated data type
    if(base::sum(typeIdx == "string") > 0){
      base::class(rpt[[idxVarNull]]) <- "character"
    } else if (base::sum(typeIdx == "boolean") > 0){
      base::class(rpt[[idxVarNull]]) <- "logical"
    } else if (base::sum(typeIdx %in% c("int","long")) > 0){
      base::class(rpt[[idxVarNull]]) <- "integer"
    } else if (base::sum(typeIdx %in% c("float","double")) > 0){
      base::class(rpt[[idxVarNull]]) <- "numeric"
    }
    
  }
  
  # Assign timestamps to POSIXct
  for(idxVar in base::names(rpt)){
    
    # Which schema field pertains to this column
    idxSchm <- base::which(base::unlist(base::lapply(Schm$fields,FUN=function(idx){idx$name})) == idxVar)
    
    # type indicated by schema
    typeIdx <- base::unlist(Schm$fields[[idxSchm]]$type)
    
    # Assign R class from schema-indicated data type
    if(base::sum(typeIdx == "timestamp-millis") > 0){
      rpt[[idxVar]] <- base::as.POSIXct(x=rpt[[idxVar]]/1000, tz = "GMT", origin=as.POSIXct('1970-01-01',tz = "GMT"))
    }
    
  }  
  
  return(rpt)
}
