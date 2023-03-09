##############################################################################################
#' @title Read Parquet file

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Read in parquet file. 

#' @param NameFile String. Name (including relative or absolute path) of AVRO file.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#' 
#' @return A data frame of the data contained in the Parquet file. The schema is included in
#' attribute 'schema'.

#' @references 
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' myData <- NEONprocIS.base::def.read.parq(NameFile='/scratch/test/myFile.avro')
#' attr(myData,'schema') # Returns the schema

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-04-01)
#     original creation
#   Cove Sturtevant (2022-02-10)
#     Use base::data.frame instead of base::as.data.frame to avoid tibble data frame
##############################################################################################
def.read.parq <- function(NameFile,
                          log=NULL
){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  if(base::length(NameFile) > 1){
    NameFile <- NameFile[1]
    log$warn(base::paste0('More than one data file was input. Using only the first: ',NameFile))
  }
  
  # Pull in as arrow object, retrieve schema, and convert to data frame
  objParq <- arrow::read_parquet(file=NameFile,as_data_frame=FALSE)
  data <- base::data.frame(objParq)
  base::attr(data,'schema') <- objParq$schema

  # Assign timezone for POSIX variables
  clssVar <- base::lapply(X=data,FUN=base::class)
  setTime <- base::unlist(base::lapply(X=clssVar,FUN=function(idxClss){base::any(base::grepl(pattern='POSIX',x=idxClss))}))
  for (idxVar in base::names(data)[setTime]){
    base::attr(data[[idxVar]],'tzone') <- 'GMT'
  }
  
  log$debug(base::paste0('Successfully read in parquet file: ',NameFile, ' to data frame.'))
  
  return(data)
}
