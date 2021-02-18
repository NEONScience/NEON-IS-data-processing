##############################################################################################
#' @title Merge the contents of multiple files (avro or parquet) that share a common time variable.

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Merge the contents of multiple avro or parquet files that share a common time 
#' variable. Other than the time variable, the files should different columns. If any duplicate 
#' column names are found, only the first instance found will be retained. Any missing
#' timestamps among the files will be filled with NA values.

#' @param file Character vector of full or relative file paths. Must be avro or parquet format.
#' @param nameVarTime Character value. The name of the time variable common across all files. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame with the merged file contents.

#' @references Currently none

#' @keywords Currently none

#' @examples
#' # Not run
#' file <- c('/path/to/file1.avro','/path/to/file2.avro')
#' NEONprocIS.base::def.file.comb.ts(file=file,nameVarTime='readout_time')

#' @seealso \link[NEONprocIS.base]{def.read.avro.deve}
#' @seealso \link[NEONprocIS.base]{def.read.parq}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-03-11)
#     original creation
#   Cove Sturtevant (2020-04-28)
#     added support for parquet format
##############################################################################################
def.file.comb.ts <- function(file,nameVarTime,log = NULL) {
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  for(idxFile in file){
    
    # What format?
    fmt <- utils::tail(base::strsplit(idxFile,'[.]')[[1]],1)
    
    # Load in file 
    if(fmt == 'avro'){
      idxData  <- base::try(NEONprocIS.base::def.read.avro.deve(NameFile=idxFile,NameLib='/ravro.so',log=log),silent=FALSE)
    } else if (fmt == 'parquet'){
      idxData  <- base::try(NEONprocIS.base::def.read.parq(NameFile=idxFile,log=log),silent=FALSE)
    } else {
      log$error(base::paste0('Cannot determine file type for ',idxFile,'. Extension must be .avro or .parquet.'))
      stop()
    }
    if(base::any(base::class(idxData) == 'try-error')){
      log$error(base::paste0('File ', idxFile,' is unreadable.')) 
      stop()
    } else {
      log$debug(base::paste0('Successfully read in file: ',idxFile))
    }
    
    # Pull out the time variable
    if(!NEONprocIS.base::def.validate.dataframe(dfIn=idxData,TestNameCol=nameVarTime,log=log)){
      stop()
    }
    
    # If this is the first file, use it as the basis for adding onto in subsequent files
    if(idxFile == file[1]){
      data <- idxData
    } else {
      # Make sure there are no duplicate columns
      dupCol <-base::names(idxData) %in% base::setdiff(base::names(data),nameVarTime)
      if(base::sum(dupCol) > 0){
        log$warn(base::paste0('The non-time column names contained in the files ', base::paste0(file,collapse=','), ' overlap. Taking the first instance of duplicate column names.'))
        idxData <- idxData[!dupCol]
      }
      
      # Issue a warning if the timestamps are not identical
      if(!base::all.equal(data[[nameVarTime]],idxData[[nameVarTime]])){
        log$warn(base::paste0('Timestamps in file ',idxFile, ' are not fully consistent with previously loaded files for this datum path. NAs for non-matching times will result (to be turned to -1 later).'))
      }
      
      # Merge the data
      data <- base::merge(x=data,y=idxData,by=nameVarTime,all=TRUE,sort=FALSE)
    }
  } # End loop around input files
  
  log$info(base::paste0('Successfully merged the contents of ', base::length(file),' files.'))
  
  return(data)
  
}
