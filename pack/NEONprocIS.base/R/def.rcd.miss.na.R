##############################################################################################
#' @title Identify any inconsistent or corrupt timeseries records across a set of files 

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Given a set of file paths containing timeseries data, identify any timestamps
#' that are not consistently present across all files. Also identify any records that contain NA 
#' values.

#' @param nameFile Character vector of any length, consisting of the full or relative paths to all
#' the data files to evaluate together for consistent timestamps and any NA-containing records. The
#' files must be avro or parquet files containing tabular data. These files will be read by 
#' NEONprocIS.base::def.read.avro.deve or NEONprocIS.base::def.read.parq, which will return a data frame. 
#' One column of the data frame must be readout_time.

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A list of \code{timeAll} and \code{timeBad}, each data frames with one column:
#' \code{readout_time} POSIXct timestamps signifying... for timeAll) the union of all encountered 
#' timestamps across the set of input files, and for timeBad) inconsistent or corrupt (NA-containing) 
#' records across the set of input files

#' @references Currently none

#' @keywords Currently none

#' @examples
#' NEONprocIS.base::def.rcd.miss.na(nameFile=c('/path/to/file1.avro','/another/path/to/file2.avro'))

#' @seealso \link[NEONprocIS.base]{def.read.avro.deve}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-03-04)
#     original creation
##############################################################################################
def.rcd.miss.na <- function(fileData,log = NULL) {
  
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Go through each sensor data file, looking for inconsistent timestamps and NA values 
  timeDmmy <- base::as.POSIXct(base::character(0)) # Initialize
  timeBad <- base::data.frame(readout_time=timeDmmy,stringsAsFactors = FALSE) # Initialize 
  for(idxFile in fileData){
    
    # What format?
    fmt <- utils::tail(base::strsplit(idxFile,'[.]')[[1]],1)
    
    # Load in file
    if (fmt == 'avro') {
      data  <-
        base::try(NEONprocIS.base::def.read.avro.deve(NameFile = idxFile,
                                                      NameLib = '/ravro.so',
                                                      log = log),
                  silent = FALSE)
    } else if (fmt == 'parquet') {
      data  <-
        base::try(NEONprocIS.base::def.read.parq(NameFile = idxFile, log = log),
                  silent = FALSE)
    } else {
      log$error(
        base::paste0(
          'Cannot determine file type for ',
          idxFile,
          '. Extension must be .avro or .parquet.'
        )
      )
      stop()
    }
    
    if (base::class(data) == 'try-error') {
      # Generate error and stop execution
      log$error(base::paste0('File ', idxFile, ' is unreadable.'))
      base::stop()
    }
    chkCol <- NEONprocIS.base::def.validate.dataframe(dfIn=data,TestNa=FALSE,TestNumc=FALSE,TestNameCol = 'readout_time',log=log)
    if(!chkCol){
      stop()
    }
    
    
    # Initialize if reading the first file
    if(idxFile == fileData[1]){
      timeAll <- base::data.frame(readout_time=data$readout_time,stringsAsFactors = FALSE)
    } 
    
    # Check for extra times
    timeXtra <- base::subset(data,subset=!(data$readout_time %in% timeAll$readout_time),select='readout_time')
    
    # Check for missing times
    timeMiss <- base::subset(timeAll,subset=!(timeAll$readout_time %in% data$readout_time),select='readout_time')
    
    # Add extra or missing times to list of bad timestamps
    timeBad <- base::rbind(timeBad,timeXtra,timeMiss)
    
    # Search for timestamps associated with NA values in any column
    setNa <-
      base::unique(base::unlist(base::lapply(
        base::subset(data, select = -readout_time),
        FUN = function(colData) {
          base::which(base::is.na(colData))
        }
      )))
    timeBad <- base::rbind(timeBad,base::subset(data,subset=base::seq_len(base::nrow(data)) %in% setNa,select='readout_time'))
    
    # Combine this file's measured times with the running list
    timeAll <- base::rbind(timeAll,timeXtra)
    
  }
  timeBad <- base::data.frame(readout_time=base::unique(timeBad$readout_time),stringsAsFactors = FALSE) # Initialize 
  log$debug(base::paste0(base::nrow(timeBad), ' missing or corrupt records found for sensor group.'))
  
  timeAll$readout_time <- base::sort(timeAll$readout_time)
  
  
  return(base::list(timeAll=timeAll,timeBad=timeBad))
  
}
