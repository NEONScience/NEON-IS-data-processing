##############################################################################################
#' @title Fill in missing timeseries records and NA-out corrupt records

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Given a set of timestamps indicating missing or corrupt records, fill in
#' missing or corrupt timeseries records with a specific value.

#' @param data Data frame with at least two columns, one of which must be:
#' \code{readout_time} POSIXct timestamps
#' @param timeBad A data frame with one column:
#' \code{readout_time} POSIXct timestamps signifying missing or corrupt records
#' @param valuBad the value to fill the records with. Defaults to NA.

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame with the same columns as \code{data}. Any timestamps contained in \code{timeBad}
#' but not found in \code{data} are filled in with other values as NA. Any timestamps found in both
#' \code{timeBad} and \code{data} have all values except readout_time set to NA.

#' @references Currently none

#' @keywords Currently none

#' @examples
#' NEONprocIS.base::def.rcd.fix.miss.na(nameFile=c('/path/to/file1.avro','/another/path/to/file2.avro'))

#' @seealso \link[NEONprocIS.base]{def.rcd.miss.na}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-03-04)
#     original creation
##############################################################################################
def.rcd.fix.miss.na <-
  function(data,
           timeBad,
           valuBad = NA,
           log = NULL) {
    # initialize logging if necessary
    if (base::is.null(log)) {
      log <- NEONprocIS.base::def.log.init()
    }
    
    # For bad records that have timestamps in this data, fill the values with valuBad
    nameColMod <- base::names(data)
    setBad <- data$readout_time %in% timeBad$readout_time
    data[setBad, base::setdiff(nameColMod, 'readout_time')] <- valuBad
    log$debug(
      base::paste0(
        length(setBad),
        ' corrupt records turned to ',
        valuBad,
        ' in original file ',
        nameFileMod
      )
    )
    
    # For missing records, fill with valueBad
    timeAdd <-
      timeBad$readout_time[!(timeBad$readout_time %in% data$readout_time)]
    numAdd <- base::length(timeAdd)
    dfAdd <-
      as.data.frame(base::matrix(valuBad, nrow = numAdd, base::length(nameColMod)))
    base::names(dfAdd) <- nameColMod
    dfAdd$readout_time <- timeAdd
    data <- base::rbind(data, dfAdd)
    data <- data[base::order(data$readout_time), ]
    log$debug(base::paste0(numAdd, ' missing records added to original data in ', nameFileMod))
    
    return(data)
    
  }
