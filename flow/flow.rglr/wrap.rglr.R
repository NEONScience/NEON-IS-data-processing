##############################################################################################
#' @title Regularization module for NEON IS data processing.

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description Wrapper function. Bin data to generate a regular time sequence of observations.
#' General code workflow:
#'      Error-check input parameters
#'      Read regularization frequency from location file if expected
#'      Create output directories and copy (by symbolic link) unmodified components
#'      Loop through all data files
#'        Regularize data in each file
#'        Write out the regularized data
#'
#'
#' @param DirIn Character value. The input path to the data from a single sensor or location, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/id, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The id is the unique identifier of the sensor or location. \cr
#'
#' Nested within this path are the folders:
#'         /data
#'         /location (only required if any ParaRglr$FreqRglr are NA)
#' The data folder holds a any number of data files to be regularized.
#' The location folder holds json files with the location data corresponding to the data files in the data
#' directory. The regularization frequency will be gathered from the location files for rows in ParaRglr where
#' ParaRglr$FreqRglr is NA. 
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#'
#' @param ParaRglr Data frame with minimum variables:\cr
#' \code{DirRglr} Character. The directory that is a direct child of DirIn where the data to be
#' regularized resides. Note that all indicated directories in the data frame must be present in DirIn.\cr
#' \code{SchmRglr} A json-formatted character string containing the schema for the regularized output. May be NA, 
#' in which case the schema will be created automatically from the output data frame with the same variable names 
#' as the input data frame.
#' \code{FreqRglr} Numeric value of the regularization frequency in Hz. May be NA, in which case the location file
#' mentioned in the DirIn parameter will be used to find the regularization frequency ("Data Rate" property). 
#' Note that a non-NA value for FreqRglr supercedes the data rate in the location file. \cr
#' \code{MethRglr} Character string indicating the regularization method (per the choices in
#' eddy4R.base::def.rglr for input parameter MethRglr)\cr
#' \code{WndwRglr} Character string indicating the windowing method (per the choices in
#' eddy4R.base::def.rglr for input parameter WndwRglr)\cr
#' \code{IdxWndw} Character string indicating the index allocation method (per the choices in
#' eddy4R.base::def.rglr for input parameter IdxWndw)\cr
#' \code{RptTimeWndw} Logical TRUE or FALSE (default) pertaining to the 
#' choices in eddy4R.base::def.rglr for input parameter RptTimeWndw. TRUE will output
#' two additional columns at the end of the output data file for the start and end times of the time windows
#' used in the regularization. Note that the output variable readout_time will be included in the output
#' regardless of the choice made here, and will probably match the start time of the bin unless 
#' MethRglr=CybiEcTimeMeas.\cr
#' \code{DropNotNumc} Logical TRUE (default) or FALSE pertaining to the 
#' choices in eddy4R.base::def.rglr for input parameter DropNotNumc. TRUE will drop
#' all non-numeric columns prior to the regularization (except for readout_time). Dropped columns will 
#' not be included in the output.\cr
#' 
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. not combined but carried through as-is).

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.

#' @return Regularized data output in Parquet format in DirOutBase, where DirOutBase directory
#' replaces BASE_REPO of DirIn but otherwise retains the child directory structure of the input path.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Not run
#' ParaRglr <- data.frame(DirRglr=c('data','flags'),
#'                        SchmRglr = c(NA,NA),
#'                        FreqRglr = c(0.5,0.5),
#'                        MethRglr = c('CybiEc','CybiEc'),
#'                        WndwRglr = c('Trlg','Trlg'),
#'                        IdxWndw = c('IdxWndwMin','IdxWndwMin'),
#'                        RptTimeWndw = c(FALSE,FALSE),
#'                        DropNotNumc = c(FALSE,FALSE),
#'                        stringsAsFactors=FALSE)
#' wrap.rglr(DirIn="~/pfs/relHumidity_calibrated_data/hmp155/2020/01/01/CFGLOC101252",
#'           DirOutBase="~/pfs/out",
#'           ParaRglr=ParaRglr
#' )

#' @seealso None currently

# changelog and author contributions / copyrights
#   Cove Sturtevant (2021-09-02)
#     Convert flow script to wrapper function
##############################################################################################
wrap.rglr <- function(DirIn,
                      DirOutBase,
                      ParaRglr,
                      DirSubCopy=NULL,
                      log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Gather info about the input directory (including date) and create the output directory.
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  timeBgn <-
    InfoDirIn$time # Earliest possible start date for the data
  timeEnd <- InfoDirIn$time + base::as.difftime(1, units = 'days')
  dirOut <- base::paste0(DirOutBase, InfoDirIn$dirRepo)
  dirLoc <- base::paste0(DirIn, '/location')
  fileLoc <- base::dir(dirLoc)
  numFileLoc <- base::length(fileLoc)
  
  # Error check - quit if we need locations and they aren't there
  expcLoc <- base::any(base::is.na(ParaRglr$FreqRglr)) # Do we need location info?
  if (expcLoc && numFileLoc == 0) {
    log$error(base::paste0('No location data found in ', dirLoc))
    stop()
  } else if (numFileLoc > 1) {
    fileLoc <- fileLoc[1]
    log$warn(base::paste0(
      'There is more than location file in ',
      dirLoc,
      '. Using ',
      fileLoc
    ))
  } 
  
  # Read regularization frequency from location file if expected
  if (expcLoc){
    # Grab the named location from the directory structure
    nameLoc <-
      utils::tail(InfoDirIn$dirSplt, 1) # Location identifier from directory path
    
    # Find the location we're looking for in the locations file
    nameFileLoc <- base::paste0(dirLoc, '/', fileLoc)
    locMeta <-
      NEONprocIS.base::def.loc.meta(
        NameFile = nameFileLoc,
        NameLoc = nameLoc,
        TimeBgn = timeBgn,
        TimeEnd = timeEnd,
        log = log
      )
    FreqRglrLoc <- base::as.numeric(locMeta$dataRate[1])
    
    # Error check
    if (base::is.na(FreqRglrLoc)) {
      log$error(
        base::paste0(
          'Cannot determine regularization frequency from location file for datum path ',
          DirIn
        )
      )
      stop()
    }
    
    log$debug(base::paste0('Regularization frequency: ',FreqRglrLoc, ' Hz read from location file'))
  }
  
  
  # Copy with a symbolic link the desired subfolders
  if (base::length(DirSubCopy) > 0) {
    NEONprocIS.base::def.dir.copy.symb(base::paste0(DirIn, '/', DirSubCopy), 
                                       dirOut, 
                                       log = log)
  }
  
  # Run through each directory to regularize
  for (idxDirRglr in ParaRglr$DirRglr) {
    # Row index to parameter set
    idxRowParaRglr <- base::which(ParaRglr$DirRglr == idxDirRglr)
    
    # Use regularization frequency from the location file if not in input args
    FreqRglrIdx <- ParaRglr$FreqRglr[idxRowParaRglr]
    if (base::is.na(FreqRglrIdx)) {
      # Use the frequency in the locations file instead
      FreqRglrIdx <- FreqRglrLoc
    }
    log$debug(base::paste0('Regularization frequency to be used for ',idxDirRglr,' directory: ',FreqRglrIdx, ' Hz'))
    
    
    # Get directory listing of input directory
    idxDirInRglr <-  base::paste0(DirIn, '/', idxDirRglr)
    fileData <- base::dir(idxDirInRglr)
    if (base::length(fileData) > 1) {
      log$warn(
        base::paste0(
          'There is more than one data file in path: ',
          DirIn,
          '... Regularizing them all!'
        )
      )
    }
    
    # Create output directory
    idxDirOutRglr <- base::paste0(dirOut, '/', idxDirRglr)
    base::dir.create(idxDirOutRglr, recursive = TRUE)
    
    # Regularize each file
    for (idxFileData in fileData) {
      # Load data
      fileIn <- base::paste0(idxDirInRglr, '/', idxFileData)
      data  <-
        base::try(NEONprocIS.base::def.read.parq(NameFile = fileIn,
                                                 log = log),
                  silent = FALSE)
      if (base::any(base::class(data) == 'try-error')) {
        log$error(base::paste0('File ', fileIn, ' is unreadable.'))
        stop()
      }
      nameVarIn <- base::names(data)
      
      # Pull out the time variable
      if (!('readout_time' %in% nameVarIn)) {
        log$error(
          base::paste0(
            'Variable "readout_time" is required, but cannot be found in file: ',
            fileIn
          )
        )
        stop()
      }
      
      # Regularize the data
      BgnRglr <- base::as.POSIXlt(timeBgn)
      EndRglr <- base::as.POSIXlt(timeEnd)
      idxTime <- base::which(nameVarIn == 'readout_time')
      dataRglr <-
        eddy4R.base::def.rglr(
          timeMeas = base::as.POSIXlt(data$readout_time),
          dataMeas = base::subset(data, select = -idxTime),
          BgnRglr = BgnRglr,
          EndRglr = EndRglr,
          FreqRglr = FreqRglrIdx,
          MethRglr = ParaRglr$MethRglr[idxRowParaRglr],
          WndwRglr = ParaRglr$WndwRglr[idxRowParaRglr],
          IdxWndw = ParaRglr$IdxWndw[idxRowParaRglr],
          DropNotNumc = ParaRglr$DropNotNumc[idxRowParaRglr],
          RptTimeWndw = ParaRglr$RptTimeWndw[idxRowParaRglr]
        )
      
      # Make sure we return the regularized data as the same class it came in with
      for (idxVarRglr in base::names(dataRglr$dataRglr)) {
        base::class(dataRglr$dataRglr[[idxVarRglr]]) <-
          base::class(data[[idxVarRglr]])
      }
      
      # Add the readout time back in, and potentially the bin start and end times
      rpt <-
        base::data.frame(readout_time = dataRglr$timeRglr,
                         stringsAsFactors = FALSE)
      rpt <- base::cbind(rpt, dataRglr$dataRglr)
      
      # Match the original column order (minus any variables we dropped)
      setColOrd <- base::match(nameVarIn,base::names(rpt))
      rpt <- rpt[,setColOrd[!is.na(setColOrd)]]
      
      # Tack on the time window start and end times to the end of the data frame
      if(ParaRglr$RptTimeWndw[idxRowParaRglr] == TRUE){
        rpt <- base::cbind(rpt, dataRglr$timeWndw)
      }
      
      # Remove any data points outside this day
      rpt <-
        rpt[rpt$readout_time >= BgnRglr & rpt$readout_time < EndRglr, ]
      
      # select output schema
      if (base::is.na(ParaRglr$SchmRglr[idxRowParaRglr])) {
        # use the output data to generate a schema
        idxSchmRglr <- base::attr(rpt, 'schema')
      } else {
        idxSchmRglr <- ParaRglr$SchmRglr[idxRowParaRglr]
      }
      
      # Write the output
      fileOut <- base::paste0(idxDirOutRglr, '/', idxFileData)
      rptWrte <-
        base::try(NEONprocIS.base::def.wrte.parq(
          data = rpt,
          NameFile = fileOut,
          NameFileSchm = NULL,
          Schm = idxSchmRglr
        ),
        silent = TRUE)
      if (base::class(rptWrte) == 'try-error') {
        log$error(base::paste0(
          'Cannot write regularized data in file ',
          fileOut,
          '. ',
          attr(rptWrte, "condition")
        ))
        stop()
      } else {
        log$info(base::paste0(
          'Regularized data written successfully in file: ',
          fileOut
        ))
      }
      
      
    } # End loop around files to regularize
  } # End loop around directories to regularize
}
