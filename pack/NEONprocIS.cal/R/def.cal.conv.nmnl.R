##############################################################################################
#' @title Convert nominal cal to calibrated data

#' @author
#' Kaelin Cawley \email{kcawley@battelleecology.org}

#' @description
#' Definition function. Apply NEON calibration CVALB1 to convert nominal data to calibrated data. 

#' @param data Data frame of nominally calibrated sensor readings. This data frame must have 
#' a column called "readout_time" with POSIXct timestamps
#' 
#' @param Meta A named list (default is an empty list) containing additional metadata to pass to 
#' calibration and uncertainty functions. For this function it should include nomVal and nomCalCoef.

#' @param varConv A character string of the target variables (columns) in the data frame \code{data} for 
#' which calibrated output will be computed (all other columns will be ignored). Defaults to the first
#' column in \code{data}.

#' @param calSlct A named list of data frames, each list element corresponding to a 
#' variable (column) to calibrate. The data frame in each list element holds 
#' information about the calibration files and time periods that apply to the variable, 
#' as returned from NEONprocIS.cal::def.cal.slct. See documentation for that function. 

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return The input data frame, with the columns specified in input \code{varConv} updated with 
#' calibrations applied.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Calibrated Measurements and Level 1 Data Products Uncertainty Budget Plan

#' @keywords Currently none

#' @examples
#' Not Run
#' data=data.frame(readout_time=as.POSIXct('2025-01-01','2025-01-02','2025-01-03'),var1=c(1,2,3),var2=c(4,5,6))
#' calSlct <- NEONprocIS.cal::wrap.cal.slct(
#'                DirCal = '/path/to/calibration/files',
#'                NameVarExpc = c('var1','var2'),
#'                TimeBgn = as.POSIXct('2025-01-01'),
#'                TimeEnd = as.POSIXct('2025-01-04'),
#'                )
#' dataCal <- def.cal.conv.nmnl(data=data,Meta=list(nomVal=c(15/90,355),nomCalCoef=c(CVAL_A1,CVAL_B1)),varConv=c('var1','var2'),calSlct=calSlct)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly.m}
#' @seealso \link[NEONprocIS.cal]{wrap.cal.conv.dp0p}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-07-28)
#     original creation, from def.cal.conv.poly
#   Kaelin Cawley (2026-02-06)
#     created new function to be used for RMyoung 05108 buoy wind speed data
#   Nora Catolico (2026-05-05)
#     updates to work with cal package
##############################################################################################
# # For Testing flow.cal.conv.R with this function
# setwd("/home/NEON/kcawley/NEON-IS-data-processing/flow/flow.cal.conv")
# # FileSchmData=$FILE_SCHEMA_DATA_AQUATROLL
# # FileSchmQf=$FILE_SCHEMA_FLAGS_AQUATROLL
# DirSubCopy=flags
# 
# Sys.setenv(DIR_IN='/scratch/pfs/rmyoung_calibration_group_and_convert_test')
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
# NumDayExpiMax = as.data.frame(matrix(data = c("speed",365), nrow = 1, ncol = 2))
# names(NumDayExpiMax) <- c('var','NumDayExpiMax')
# NumDayExpiMax$NumDayExpiMax <- as.numeric(NumDayExpiMax$NumDayExpiMax)
# arg <- c(DirIn=$DIR_IN, 
#          DirOut="/pfs/out",
#          dirErr="/pfs/out/errored_datums",
#          ConvFuncTerm1=def.cal.conv.nmnl:speed, 
#          NumDayExpiMax = NumDayExpiMax, 
#          UcrtFuncTerm1=def.ucrt.meas.cnst:speed)
# # Then copy and paste rest of workflow into the command window
# 
# 
# # For Testing speed calibration
# data = NEONprocIS.base::def.read.parq(NameFile = '~/pfs/rmyoung_data_source_trino/rmyoung/2025/12/14/32356/data/rmyoung_32356_2025-12-14.parquet')
# Meta = list(c("term=speed", "nomVal=15/90", "nomCalCoef=CVAL_B1"),
#            c("term=direction", "nomVal=355", "nomCalCoef=CVAL_A1"))
# varConv = base::names(data)[4] #speed
# 
# calSlct <- NEONprocIS.cal::wrap.cal.slct(
#                DirCal = '~Git/pfs/rmyoung_calibration_group_and_convert_test/rmyoung/2025/12/14/32356/calibration',
#                NameVarExpc = c('speed'),
#                TimeBgn = as.POSIXct('2025-12-13'),
#                TimeEnd = as.POSIXct('2025-12-15'),
#                NumDayExpiMax = NumDayExpiMax
#                )
# log = NULL
##############################################################################################
def.cal.conv.nmnl <- function(data = data.frame(data=base::numeric(0)),
                                varConv = base::names(data)[1],
                                calSlct = NULL,
                                Meta=list(),
                                log = NULL) {
  # Intialize logging if needed
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Ensure input is data frame with variables to be calibrated
  chk <- NEONprocIS.base::def.validate.dataframe(dfIn=data,TestNameCol=c(varConv,'readout_time'),TestEmpty=FALSE, log = log)
  if (!chk) {
    stop()
  }
  
  # Basic starting info
  timeMeas <- data$readout_time
  
  if(!("POSIXt" %in% base::class(timeMeas))){
    log$error('Variable readout_time must be of class POSIXt')
    stop()
  }
  
  # Run through the variable to be calibrated
  # Check to see if data to be calibrated is a numeric array
  chk <-
    NEONprocIS.base::def.validate.vector(data[[varConv]], TestEmpty = FALSE, TestNumc = TRUE, log = log)
  if (!chk) {
    stop()
  }
  
  # Pull cal file info for this variable and initialize the output
  calSlctIdx <- calSlct[[varConv]]
  dataConvIdx <- data[[varConv]]
  dataConvOutIdx <- as.numeric(NA)*dataConvIdx
  
  # Skip calibration if no cal info supplied
  if(base::is.null(calSlctIdx)){
    log$warn(base::paste0('No applicable calibration files available for ',varConv, '. Returning NA for calibrated output.'))
    calSlctIdx <- base::data.frame()
  }

  # Parse Meta into term-specific nominal values and calibration coefficients.
  # Expected serialized form is Meta="term:nomVal,nomCalCoef|...".
  # Legacy vector values c(term, nomVal, nomCalCoef) are also supported.
  parseMetaEntry <- function(termName, entryVal) {
    if (base::is.atomic(entryVal) && base::length(entryVal) >= 3) {
      return(base::list(term = as.character(entryVal[1]),
                        nomVal = as.character(entryVal[2]),
                        nomCalCoef = as.character(entryVal[3])))
    }
    if (base::is.atomic(entryVal) && base::length(entryVal) == 1) {
      part <- as.character(entryVal)
      # If no explicit term prefix is present, fall back to list element name.
      if (base::grepl(':', part, fixed = TRUE)) {
        splitTerm <- base::strsplit(part, ':', fixed = TRUE)[[1]]
        if (base::length(splitTerm) != 2) {
          return(NULL)
        }
        termParsed <- splitTerm[1]
        coefPart <- splitTerm[2]
      } else {
        termParsed <- termName
        coefPart <- part
      }
      splitCoef <- base::strsplit(coefPart, ',', fixed = TRUE)[[1]]
      if (base::length(splitCoef) != 2) {
        return(NULL)
      }
      return(base::list(term = termParsed,
                        nomVal = splitCoef[1],
                        nomCalCoef = splitCoef[2]))
    }
    NULL
  }

  metaParsed <- base::lapply(base::names(Meta), function(nm) parseMetaEntry(nm, Meta[[nm]]))
  metaParsed <- metaParsed[!base::vapply(metaParsed, base::is.null, logical(1))]
  if (base::length(metaParsed) == 0) {
    log$error('Meta must contain term-specific nominal calibration info (e.g. Meta=speed:0.1666667,CVAL_B1|direction:355,CVAL_A1).')
    stop()
  }
  metaDf <- base::do.call(base::rbind, base::lapply(metaParsed, base::as.data.frame, stringsAsFactors = FALSE))

  #retrieve appropriate nominal value and cal coef where term matches varConv
  nomValIdx <- as.numeric(metaDf$nomVal[metaDf$term == varConv][1])
  nomCalCoefIdx <- as.character(metaDf$nomCalCoef[metaDf$term == varConv][1])
  if (base::is.na(nomValIdx) || base::is.na(nomCalCoefIdx) || base::identical(nomCalCoefIdx, "NA")) {
    log$error(base::paste0('Meta is missing nominal value or nominal calibration coefficient for term ', varConv, '.'))
    stop()
  }
  log$debug(base::paste0('Nominal value for ',varConv,': ',nomValIdx))
  log$debug(base::paste0('Nominal calibration coefficient for ',varConv,': ',nomCalCoefIdx))
  
  # Run through each calibration file and apply the calibration function for the applicable time period
  for(idxRow in base::seq_len(base::nrow(calSlctIdx))){
    
    # What records in the data correspond to this cal file?
    setCal <- timeMeas >= calSlctIdx$timeBgn[idxRow] & timeMeas < calSlctIdx$timeEnd[idxRow]
    
    # If a calibration file is available for this period, open it and get calibration information
    if(!base::is.na(calSlctIdx$file[idxRow])){
      fileCal <- base::paste0(calSlctIdx$path[idxRow],calSlctIdx$file[idxRow])
      infoCal <- NEONprocIS.cal::def.read.cal.xml(NameFile=fileCal,Vrbs=TRUE,log=log)
    } else {
      infoCal <- NULL
    }
    
    # If infoCal is NULL, return NA data
    if (is.null(infoCal)) {
      dataConvOutIdx[setCal] <- as.numeric(NA)
      next
    }
    
    # Remove the nominal value only for records covered by this calibration file
    dataConvOutIdx[setCal] <- data[[varConv]][setCal]/nomValIdx
    
    # Apply the value associated with the nomCalCoef only to this calibration period
    dataConvOutIdx[setCal] <- dataConvOutIdx[setCal] *
      as.numeric(infoCal$cal$Value[infoCal$cal$Name==nomCalCoefIdx])
    
  } # End loop around calibration files
  
  # Add calibrated data and retain raw data
  currNames <- names(data)
  nameToAdd <- paste0(varConv,"Calibrated")
  
  data[[ncol(data)+1]] <- NA
  names(data) <- c(currNames,nameToAdd)
  
  data[[nameToAdd]] <- dataConvOutIdx
  
  return(data)
  
}
