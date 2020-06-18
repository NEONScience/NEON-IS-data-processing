##############################################################################################
#' @title Perform spike test with optional spike removal

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Evaluate the spike test - whether a change in value from one observation
#' to the next in a timeseries exceeds a threshold based on the variability over a surrounding 
#' window of observations

#' @param data a numeric vector
#' @param Meth Character string. 
#' @param Mad Value
#' @param Wndw Value. Must be odd
#' @param WndwStep Value
#' @param NumPtsGrp Value
#' @param NaFracMax Value
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame the same size as data, with quality flag results of the despike test 
#' (0 = pass, 1 = fail, -1 = cannot eval)

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000783 - ATBD: Time Series Automatic Despiking for TIS Level 1 Data Products

#' @keywords despike, plausibility, QA/QC, quality

#' @examples
#' PUT SOME HERE

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-06-16)
#     original creation
##############################################################################################
def.dspk.mad <-
  function(data,
           Meth=c('A','B')[1],
           Mad,
           Wndw,
           WndwStep,
           NumPtsGrp,
           NaFracMax,
           log = NULL) {
    # initialize logging if necessary
    if (base::is.null(log)) {
      log <- NEONprocIS.base::def.log.init()
    }
    
    # Validate the data frame & numeric data only
    if (!NEONprocIS.base::def.validate.dataframe(dfIn = data,
                                                 TestNumc = TRUE,
                                                 log = log)) {
      stop()
    }
    
    
    # CHECK PARAMETERS
    # Meth only A or B
    # Wndw integer & odd for Method A
    # WndwStep only applied to method B and must be less than Wndw/2

    
    MultMad <- 1.4826 # Scale factor for MAD equal to reciprocal of the quantile function at probability of 75%
     <- 
    
    # Setup
    numData <- base::nrow(data)
    
    # Method A
    if (Meth == 'A'){
      
      # Run through each window, stepping forward 1 value each time, evaluating spike test for the central value
      for(idxBgn in 1:(numData-Wndw+1)){
        
        setMad <- idxBgn:(idxBgn+Wndw-1) # set of indices for the window
        idxMid <- idxBgn+(Wndw-1)/2 # mid point of the window
        dataWndw <- data[setMad]
        valuMid <- data[idxMid]
        
        # Get rid of NAs
        data <- data[!base::is.na(data)]
        numDataWndw <- base::length(data)
          
        # Calculate median absolute deviation over the window
        med <- stats::median(dataWndw)
        diffAbs <-base::abs(dataWndw-med)
        mad <- stats::median(diffAbs) # Median absolute deviation from median
        
        numObs <- sum()
        
        # Evaluate mad at central point
        mad > 
        
        
        
        
        
      }
    
      # Behavior at the edges (-1?? - yes, bc that's what timeseries padding does)
      
    } # End Method A
    
    
    
    return(qf)
  }
