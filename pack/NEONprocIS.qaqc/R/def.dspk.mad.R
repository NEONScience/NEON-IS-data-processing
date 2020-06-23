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
#' @param ThshMad Value
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
           ThshMad,
           Wndw,
           WndwStep=1,
           NumGrp=4,
           NaFracMax=1,
           log = NULL) {
    # initialize logging if necessary
    if (base::is.null(log)) {
      log <- NEONprocIS.base::def.log.init()
    }
    
    # Validate numeric data only
    if (!NEONprocIS.base::def.validate.vector(vectIn = data,
                                              TestNumc = TRUE,
                                              log = log)) {
      stop()
    }
    
    
    # CHECK PARAMETERS
    # Meth only A or B
    # Wndw integer & odd for Method A
    # WndwStep only applied to method B and must be less than Wndw/2
    # NumGrp ,ust be > 1
    
    # Scale factor to use MAD as a consistent estimation of std
    # equal to reciprocal of the quantile function at probability of 75%
    CorStd <- 1.4826 
    
    # Setup
    numData <- base::length(data)
    dmmyVect <- rep(NA,numData)
    
    # Method A
    if (Meth == 'A'){
      
      numDataWndw<-dmmyVect
      med <- dmmyVect
      mad <- dmmyVect
      qf <- rep(-1,numData)
      
      # Run through each window, stepping forward 1 value each time, evaluating spike test for the central value
      for(idxBgn in 1:(numData-Wndw+1)){
      
        setMad <- idxBgn:(idxBgn+Wndw-1) # set of indices for the window
        idxMid <- idxBgn+(Wndw-1)/2 # mid point of the window
        dataWndw <- data[setMad]

        # Get rid of NAs
        #dataWndw <- dataWndw[!is.na(dataWndw)]
        numDataWndw[idxMid] <- sum(!is.na(dataWndw))
          
        # Calculate median absolute deviation over the window
        med[idxMid] <- median(dataWndw,na.rm=TRUE)
        diffAbs <-abs(dataWndw-med[idxMid])
        mad[idxMid] <- median(diffAbs,na.rm=TRUE) # Median absolute deviation from median
        
      }
      
      # Compute correction factor to reduce bias with low window size
      corWndw <- numDataWndw/(numDataWndw-0.8)
      if(base::min(numDataWndw,na.rm=TRUE) < 10){
        corWndw[numDataWndw == 9] <- 1.107
        corWndw[numDataWndw == 8] <- 1.129
        corWndw[numDataWndw == 7] <- 1.140
        corWndw[numDataWndw == 6] <- 1.200
        corWndw[numDataWndw == 5] <- 1.206
        corWndw[numDataWndw == 4] <- 1.363
      }
        
      # Adjust for window size, stardard deviation approx, and threshold factor
      madAdj <- mad*corWndw*CorStd*ThshMad 
      
      # Evaluate central point of each window for spike
      setEval <- numDataWndw >= 4 & numDataWndw >= (1-NaFracMax)*Wndw # center indices with enough non-NA data points in the window
      setSpkFail <- setEval & data < med-madAdj | data > med+madAdj
      setSpkPass <- setEval & !setSpkFail
      
      # Set the pass & fail flags (all others are -1)
      qf[setSpkFail] <- 1
      qf[setSpkPass] <- 0

      # Set a fail to a pass if there are more than NumGrp consective fails (suggesting a real data pattern)
      diff(qf,differences=NumGrp)
      
      qfGrp <- utils::head(qf,-(NumGrp-1))
      for(idx in 2:NumGrp){
       qfGrp <- qfGrp + qf[idx:(numData-NumGrp+idx)]
      }
      
      setFix <- which(qfGrp == NumGrp)
      for(idxFix in setFix){
        qf[idxFix:min(idxFix+NumGrp-1,numData)] <- 0
      }
      
    } # End Method A
    
    
    
    return(qf)
  }

# timeBgn <- Sys.time()
# X <- def.dspk.mad(data=data,Meth='A',ThshMad=7,Wndw=3601,WndwStep=1,NumGrp=4,NaFracMax=1,log=log)
# print(Sys.time()-timeBgn)