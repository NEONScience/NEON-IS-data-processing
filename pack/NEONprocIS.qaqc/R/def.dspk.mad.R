##############################################################################################
#' @title Perform spike test

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
#' @param WndwFracSpkMin Method B only. Minimum threshold fraction of windows a point must fail the spike test as the window steps through the timeseries to be marked as a spike
#' @param NumPtsGrp Value
#' @param NaFracMax Value
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return An integer vector the same length as data, with quality flag results of the spike test 
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
def.spk.mad <-
  function(data,
           Meth=c('A','B')[1],
           ThshMad,
           Wndw,
           WndwStep=1,
           WndwFracSpkMin=0.1,
           NumGrp=4,
           NaFracMax=.1,
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
    # Wndw integer & odd for Method A (and B the way it's being implemented) (just add 1)
    # WndwStep only applied to method B and must be less than Wndw/2
    # NumGrp must be > 1
    
    # Scale factor to use MAD as a consistent estimation of std
    # equal to reciprocal of the quantile function at probability of 75%
    CorStd <- 1.4826 
    
    # Setup
    numData <- base::length(data)
    dmmyVect <- rep(NA,numData)
    numDataWndw<-dmmyVect
    med <- dmmyVect
    mad <- dmmyVect
    qf <- rep(-1,numData)
    
    # To use the running median and mad functions below, we must first handle NA values. 
    # It appears that later iterations of the native R stats package may handle NAs, but the 3.6.0 version does not.
    # Let's replicate the "+Big_alternate" method of stats v3.6.2
    # The tradeoff here is that the faster med and mad algorithms change the stats a bit when there are NAs in the window.
    # Leaving the NAs in there causes some extra qf=-1 when the NA value is the first value in the window. Maybe we can 
    # replace the NA/Inf values in these cases with the next non-NA values in the timeseries.
    numBig <- .Machine$double.xmax/3
    setNa <- which(is.na(data))
    numBigAlt <- array(c(numBig,-numBig),length(setNa))
    dataPrepNa <- data
    dataPrepNa[setNa] <- numBigAlt 
    
    # Compute the running median of the data (WAY faster than running it in the for loop)
    med <- stats::runmed(x=data,k=Wndw,endrule='constant')
    mad <- caTools::runmad(x=data,k=Wndw,constant=CorStd,endrule="NA",align='center')
    numDataWndw[((Wndw-1)/2+1):(numData-(Wndw-1)/2)] <- RcppRoll::roll_sum(x=!is.na(data),n=Wndw,by=1,align='center',na.rm=TRUE)
    
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
    madAdj <- mad*corWndw*ThshMad 
    
    # Determine the windows that have sufficient sample size to determine mad (indicated at index of central value)
    setEval <- numDataWndw >= 4 & numDataWndw >= (1-NaFracMax)*Wndw # center indices with enough non-NA data points in the window
    
    # Evaluate spike test depending on chosen method
    if (Meth == 'A'){
      
      # Evaluate central point of each window for spike
      setSpkFail <- setEval & (data < med-madAdj | data > med+madAdj)
      setSpkPass <- setEval & !setSpkFail
      
      # Set the pass & fail flags (all others are -1)
      qf[setSpkFail] <- 1
      qf[setSpkPass] <- 0

    } else if (Meth == 'B'){
      
      # Step through the windows at interval WndwStep, assessing all points in the window
      numFail <- rep(0,numData)
      numTest <- rep(0,numData)
      for (idxBgn in seq(from=1,to=numData-Wndw+1,by=WndwStep)){
        
        # Indices of the window
        setWndw <- idxBgn:(idxBgn+Wndw-1)
        idxMid <- idxBgn + (Wndw-1)/2
        
        # Evaluate the whole window, adding to the running count of times a data point has failed the spike test for each of the windows that pass across it
        setSpkFailIdx <- setEval[idxMid] & abs(data[setWndw]-med[idxMid]) > madAdj[idxMid]
        numFail[setWndw] <- numFail[setWndw] + setSpkFailIdx
        numTest[setWndw] <- numTest[setWndw] + setEval[idxMid]
        
      }
      
      # Evaluate whether the counts of spike failures for each value surpass the threshold percentage
      setSpkFail <- numFail/numTest >= WndwFracSpkMin # numTest = 0 results in NA
      setSpkPass <- !setSpkFail
      
      qf[setSpkFail] <- 1 # NA values ignored (retained as -1)
      qf[setSpkPass] <- 0 # NA values ignored (retained as -1)
      
    }
    
    # Set a fail to a pass if there are more than NumGrp consective fails (suggesting a real data pattern as opposed to spikes)
    qfGrp <- utils::head(qf,-(NumGrp-1))
    for(idx in 2:NumGrp){
     qfGrp <- qfGrp + qf[idx:(numData-NumGrp+idx)]
    }
    
    setFix <- which(qfGrp == NumGrp)
    for(idxFix in setFix){
      qf[idxFix:(idxFix+NumGrp-1)] <- 0
    }
  
    return(as.integer(qf))
  }

timeBgn <- Sys.time()
qfEddy4R <- base::do.call(eddy4R.qaqc::def.dspk.wndw, argsSpk)$qfSpk
print(Sys.time()-timeBgn)

timeBgn <- Sys.time()
# profvis::profvis({qf <- def.spk.mad(data=data,Meth='A',ThshMad=7,Wndw=3601,WndwStep=1,NumGrp=4,NaFracMax=1,log=log)})
qf <- def.spk.mad(data=data,Meth='A',ThshMad=1,Wndw=1801,WndwStep=1,WndwFracSpkMin=0.1,NumGrp=4,NaFracMax=.1,log=log)
print(Sys.time()-timeBgn)
