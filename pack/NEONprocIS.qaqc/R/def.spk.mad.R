##############################################################################################
#' @title Perform spike test using the median absolute deviation method

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Evaluate the spike test - whether an observation lies outside the median 
#' plus/minus a multiple of the median absolute deviation computed over a surrounding window of 
#' observations.

#' @param data a numeric vector of observations
#' @param Meth Character value of either 'A' or 'B'. Method A evaluates the central point in 
#' each window as the window slides along the timeseries. Method B evaluates all points within
#' each window as it slides along the timeseries by WndwStep each time, thus evaluating a point 
#' multiple times. For this method, a point must be identified as a spike at least the fraction 
#' of the time specified in WndwFracSpkMin to be flagged as a spike. Default is A.
#' @param ThshMad The threshold multiple of the median absolute deviation of the median (MAD) for 
#' each window outside which a value must lie to be identified as a spike. Default is 7.
#' @param Wndw Integer window size for calculating MAD. Must be odd. If even, 1 will be added. 
#' If adding 1 makes Wndw longer than the length of the data, 1 will be subtracted.
#' @param WndwStep Method B only. An integer value specifying the number of data points to slide 
#' the window as it moves along the timeseries. Default is 1. Maximum is 1/2 the window size.
#' @param WndwFracSpkMin Method B only. Minimum threshold fraction of windows a point must fail 
#' the spike test to be flagged as a spike.
#' @param NumGrp Either NULL or an integer value specifying the minimum number of consective spikes,
#'  at and above which the points will be deemed plausible values and not flagged. 
#' Defaults to NULL, which will result in no evaluation of consecutive spikes. Default is NULL.
#' @param NaFracMax Fraction between 0 and 1 specifying the maximum fraction of missing values 
#' for which each window retains a sufficient sample size to determine the median absolute deviation.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return An integer vector the same length as data, with quality flag results of the spike test 
#' (0 = pass, 1 = fail, -1 = cannot eval)

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000783 - ATBD: Time Series Automatic Despiking for TIS Level 1 Data Products

#' @keywords despike, plausibility, QA/QC, quality

#' @examples
#' data <- c(1,2,3,4,3,2,1,2,3,4,50,3,2,1,2,3,4,3,2,1)
#' # Method A
#' qfSpk <- def.spk.mad(data=data,Meth='A',ThshMad=7,Wndw=5)
#' 
#' # Method B
#' qfSpk <- def.spk.mad(data=data,Meth='B',ThshMad=7,Wndw=5,WndwStep=3,WndwFracSpkMin=0.5)

#' @seealso \link[eddy4R.qaqc]{def.dspk.wndw}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-07-09)
#     original creation
#   Cove Sturtevant (2022-08-16)
#     improve speed when dataset has a large proportion of missing values
##############################################################################################
def.spk.mad <-
  function(data,
           Meth=c('A','B')[1],
           ThshMad=7,
           Wndw,
           WndwStep=1,
           WndwFracSpkMin=0.1,
           NumGrp=NULL,
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
    
    numData <- base::length(data)
    
    # Check input parameters
    if(base::length(Meth) != 1 || !(Meth %in% c('A','B'))){
      log$fatal('Input argument Meth must be either "A" or "B"')
      stop()
    }
    if(base::length(ThshMad) != 1 || !base::is.numeric(ThshMad)){
      log$fatal('Input argument ThshMad must be a single numeric value')
      stop()
    }
    if(base::length(Wndw) != 1 || base::as.integer(Wndw) != Wndw || Wndw > numData){
      log$fatal('Input argument Wndw must be a single integer value')
      stop()
    }
    if(Wndw %% 2 == 0){
      if(Wndw != numData){
        Wndw <- Wndw + 1
        log$info(base::paste0('Spike test window increased by 1 to ',Wndw, ' in order to make it odd'))
      } else {
        Wndw <- Wndw - 1
        log$info(base::paste0('Spike test window decreased by 1 to ',Wndw, ' in order to make it odd'))
      }
    }
    if(base::length(WndwStep) != 1 || base::as.integer(WndwStep) != WndwStep || WndwStep < 1 || WndwStep > base::floor(Wndw/2)){
      log$fatal('Input argument WndwStep must be a single integer value >= 1 and <= Wndw/2')
      stop()
    }
    if(base::length(WndwFracSpkMin) != 1 || WndwFracSpkMin < 0 || WndwFracSpkMin > 1){
      log$fatal('Input argument WndwFracSpkMin must be a single value between 0 and 1')
      stop()
    }
    if(!is.null(NumGrp) && (base::length(NumGrp) != 1 || base::as.integer(NumGrp) != NumGrp || NumGrp < 1 || NumGrp > numData)){
      log$fatal('Input argument NumGrp must either be NULL or a single integer value >= 1 and <= length of the data')
      stop()
    }
    if(base::length(NaFracMax) != 1 || NaFracMax < 0 || NaFracMax > 1){
      log$fatal('Input argument NaFracMax must be a single value between 0 and 1')
      stop()
    }
    

    # Scale factor to use MAD as a consistent estimation of std
    # equal to reciprocal of the quantile function at probability of 75%
    CorStd <- 1.4826 
    
    # Setup
    dmmyVect <- rep(NA,numData)
    numDataWndw<-dmmyVect
    med <- dmmyVect
    mad <- dmmyVect
    qf <- rep(-1,numData)
    
    
    # Let's skip everything if all the data are NA
    if(base::sum(base::is.na(data))==numData){
      return(as.integer(qf))
    }
    
    # To use the running median and mad functions below, we must first handle NA values. 
    # Later iterations of the native R stats package handle NAs, but the 3.6.0 version does not.
    # Uncomment the following and replace the med variable if running on 3.6.0
    
    # # Leaving the NAs in there causes some extra qf=-1 when the NA value is the first value in the window. 
    # # Let's replicate the "+Big_alternate" method of stats v3.6.2, which replaces NA values with alternating very large
    # # numbers so that the median is unaffected.
    # numBig <- .Machine$double.xmax/3
    # setNa <- which(is.na(data))
    # numBigAlt <- array(c(numBig,-numBig),length(setNa))
    # dataPrepNa <- data
    # dataPrepNa[setNa] <- numBigAlt
    # # Compute the running median of the data 
    # med <- stats::runmed(x=dataPrepNa,k=Wndw,endrule='constant')
    med <- stats::runmed(x=data,k=Wndw,endrule='constant',na.action="+Big_alternate") # window is centered

    # The caTools::runmad function is ultra fast when there are no missing values, but the 
    # performance degrades rapidly with increasing proportion of NA, especially when they are
    # in large continuous chunks. If the proportion of NAs exceeds 15% we will use a different 
    # function. Performance of both methods scales ~linearly with data length. More could be done 
    # here to tailor which method is chosen according to gap distribution.
    dataReal <- !base::is.na(data)
    numDataReal <- base::sum(dataReal)
    
    if(numDataReal/numData >= 0.85){
      mad <- caTools::runmad(x=data,k=Wndw,constant=CorStd,endrule="NA",align='center',center=med)
    } else {
      mad <- data.table::frollapply(x=data,n=Wndw,FUN=stats::mad,na.rm=TRUE,constant=CorStd,fill=NA,align='center')
    }

    numDataWndw[((Wndw-1)/2+1):(numData-(Wndw-1)/2)] <- RcppRoll::roll_sum(x=dataReal,n=Wndw,by=1,align='center',na.rm=TRUE)
    
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
    if(!is.null(NumGrp)){
      qfGrp <- utils::head(qf,-(NumGrp-1))
      for(idx in 2:NumGrp){
       qfGrp <- qfGrp + qf[idx:(numData-NumGrp+idx)]
      }
      
      setFix <- which(qfGrp == NumGrp)
      for(idxFix in setFix){
        qf[idxFix:(idxFix+NumGrp-1)] <- 0
      }
      
    }
  
    return(as.integer(qf))
  }
