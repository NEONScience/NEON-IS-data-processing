library(dplyr)

# NOTE: testScriptCove must be (re)run before each run of this script


# Add in artificial rain
numData <- nrow(strainGaugeDepthAgr)
precipAdd <- rep(0,numData)
timeAdd <- as.POSIXct("2022-10-12 12:00:00",tz='GMT')
# timeAdd <- as.POSIXct("2022-10-12 12:00:00",tz='GMT')
timeAdd <- as.POSIXct("2022-10-12 12:00:00",tz='GMT') + as.difftime(c(-5:5),units='hours')
idxAdd <- strainGaugeDepthAgr$startDateTime %in% timeAdd # logical indices of time points to add rain
precipAdd[idxAdd] <- .2 # Add this amount of precip at the time points indicated
benchAdd <- cumsum(precipAdd)
# Add precip
# strainGaugeDepthAgr$bench <- strainGaugeDepthAgr$bench[1] # Reset bench to start (no rain)
# strainGaugeDepthAgr$strainGaugeDepth = strainGaugeDepthAgr$strainGaugeDepth + benchAdd
# strainGaugeDepthAgr$bench <- strainGaugeDepthAgr$bench+benchAdd



# Iterated amplitude-adjusted Fourier Transform surrogates
# These surrogates
# are supposed to have the same amplitude distribution (marginal cdf) 
# and autocorrelation as the given time series 'xV'. 
# The IAAFT algorithm is proposed in 
# Schreiber, T. and Schmitz, A. (1996) "Improved Surrogate Data for 
# Nonlinearity Tests", Physical Review Letters, Vol 77, 635-638.
# The IAAFT is an improvement of the AAFT. Iteratively, it fits the 
# amplitudes and at each step improves the spectral phases and then 
# reorders the derived time series at each step until convergence of 
# both spectral density and amplitude distribution is reached. 
# The algorithm terminates if complete convergence (same reordering in 
# two consecutive steps) is succeeded or if the 'maxi' number of 
# iterations is reached. 
# surr <- iaaft(x=strainGaugeDepthAgr$strainGaugeDepth)
depthMinusBench <- strainGaugeDepthAgr$strainGaugeDepth - strainGaugeDepthAgr$bench
nSurr <- 10

# Remove all NA
setNotNa <- !is.na(depthMinusBench)
surrFill <- iaaft(x=depthMinusBench[setNotNa],N=nSurr)
surr <- matrix(NA,nrow=numData,ncol=nSurr)
surr[setNotNa,] <- surrFill

# Pre-allocate additional variables
nameVarBenchS <- paste0('benchS',seq_len(nSurr))
nameVarDepthS <- paste0('strainGaugeDepthS',seq_len(nSurr))
nameVarPrecipS <- paste0('precipS',seq_len(nSurr))
nameVarPrecipTypeS <- paste0('precipTypeS',seq_len(nSurr))
nameVarPrecipBulkS <- paste0('precipBulkS',seq_len(nSurr))
strainGaugeDepthAgr[,nameVarBenchS] <- as.numeric(NA)
strainGaugeDepthAgr[,nameVarDepthS] <- strainGaugeDepthAgr$bench + surr    # Add the surrogates to the benchmark
strainGaugeDepthAgr[,c(nameVarPrecipS)] <- FALSE
strainGaugeDepthAgr[,nameVarPrecipTypeS] <- as.character(NA)
strainGaugeDepthAgr[,nameVarPrecipBulkS] <- as.numeric(NA)

# Testing performance
strainGaugeDepthAgr$benchTest <- as.numeric(NA)
strainGaugeDepthAgr$precipTest <- FALSE
strainGaugeDepthAgr$precipTypeTest <- as.character(NA)

# Take the benchmark and add on the surrogate variability
for(idxSurr in c(0,seq_len(nSurr))){
# for(idxSurr in c(0)){

  if (idxSurr == 0){
    message(paste0('Rerunning original timeseries with synthetic precip'))
    nameVarDepth <- 'strainGaugeDepth'
    nameVarBench <- 'bench'
    nameVarPrecip <- 'precip'
    nameVarPrecipType <- 'precipType'
    nameVarPrecipBulk <- 'precipBulk'
      
    strainGaugeDepthS <- strainGaugeDepthAgr$strainGaugeDepth
    
  } else {
    message(paste0('Running Surrogate ',idxSurr))
    nameVarDepth <- paste0('strainGaugeDepthS',idxSurr)
    nameVarBench <- paste0('benchS',idxSurr)
    nameVarPrecip <- paste0('precipS',idxSurr)
    nameVarPrecipType <- paste0('precipTypeS',idxSurr)
    nameVarPrecipBulk <- paste0('precipBulkS',idxSurr)
    
    strainGaugeDepthS <- strainGaugeDepthAgr[[nameVarDepth]]
    
  }  
  

  #start counters
  rawCount <- 0
  timeSincePrecip <- NA
  currRow <- rangeSize #instead of 24 for hourly this will be how ever many rows encompass one day
  
  #initialize fields. 
  strainGaugeDepthAgr[[nameVarBench]] <- NA
  strainGaugeDepthAgr[[nameVarBench]][1:currRow] <-  stats::quantile(strainGaugeDepthS[1:currRow],Quant,na.rm=TRUE)
  
  # Use standalone variables when running through the loop for speed
  varBench <- strainGaugeDepthAgr[[nameVarBench]]
  varPrecip <- strainGaugeDepthAgr[[nameVarPrecip]]
  varPrecipType <- strainGaugeDepthAgr[[nameVarPrecipType]]
    
  ##loop through data to establish benchmarks
  skipping <- FALSE
  numRow <- nrow(strainGaugeDepthAgr)
  for (i in 1:numRow){
    
    # if(i == 189){stop()}
    
    # Check for at least 1/2 a day of non-NA values.
    # If not, get to the next point at which we have at least 1/2 day and start fresh
    if(base::sum(base::is.na(strainGaugeDepthS[i:currRow])) > .5*rangeSize){
      
      # Find the last non-NA value
      setEval <- i:currRow
      idxEnd <- setEval[tail(which(!is.na(strainGaugeDepthS[setEval])),1)]
      
      if(length(idxEnd) > 0){
        # Remove the benchmark extending into the gap
        varBench[(idxEnd+1):currRow] <- NA
      }
      
      # Skip until there is enough data
      skipping <- TRUE
      currRow <- currRow + 1
      
      # stop at end of data frame
      if (currRow == numRow){
        break()
      } else {
        next
      }
      
    } else if (skipping) {
      
      # Find the first non-NA value to begin at
      setEval <- i:currRow
      idxBgnNext <- setEval[head(which(!is.na(strainGaugeDepthS[setEval])),1)]
      
      # Re-establish the benchmark
      varBench[idxBgnNext:currRow] <- stats::quantile(strainGaugeDepthS[idxBgnNext:currRow],Quant,na.rm=TRUE)
      timeSincePrecip <- NA
      skipping <- FALSE
    }
    
    
    if(!is.na(timeSincePrecip)){
      timeSincePrecip <- timeSincePrecip + 1
    }
    
    # #establish nth min/max of range
    recentPrecip <- base::any(varPrecip[i:currRow])
    
    
    #establish benchmark
    bench <- varBench[currRow-1]
    raw <- strainGaugeDepthS[currRow]
    precip <- FALSE
    precipType <- NA
    
    # missing data handling
    # !!!! THIS NEEDS ATTENTION. If the initial benchmark creation was NA then this may still be a problem.
    # If the current depth value is NA, set it to the benchmark
    raw <- ifelse(is.na(raw), bench, raw)
    
    #how many measurements in a row has raw been >= bench?
    if (raw >= bench){
      rawCount <- rawCount + 1
    }else{
      rawCount <- 0
    }
    
    # Compute median over last range size (i.e. 1 day)
    # !!! Write a note about what we're going to use this for. !!!
    raw_med_lastDay <- quantile(strainGaugeDepthS[i:currRow],Quant,na.rm=TRUE)
    raw_min_lastDay <- min(strainGaugeDepthS[i:currRow],na.rm=TRUE)
    
    
    # if precip total increased check to if any precip triggers are reached
    if (raw > bench){
      rawChange <- raw - bench
      
      if ((rawChange > (ChangeFactor * Envelope) & rawChange > ThshChange )){
        # If change was bigger than 90% of diel range of noise in the data and also
        #   greater than the expected instrument sensitivity to rain, then it rained!
        bench <- raw # Update the benchmark for the next data point
        precip <- TRUE
        timeSincePrecip <- 0
        precipType <- 'volumeThresh'
        # SHOULD WE RESET THE rawCount HERE???
        # rawCount <- 0
        
      } else if (grepl('volumeThresh',x=varPrecipType[currRow-1]) && rawChange > ThshChange){
        # Or, if is has been raining with the volume threshold and the precip depth continues to increase
        #   above the expected instrument sensitivity, continue to say it is raining.
        bench <- raw # Update the benchmark for the next data point
        precip <- TRUE
        timeSincePrecip <- 0
        precipType <- 'volumeThreshContinued'
        # SHOULD WE RESET THE rawCount HERE???
        # rawCount <- 0
        
      } else if (rawCount == ThshCount){
        
        # Or, if the precip depth has been above the benchmark for exactly the time threshold
        #   considered for drizzle (ThshCount), say that it rained (i.e. drizzle), and
        #   continue to count.
        bench <- raw # Update the benchmark for the next data point
        precip <- TRUE
        timeSincePrecip <- 0
        precipType <- 'ThshCount'
        
        # Now go back to the start of the drizzle and set the bench to increasing
        #   raw values and continue to count.
        benchNext <- bench
        for (idx in (currRow-1):(currRow-ThshCount+2)) {
          rawIdx <- strainGaugeDepthS[idx]
          rawIdx <- ifelse(is.na(rawIdx), benchNext, rawIdx)
          if(rawIdx < benchNext){
            benchNext <- rawIdx
            varBench[idx] <- rawIdx
          } else {
            varBench[idx] <- benchNext
          }
          
          # Record rain stats
          varPrecip[idx] <- precip
          varPrecipType[idx] <- 'ThshCountBackFilledToStart'
        }
        
      } else if (rawCount >= ThshCount){
        # Or, if it continues to drizzle and raw is continuing to rise, keep saying that it's raining
        bench <- raw
        precip <- TRUE
        timeSincePrecip <- 0
        precipType <- 'ThshCount'
      }
      # } else if (!is.na(timeSincePrecip) && timeSincePrecip == rangeSize && raw > (strainGaugeDepthAgr$bench[i-1]-Recharge)){  # Maybe use Envelope instead of Recharge?
    } 
    if (!is.na(timeSincePrecip) && timeSincePrecip == rangeSize && raw > (varBench[i-1]-Recharge)){  # Maybe use Envelope instead of Recharge?
      
      # Exactly one day after rain ends, and if the depth hasn't dropped precipitously (as defined by the Recharge threshold),
      # back-adjust the benchmark to the median of the last day to avoid overestimating actual precip
      
      bench <- raw_med_lastDay
      varBench[i:currRow] <- bench
      varPrecipType[i:currRow] <- "postPrecipAdjToMedNextDay"
      
      idxBgn <- i-1
      keepGoing <- TRUE
      while(keepGoing == TRUE) {
        
        if(is.na(varPrecip[idxBgn]) || varPrecip[idxBgn] == FALSE){
          # Stop if we are past the point where the precip started
          keepGoing <- FALSE
        } else if(varBench[idxBgn] > bench){
          varBench[idxBgn] <- bench
          varPrecipType[idxBgn] <- paste0(varPrecipType[idxBgn],"BackAdjToMedNextDay")
          idxBgn <- idxBgn - 1
        } else {
          keepGoing <- FALSE
        }
      }
    } else if ((raw < bench) && (bench-raw_med_lastDay) > ChangeFactorEvap*Envelope && !recentPrecip){
      # If it hasn't rained in at least 1 day, check for evaporation & reset benchmark if necessary
      # bench <- raw_med_lastDay
      bench <- raw_min_lastDay
      precipType <- 'EvapAdj'

    } else if ((bench - raw) > Recharge){
      # If the raw depth has dropped precipitously (as defined by the recharge rage), assume bucket was emptied. Reset benchmark.
      bench <- raw
      
      # Get rid of a couple hours before the recharge. This is when calibrations are occuring and strain gauges are being replaced.
      # Set the benchmark constant to the point 2 hours before the recharge
      setAdj <- strainGaugeDepthAgr$startDateTime > (strainGaugeDepthAgr$startDateTime[currRow] -as.difftime(3,units='hours')) &
        strainGaugeDepthAgr$startDateTime < strainGaugeDepthAgr$startDateTime[currRow]
      idxSet <- head(which(setAdj),1) - 1
      if (idxSet < 1){
        varBench[setAdj] <- NA
        varPrecip[setAdj] <- NA
        varPrecipType[setAdj] <- NA
      } else {
        varBench[setAdj] <- varBench[idxSet]
        varPrecip[setAdj] <- varPrecip[idxSet]
        varPrecipType[setAdj] <- "ExcludeBeforeRecharge"
      }
      
    }
    
    #update in the data
    varBench[currRow] <- bench
    varPrecip[currRow] <- precip
    varPrecipType[currRow] <- precipType

    #move to next row
    currRow <- currRow + 1
    
    #stop at end of data frame
    if (currRow == nrow(strainGaugeDepthAgr)){
      varBench[currRow] <- bench
      break()
    }
  }
  
  # Reassign outputs
  strainGaugeDepthAgr[[nameVarBench]] <- varBench
  strainGaugeDepthAgr[[nameVarPrecip]] <- varPrecip 
  strainGaugeDepthAgr[[nameVarPrecipType]] <- varPrecipType
  
  # Compute precip
  strainGaugeDepthAgr[[nameVarPrecipBulk]] <- c(diff(varBench),as.numeric(NA))
  strainGaugeDepthAgr[[nameVarPrecipBulk]][strainGaugeDepthAgr[[nameVarPrecipBulk]] < 0] <- 0
  
} # End loop around surrogates

df <- data.table::melt(strainGaugeDepthAgr[,c('startDateTime','strainGaugeDepth',paste0('strainGaugeDepthS',seq_len(nSurr)),'bench',paste0('benchS',seq_len(nSurr)))],id.vars=c('startDateTime'))
plotly::plot_ly(data=df,x=~startDateTime,y=~value,color=~variable,mode='lines')

# Compute the uncertainty in precip based on the variability in computed benchmark of the surrogates
# The uncertainty of a sum or difference is equal to their individual uncertainties added in quadrature.
nameVar <- names(strainGaugeDepthAgr)
nameVarBenchS <- nameVar[grepl('benchS[0-9]',nameVar)]
strainGaugeDepthAgr$benchS_std <- matrixStats::rowSds(as.matrix(strainGaugeDepthAgr[,nameVarBenchS]))
strainGaugeDepthAgr$precipS_std <- sqrt(strainGaugeDepthAgr$benchS_std^2 + lag(strainGaugeDepthAgr$benchS_std, 1)^2)
strainGaugeDepthAgr$precipS_u95 <- strainGaugeDepthAgr$precipS_std*2
df <- data.table::melt(strainGaugeDepthAgr[,c('startDateTime','precipBulk','precipS_u95')],id.vars=c('startDateTime'))
plotly::plot_ly(data=df,x=~startDateTime,y=~value,color=~variable,mode='lines')

# Report total precip, and compute uncertainty for the central day
# We can use the same equation here, adding the uncertainties for the start and
# end of the day in quadrature, with the caveat that the benchmark does not drop 
# over the course of the day. If this occurs we need to compute for each leg of 
# a flat or increasing benchmark, summing the legs in quadrature
dayOut <- InfoDirIn$time
setDayUcrt <- which(strainGaugeDepthAgr$startDateTime >= dayOut & 
  strainGaugeDepthAgr$startDateTime <= (dayOut + as.difftime(1,units='days'))) # include first point of next day, because that is the point from which the difference is taken
setOut <- which(strainGaugeDepthAgr$startDateTime >= dayOut & 
  strainGaugeDepthAgr$startDateTime < (dayOut + as.difftime(1,units='days')))

# Compute uncertainty for each differencing leg (i.e. period of same or increasing benchmark)
benchDiff <- diff(strainGaugeDepthAgr$bench[setDayUcrt])
setBrk <- c(0,which(is.na(benchDiff) | benchDiff < 0),length(setDayUcrt))
ucrtDayBrk <- rep(0,length(setBrk)-1)
for (idxBrk in seq_len(length(setBrk)-1)){
  idxLegBgn <- setDayUcrt[setBrk[idxBrk]+1]
  idxLegEnd <- setDayUcrt[setBrk[idxBrk+1]]
  if((idxLegEnd-idxLegBgn) == 0){
    next
  }
  ucrtDayBrk[idxBrk] <- sqrt(strainGaugeDepthAgr$precipS_std[idxLegBgn]^2 + strainGaugeDepthAgr$precipS_std[idxLegEnd]^2)
}
UcrtDay <- sqrt(sum(ucrtDayBrk^2))
PrecipDay <- sum(strainGaugeDepthAgr$precipBulk[setOut])
print(paste0('precip sum for ', dayOut, ': ',round(PrecipDay,1), ' mm +- ',round(UcrtDay,1), ' mm (std)'))



# From https://rdrr.io/github/wol-fi/multifractal/src/R/iaaft.R
iaaft <- function(x, xdist=x, N=1, tolerance=0.01, maxit=100, adjust.var=TRUE,
                  zero.mean=TRUE, quiet=TRUE, diff=FALSE, criterion=c("periodogram"),
                  rel.convergence=TRUE, method=c("shell")){
  rescale_ia <- function(X,Y,method=c("shell")){
    X[sort(X,index=T,method=method)$ix] <- sort(Y,method=method)
    return(X)
  }
  if(criterion!="periodogram" && criterion!="acf"){
    cat("Criterion",criterion,"unknown, using periodogram\n")
    criterion <- c("periodogram")
  }
  n <- length(x)
  if(n==length(xdist)){
    if(adjust.var){
      sigx <- sqrt(var(x))
      sigxdist <- sqrt(var(xdist))
      x <- x*sigxdist/sigx
    }
    
    xdist.mean <- NULL
    if(zero.mean){
      xdist.mean <- mean(xdist)
      xdist <- xdist-xdist.mean
    }
    
    c <- sort(xdist,method=method)
    S <- fft(x)
    
    if(criterion=="acf")
      Xacf <- acf(x,plot=FALSE,lag.max=n-1) ## for acf convergence criterion
    else
      Xspc <- spectrum(x,plot=FALSE)    ## for periodogram convergence criterion
    
    rr <- list()
    for(j in 1:N){
      r <- sample(xdist)
      convergence <- FALSE
      it <- 0                              ## initialize iteration counter
      if(diff) diff.vect <- rep(NA,maxit)  ## take along a difference vector
      else diff.vect <- NULL
      rel.diff <- NA                       ## initialize relative difference
      
      while(!convergence && it<=maxit){
        R <- fft(r)
        s <- fft(complex(modulus=Mod(S),argument=Arg(R)),inverse=TRUE)/n
        r.new <- rescale_ia(Re(s),c,method=method)
        if(criterion=="acf"){
          Racf <- acf(r,plot=FALSE,lag.max=n-1)
          diff.new <- sum((Racf$acf-Xacf$acf)^2)/n
        }
        else{
          Rspc <- spectrum(r,plot=FALSE)
          diff.new <- sum((Rspc$spec-Xspc$spec)^2)/n
        }
        if(it>=1){
          if(rel.convergence)
            diff <- abs((diff.new-diff.old)/diff.old)
          else
            diff <- diff.new
          if(diff<tolerance)
            convergence <- TRUE
        }
        if(!quiet) cat("Iteration: ",it,", Difference in ACF =",diff.new,", relative Improvement = ",diff,"\n")
        diff.old <- diff.new
        if(diff) diff.vect[it] <- diff.new
        r <- r.new
        it <- it+1
      }
      
      if(!is.null(xdist.mean)) r <- r+xdist.mean
      
      rr[[j]] <- r
    }
    rr <- do.call("cbind", rr)
    return(rr)
  } else {
    cat("Series need to have same length, exiting!!\n")
  }
}