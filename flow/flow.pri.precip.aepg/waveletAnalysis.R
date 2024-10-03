# y <- modwt(x=strainGaugeDepthAgr$strainGaugeDepth, wf = "la8", n.levels = 10, boundary = "reflection")
# y1 <- y; y1[2:11] <- NULL; d1 <- imodwt(y1)
MRA <- waveslim::mra(x=strainGaugeDepthAgr$strainGaugeDepth, wf = "la8", J = 10, method = "modwt", boundary = "reflection")
base::plot(MRA$D1+MRA$D2+MRA$D3+MRA$D4+MRA$D5+MRA$D6+MRA$D7+MRA$D8+MRA$D9+MRA$D10+MRA$S10)

# What if we just add variability at the small scale for uncertainty calc? actual precip results in longer scale variability (increase in bucket depth)
# Maybe a bit beyond diel scale
# 5-min data (288 values per day)
# detail scale 1 (2^1 values) = 10 min
# detail scale 2 (2^2 values) = 20 min
# detail scale 3 (2^3 values) = 40 min
# detail scale 4 (2^4 values) = 1 hr 20 min
# detail scale 5 (2^5 values) = 2 hr 40 min
# detail scale 6 (2^6 values) = 5 hr 20 min
# detail scale 7 (2^7 values) = 10 hr 40 min
# detail scale 8 (2^8 values) = 21 hr 20 min
# detail scale 9 (2^9 values) = 42 hr 40 min
# detail scale 10 (2^10 values) = 3 days 13 hr 20 min
# detail scale 11 (2^11 values) = 7 days 2 hr 40 min
base::plot(MRA$D1+MRA$D2+MRA$D3+MRA$D4+MRA$D5+MRA$D6+MRA$D7+MRA$D8+MRA$D9)
base::plot(MRA$D10 + MRA$S10)

# Subtract the benchmark first? Then do wavelets? This would remove evaporation, which we do want to test for
depthMinusBench <- strainGaugeDepthAgr$strainGaugeDepth - strainGaugeDepthAgr$bench
MRA2 <- waveslim::mra(x=depthMinusBench, wf = "la8", J = 10, method = "modwt", boundary = "reflection")
base::plot(MRA2$D1+MRA2$D2+MRA2$D3+MRA2$D4+MRA2$D5+MRA2$D6+MRA2$D7+MRA2$D8+MRA2$D9+MRA2$D10+MRA2$S10)
base::plot(MRA2$D10 + MRA2$S10)


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
nSurr <- 10
surr <- iaaft(x=depthMinusBench,N=nSurr)


# Take the benchmark and add on the surrogate variability
for(idxSurr in seq_len(nSurr)){
  
  message(paste0('Running Surrogate ',idxSurr))
  nameVarBench <- paste0('benchS',idxSurr)
  nameVarPrecip <- paste0('precipS',idxSurr)
  nameVarPrecipType <- paste0('precipTypeS',idxSurr)
  
  # Add the surrogates to the benchmark
  strainGaugeDepthS <- strainGaugeDepthAgr$bench + surr[,idxSurr]

  #initialize fields
  strainGaugeDepthAgr[[nameVarBench]] <- NA
  strainGaugeDepthAgr[[nameVarPrecip]] <- FALSE #add TRUE when rain detected
  strainGaugeDepthAgr[[nameVarPrecipType]] <- FALSE #add TRUE when rain detected
  
  #start counters
  rawCount <- 0
  timeSincePrecip <- NA
  currRow <- rangeSize #instead of 24 for hourly this will be how ever many rows encompass one day
  
  #initialize fields
  strainGaugeDepthAgr[[nameVarBench]] <- NA
  strainGaugeDepthAgr[[nameVarBench]][1:currRow] <-  stats::quantile(strainGaugeDepthS[1:currRow],Quant,na.rm=TRUE)
  
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
        strainGaugeDepthAgr[[nameVarBench]][(idxEnd+1):currRow] <- NA
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
      strainGaugeDepthAgr[[nameVarBench]][idxBgnNext:currRow] <- stats::quantile(strainGaugeDepthS[idxBgnNext:currRow],Quant,na.rm=TRUE)
      timeSincePrecip <- NA
      skipping <- FALSE
    }
    
    
    if(!is.na(timeSincePrecip)){
      timeSincePrecip <- timeSincePrecip + 1
    }
    
    # #establish nth min/max of range
    recentPrecip <- base::any(strainGaugeDepthAgr[[nameVarPrecip]][i:currRow])
    
    
    #establish benchmark
    bench <- strainGaugeDepthAgr[[nameVarBench]][currRow-1]
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
        
      } else if (grepl('volumeThresh',x=strainGaugeDepthAgr[[nameVarPrecipType]][currRow-1]) && rawChange > ThshChange){
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
            strainGaugeDepthAgr[[nameVarBench]][idx] <- rawIdx
          } else {
            strainGaugeDepthAgr[[nameVarBench]][idx] <- benchNext
          }
          
          # Record rain stats
          strainGaugeDepthAgr[[nameVarPrecip]][idx] <- precip
          strainGaugeDepthAgr[[nameVarPrecipType]][idx] <- 'ThshCountBackFilledToStart'
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
    if (!is.na(timeSincePrecip) && timeSincePrecip == rangeSize && raw > (strainGaugeDepthAgr[[nameVarBench]][i-1]-Recharge)){  # Maybe use Envelope instead of Recharge?
      
      # Exactly one day after rain ends, and if the depth hasn't dropped precipitously (as defined by the Recharge threshold),
      # back-adjust the benchmark to the median of the last day to avoid overestimating actual precip
      
      bench <- raw_med_lastDay
      strainGaugeDepthAgr[[nameVarBench]][i:currRow] <- bench
      strainGaugeDepthAgr[[nameVarPrecipType]][i:currRow] <- "postPrecipAdjToMedNextDay"
      
      idxBgn <- i-1
      keepGoing <- TRUE
      while(keepGoing == TRUE) {
        
        if(is.na(strainGaugeDepthAgr[[nameVarPrecip]][idxBgn]) || strainGaugeDepthAgr[[nameVarPrecip]][idxBgn] == FALSE){
          # Stop if we are past the point where the precip started
          keepGoing <- FALSE
        } else if(strainGaugeDepthAgr[[nameVarBench]][idxBgn] > bench){
          strainGaugeDepthAgr[[nameVarBench]][idxBgn] <- bench
          strainGaugeDepthAgr[[nameVarPrecipType]][idxBgn] <- paste0(strainGaugeDepthAgr[[nameVarPrecipType]][idxBgn],"BackAdjToMedNextDay")
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
        strainGaugeDepthAgr[[nameVarBench]][setAdj] <- NA
        strainGaugeDepthAgr[[nameVarPrecip]][setAdj] <- NA
        strainGaugeDepthAgr[[nameVarPrecipType]][setAdj] <- NA
      } else {
        strainGaugeDepthAgr[[nameVarBench]][setAdj] <- strainGaugeDepthAgr[[nameVarBench]][idxSet]
        strainGaugeDepthAgr[[nameVarPrecip]][setAdj] <- strainGaugeDepthAgr[[nameVarPrecip]][idxSet]
        strainGaugeDepthAgr[[nameVarPrecipType]][setAdj] <- "ExcludeBeforeRecharge"
      }
      
    }
    
    #update in the data
    strainGaugeDepthAgr[[nameVarBench]][currRow] <- bench
    strainGaugeDepthAgr[[nameVarPrecip]][currRow] <- precip
    strainGaugeDepthAgr[[nameVarPrecipType]][currRow] <- precipType
    #move to next row
    currRow <- currRow + 1
    
    #stop at end of data frame
    if (currRow == nrow(strainGaugeDepthAgr)){
      strainGaugeDepthAgr[[nameVarBench]][currRow] <- bench
      break()
    }
  }
  
  nameVarPrecipBulk <- paste0('precipBulkS',idxSurr)
  strainGaugeDepthAgr[[nameVarPrecipBulk]] <- strainGaugeDepthAgr[[nameVarBench]] - lag(strainGaugeDepthAgr[[nameVarBench]], 1)
  strainGaugeDepthAgr[[nameVarPrecipBulk]][strainGaugeDepthAgr[[nameVarPrecipBulk]] < 0] <- 0
  
} # End loop around surrogates

df <- data.table::melt(strainGaugeDepthAgr[,c('startDateTime','strainGaugeDepth','bench',paste0('benchS',seq_len(nSurr)))],id.vars=c('startDateTime'))
plotly::plot_ly(data=df,x=~startDateTime,y=~value,color=~variable,mode='lines')









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