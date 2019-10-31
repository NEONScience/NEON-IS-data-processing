# starting point is the geoloc history for a single named location
geoloc <- 
  list(
    geoloc1=list(
      geometry=NULL,
      properties=list(
        date=1,
        xoffset=1,
        ref=list(
          geometry=NULL,
          name='ref1',
          geoloc=list(
            geoloc11=list(
              geometry=list(
                coord=c(1,2,3)
              ),
              properties=list(
                date=1,
                xoffset=NA,
                ref=list(
                  name='ref1',
                  geoloc=NULL
                )
              )
            ),
            geoloc12=list(
              geometry=NULL,
              properties=list(
                date=1,
                xoffset=1,
                ref=list(
                  name='ref11',
                  geoloc=list(
                    geoloc121=list(
                      geometry=list(
                        coord=c(4,5,6)
                      ),
                      properties=list(
                        date=1,
                        xoffset=NA,
                        ref=list(
                          name='ref11',
                          geoloc=NULL
                        )
                      )
                    )
                  )
                )
              )
            )
          )
        )
      )
    ), # close geoloc1
    geoloc2=list(
      geometry=NULL,
      properties=list(
        date=1,
        xoffset=1,
        ref=list(
          geometry=NULL,
          name='ref2',
          geoloc=list(
            geoloc21=list(
              geometry=NULL,
              properties=list(
                date=1,
                xoffset=1,
                ref=list(
                  name='ref21',
                  geoloc=list(
                    geoloc221=list(
                      geometry=NULL,
                      properties=list(
                        date=1,
                        xoffset=5,
                        ref=list(
                          name='ref211',
                          geoloc=list(
                            geoloc2211=list(
                              geometry=list(
                                coord=c(7,7,7)
                              ),
                              properties=list(
                                date=5,
                                xoffset=NA,
                                ref=list(
                                  name='ref211',
                                  geoloc=NULL
                                )
                              )
                            ) # End geoloc2211
                          )
                        ) # end ref211
                      )
                    ), # End geoloc221
                    geoloc222=list(
                      geometry=list(
                        coord=c(1,1,1)
                      ),
                      properties=list(
                        date=1,
                        xoffset=NA,
                        ref=list(
                          name='ref21',
                          geoloc=NULL
                        )
                      )
                    )
                  )
                )
              )
            ),
            geoloc22=list(
              geometry=list(
                coord=c(1,2,3)
              ),
              properties=list(
                date=1,
                xoffset=NA,
                ref=list(
                  name='ref2',
                  geoloc=NULL
                )
              )
            )
          )
        )
      )
    ) # close geoloc2
  ) # close list of geoloc history for a single named location
    


locRef0 <- geoloc$geoloc2$properties$ref


idxGeo <- 1
cont <- TRUE
while(cont){
  
  # Get the parent of our current level
  if(length(idxGeo) == 1){
    txtEval <- 'locRef0$geoloc'
  } else if(length(idxGeo) == 2) {
    txtEval <- paste0('locRef0$geoloc[[',idxGeo[1],']]',paste0('$properties$ref$geoloc',collapse=''))
  } else {
    txtEval <- paste0('locRef0$geoloc[[',idxGeo[1],']]',paste0('$properties$ref$geoloc[[',idxGeo[2:(length(idxGeo)-1)],']]',collapse=''),'$properties$ref$geoloc')
  }
  locRefPrnt <- eval(parse(text=txtEval)) 
  
  # How many geolocs do we have ?
  numLocGeo <- length(locRefPrnt)
  
  # Check whether we are beyond the end of geolocations at this level
  if(tail(idxGeo,1) > numLocGeo){
    
    # Back out a level, increment the index
    idxGeo <- idxGeo[-length(idxGeo)]
    if(length(idxGeo) == 0){
      # We've gone through the entire hierarchy. We're done.
      cont <- FALSE
      next
    } else {
      idxGeo[length(idxGeo)] <- tail(idxGeo,1) + 1 # increment index
      next
    }
  }
  
  # Set locRef to the data at our current level
  if(length(idxGeo) > 1){
    txtEval <- paste0('locRef0$geoloc[[',idxGeo[1],']]',paste0('$properties$ref$geoloc[[',idxGeo[-1],']]',collapse=''))
  } else {
    txtEval <- paste0('locRef0$geoloc[[',idxGeo[1],']]')
  }
  locRef <- eval(parse(text=txtEval)) 
  
  # Are there more reference locations?
  if(length(locRef$properties$ref$geoloc) != 0){
    
    # Go a level deeper
    idxGeo <- c(idxGeo,1)
    next

  } else {
    
    # Show we found the end
    print(paste0(idxGeo,collapse=','))
    
    # Move on to the next geolocation at this same level.
    idxGeo[length(idxGeo)] <- tail(idxGeo,1) + 1

  }
}