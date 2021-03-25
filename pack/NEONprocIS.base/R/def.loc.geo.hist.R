##############################################################################################
#' @title Get geolocation history from a sensor-based location file 

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Read sensor locations json file and return the geolocation history of
#' all the configured locations within the file.  

#' @param NameFile Filename (including relative or absolute path). Must be json format.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return 
#' A list of configured locations found in the file. Nested within the list element for each 
#' configured location is a list of geolocation history, one list element per location change. 
#' Further nested within the list element for each geolocation is a variable list of properties 
#' of that geolocation. Each geolocation property may also be a list of elements.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords currently none

#' @examples 
#' # Not run
#' # NameFile <- '~/pfs/aquatroll200_23614_locations.json'
#' # locGeoHist <- NEONprocIS.base::def.loc.meta(NameFile=NameFile)

#' @seealso \link[NEONprocIS.base]{def.loc.meta}
#' @seealso \link[NEONprocIS.base]{def.loc.filt}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2021-01-26)
#     original creation
##############################################################################################
def.loc.geo.hist <- function(NameFile,log=NULL){

  # Initialize log if not input
  if (is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Validate the json
  if(NEONprocIS.base::def.validate.json(jsonIn=NameFile,log=log) != TRUE){
    stop()
  }
  
  # Load the full json into list & grab metadata
  locFull <- rjson::fromJSON(file=NameFile,simplify=TRUE)
  locMeta <- NEONprocIS.base::def.loc.meta(NameFile=NameFile)
  
  # For each location, grab the geolocation history
  nameLoc <- base::unique(locMeta$name)
  
  # Grab geo location history for each named location
  locGeoHist <- base::vector(mode='list',length=base::length(nameLoc)) # Initialize
  base::names(locGeoHist) <- nameLoc
  
  log$debug(base::paste0(base::length(nameLoc),' configured locations found in file ',NameFile))
  
  for(nameLocIdx in nameLoc){
    
    # Which index into features
    idxFeat <- base::which(locMeta$name == nameLocIdx)[1]

    locHist <- locFull$features[[idxFeat]]$properties$locations$features
    numHist <- base::length(locHist) # length of geoloc history 
    locGeoHist[[nameLocIdx]] <- base::vector(mode='list',length=numHist) # Initialize
    
    log$debug(base::paste0(numHist,' geolocations found in the history for configured location ',nameLocIdx))
              
    for(idxHist in base::seq.int(numHist)){
      
      # Organize location properties 
      locHistIdx <- locHist[[idxHist]]
      propLoc <- locHistIdx$properties$location_properties
      propLoc <- base::lapply(propLoc,FUN=function(idx){
        if(base::is.list(idx)){
          return(base::list(name=idx[[1]],value=idx[[2]]))
        } else {
          return(base::list(name=idx[1],value=utils::tail(idx,-1)))
        }
      })
      nameProp <- base::unlist(base::lapply(propLoc,FUN=function(idx){idx$name}))
      propLoc <- base::lapply(propLoc,FUN=function(idx){idx$value})
      base::names(propLoc) <- nameProp

      # Format the dates
      if(base::is.null(locHistIdx$properties$start_date)){
        timeBgnIdx <- NA
      } else {
        timeBgnIdx <- base::as.POSIXct(locHistIdx$properties$start_date,tz='GMT')
      }
      if(base::is.null(locHistIdx$properties$end_date)){
        timeEndIdx <- NA
      } else {
        timeEndIdx <- base::as.POSIXct(locHistIdx$properties$end_date,tz='GMT')
      }
      
      # Combine with other location properties in a different list
      propLoc <- 
        c(
          base::list(
            start_date=timeBgnIdx,
            end_date=timeEndIdx,
            geometry=locHistIdx$geometry,
            alpha = locHistIdx$properties$alpha,
            beta = locHistIdx$properties$beta,
            gamma = locHistIdx$properties$gamma,
            x_offset = locHistIdx$properties$x_offset,
            y_offset = locHistIdx$properties$y_offset,
            z_offset = locHistIdx$properties$z_offset
          ),
          propLoc
        )
 
      # Follow the chain of reference locations to the end, nesting their location history
      locRef00 <- locHistIdx$properties$reference_location
      nameLocRef=locRef00$properties$name
      idxRef <- 1
      cont <- TRUE
     
      while (cont) {
        # Get the parent of our current level
        numLvl <- base::length(idxRef)
        
        if (numLvl == 1) {
          txtEval <- 'locRef00$properties$locations$features'
        } else if (numLvl == 2) {
          txtEval <-
            base::paste0(
              'locRef00$properties$locations$features[[',
              idxRef[1],
              ']]',
              base::paste0(
                '$properties$reference_location$properties$locations$features',
                collapse = ''
              )
            )
        } else {
          txtEval <-
            base::paste0(
              'locRef00$properties$locations$features[[',
              idxRef[1],
              ']]',
              base::paste0(
                '$properties$reference_location$properties$locations$features[[',
                idxRef[2:(length(idxRef) - 1)],
                ']]',
                collapse = ''
              ),
              '$properties$reference_location$properties$locations$features'
            )
        }
        locRefPrnt <- base::eval(parse(text = txtEval))
        
        # How many geolocs do we have ?
        numRef <- base::length(locRefPrnt)
        
        # If there was a named location listed as the first reference location, but no actual geolocation history...
        # record the name of the reference location
        if(base::length(idxRef) == 1 & numRef == 0 && base::length(
          locRef00$properties$name
        ) != 0) {
          propLoc <- c(propLoc,
                       base::list(reference_location=
                                  base::list(
                                      base::list(
                                            name=locRef00$properties$name
                                      )
                                  )
                       )
          )
        }
        
        
        # Check whether we are beyond the end of reference geolocations at this level
        if (utils::tail(idxRef, 1) > numRef) {
          # Back out a level, increment the index
          idxRef <- idxRef[-length(idxRef)]
          if (base::length(idxRef) == 0) {
            # We've gone through the entire hierarchy. We're done.
            cont <- FALSE
            next
          } else {
            idxRef[base::length(idxRef)] <-
              utils::tail(idxRef, 1) + 1 # increment index
            next
          }
        }
        
        # Set locRef to the data at our current level
        if (base::length(idxRef) > 1) {
          # Where we will find the reference location info
          txtEval <-
            base::paste0(
              'locRef00$properties$locations$features[[',
              idxRef[1],
              ']]',
              base::paste0(
                '$properties$reference_location$properties$locations$features[[',
                idxRef[-1],
                ']]',
                collapse = ''
              )
            )
          
          # Where we will find the reference location name
          txtChop <- base::paste0(
            'locations$features[[',
            tail(idxRef,1),
            ']]'
          )
          txtEvalName <-base::paste0(
            base::substr(txtEval,start=1,stop=nchar(txtEval)-base::nchar(txtChop)),
            'name')
          
          # Where we will save the reference location info in the output
          txtAsgnLeft <- 
            base::paste0(
              'propLoc$reference_location[[',
              idxRef[1],
              ']]',
              base::paste0(
                '$reference_location[[',
                idxRef[-1],
                ']]',
                collapse = ''
              )
            )
        } else {
          # Where we will find the reference location geolocation history
          txtEval <-
            base::paste0('locRef00$properties$locations$features[[',
                         idxRef[1],
                         ']]')
          
          # Where we will find the reference location name
          txtChop <- base::paste0(
            'locations$features[[',
            tail(idxRef,1),
            ']]'
          )
          txtEvalName <-base::paste0(
            base::substr(txtEval,start=1,stop=nchar(txtEval)-base::nchar(txtChop)),
            'name')
          
          # Where we will save the reference location info in the output
          txtAsgnLeft <- 
            base::paste0('propLoc$reference_location[[',
                                  idxRef[1],
                                  ']]')
        }
        locRef <- base::eval(parse(text = txtEval))
        nameLocRef <- base::eval(parse(text = txtEvalName))
        
        # Organize location properties 
        propLocRef <- locRef$properties$location_properties
        propLocRef <- base::lapply(propLocRef,FUN=function(idx){
          if(base::is.list(idx)){
            return(base::list(name=idx[[1]],value=idx[[2]]))
          } else {
            return(base::list(name=idx[1],value=utils::tail(idx,-1)))
          }
        })
        nameProp <- base::unlist(base::lapply(propLocRef,FUN=function(idx){idx$name}))
        propLocRef <- base::lapply(propLocRef,FUN=function(idx){idx$value})
        base::names(propLocRef) <- nameProp
        
        # Format the dates
        if(base::is.null(locRef$properties$start_date)){
          timeBgnRef <- NA
        } else {
          timeBgnRef <- base::as.POSIXct(locRef$properties$start_date,tz='GMT')
        }
        if(base::is.null(locRef$properties$end_date)){
          timeEndRef <- NA
        } else {
          timeEndRef <- base::as.POSIXct(locRef$properties$end_date,tz='GMT')
        }
        
        # Combine with other location properties in a different list
        propLocRef <- 
          c(
            base::list(
              name=nameLocRef,
              start_date=timeBgnRef,
              end_date=timeEndRef,
              geometry=locRef$geometry,
              alpha = locRef$properties$alpha,
              beta = locRef$properties$beta,
              gamma = locRef$properties$gamma,
              x_offset = locRef$properties$x_offset,
              y_offset = locRef$properties$y_offset,
              z_offset = locRef$properties$z_offset
            ),
            propLocRef
          )

        # save it into the output
        txtAsgn <- base::paste0(txtAsgnLeft,' <- propLocRef')
        base::eval(base::parse(text = txtAsgn))
        
        # Are there more reference locations?
        if (base::length(
          locRef$properties$reference_location$properties$locations$features
        ) != 0) {
          # Go a level deeper
          idxRef <- c(idxRef, 1)
          next
          
        } else {
          # We found the end of the reference chain! Let's move on to the next geolocation at this same level.
          
          # If there was a named location listed as the reference location, but no actual geolocation history...
          # record the name of the reference location
          if(base::length(
            locRef$properties$reference_location$properties$name
          ) != 0) {
            propLocRef <- c(propLocRef,
                            base::list(reference_location=
                                         base::list(
                                           base::list(
                                              name=locRef$properties$reference_location$properties$name
                                              )
                                          )
                            )
            )
            # save it into the output
            txtAsgn <- base::paste0(txtAsgnLeft,' <- propLocRef')
            base::eval(base::parse(text = txtAsgn))
            
          }
          
          
          idxRef[base::length(idxRef)] <-
            utils::tail(idxRef, 1) + 1
          
        }
      } # End loop around reference locations
        
      # Save the location properties for this geolocation history entry
      locGeoHist[[nameLocIdx]][[idxHist]] <- propLoc
      
    } # End loop around location history
    
  } # End loop around named locations

  return(locGeoHist)
  
}
