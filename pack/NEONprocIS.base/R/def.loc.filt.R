##############################################################################################
#' @title Filter named location information by date-time range for NEON instrumented systems sensors

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Read named location information (including geolocation history) from JSON file
#' for NEON instrumented systems sensor and filter it for a date-time range

#' @param NameFileIn Filename (including relative or absolute path). Must be json format.
#' @param NameFileOut Filename (including relative or absolute path). Must be json format. Defaults to
#' NULL, in which case only the filtered json will be returned in list format
#' @param TimeBgn POSIX timestamp of the start time (inclusive)
#' @param TimeEnd POSIX timestamp of the end time (non-inclusive). Defaults to NULL, in which case the
#' location information will be filtered for the exact time of TimeBgn
#' @param Prop character vector of the properties in the location file to retain. Defaults to 'all', 
#' in which all properties of the original file are retained. Include here the names of properties embedded
#' at the top level of each location install entry (e.g. "HOR","VER","Data Rate") as well as within the 
#' "properties" list of each location install entry (e.g. "context","locations","install_date","remove_date",
#' "name","site"). Currently there is no option to filter for particular geolocation properties (within the
#' properties:locations:features list)

#' @return A list of filtered location information. If NameFileOut is specified, the filtered location
#' information will also be writted to file in the same json format of NameFileIn

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' TimeBgn <- base::as.POSIXct('2018-01-01',tz='GMT')
#' TimeEnd <- base::as.POSIXct('2018-01-02',tz='GMT')
#' NameFileIn <- 'locations.json'
#' NameFileOut <- 'filtered_locations.json'
#' loc <- def.loc.filt(NameFileIn,NameFileOut,TimeBgn,TimeEnd)
#' 
#' Filtering for some basics only, and excluding geolocation history
#' Prop <- c("HOR","VER","install_date","remove_date","name","site","Data Rate")
#' loc <- def.loc.filt(NameFileIn,NameFileOut,TimeBgn,TimeEnd,Prop)


#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-07-09)
#     original creation
#   Mija Choi (2020-01-14)
#     Added parameter validations and logging
#   Mija Choi (2020-01-30)
#     Added json schema validations
#   Mija Choi (2020-03-25)
#     Modified to add a read-only file, inst/extdata/locations-schema.json, in NEONprocIS.base package 
#   Cove Sturtevant (2020-08-19)
#     Modified filtered output to truncate all relevant dates in the locations file to the 
#      date range of interest (TimeBgn to TimeEnd). Original dates falling between TimeBgn
#      and TimeEnd will not be modified.
#   Cove Sturtevant (2021-08-31)
#      stop execution when schema does not conform
#   Cove Sturtevant (2023-11-16)
#      add option to filter for select properties
#   Cove Sturtevant (2024-07-09)
#      apply additional sorting of output 
##############################################################################################
def.loc.filt <- function(NameFileIn,
                         NameFileOut = NULL,
                         TimeBgn,
                         TimeEnd = NULL,
                         Prop = 'all',
                         log = NULL) {
    # Initialize log if not input
    if (is.null(log)) {
      log <- NEONprocIS.base::def.log.init()
    }
    #
    # First, validate the syntax of input json to see if it is valid
    #
    validateJson <-
      NEONprocIS.base::def.validate.json (NameFileIn)
    #
    # Second, validate the json against the schema only if the syntax is valid.
    # Otherwise, validateJsonSchema errors out due to the syntax error
    #

    validateJsonSchema <- FALSE
    locJsonSchema <- system.file("extdata", "locations-sensor-schema.json", package="NEONprocIS.base")
    if (validateJson == TRUE)  {
      validateJsonSchema <-
        NEONprocIS.base::def.validate.json.schema (NameFileIn, locJsonSchema)
    }
    
    #if the validation fails, the function will not be executed 
    if ((validateJson == FALSE) || (validateJsonSchema == FALSE))
    {
      log$error(
        base::paste0(
          'In def.loc.filt::: Erred out due to the json validation failure of this file, ',
          NameFileIn
        )
      )
      stop("In def.loc.filt::::: Erred out due to the validation failure of the input JSON")
    }

    # Run the code when the input json is correct syntacically and valid against the schema 
    FmtTime <- '%Y-%m-%dT%H:%M:%SZ' # Time format in the location file
    
    # If NULL, set TimeEnd to 1 second after TimeBgn
    if (base::is.null(TimeEnd)) {
      TimeEnd <- TimeBgn + as.difftime(1, units = 'secs')
    }
    
    # Get formatted character representations of the start and end times
    TimeBgnFmt <- base::format(TimeBgn,format=FmtTime)
    TimeEndFmt <- base::format(TimeEnd,format=FmtTime)
    
    # Load in the raw json info
    loc <- rjson::fromJSON(file = NameFileIn, simplify = FALSE)
    
    # Filter the named locations
    setKeepLoc <- base::numeric(0)
    for (idxLoc in base::seq_len(length(loc$features))) {
      timeInst <- loc$features[[idxLoc]]$properties$install_date
      if (!base::is.null(timeInst)) {
        timeInst <-
          base::as.POSIXct(timeInst, format = FmtTime, tz = 'GMT')
      }
      timeRmv <- loc$features[[idxLoc]]$properties$remove_date
      if (!base::is.null(timeRmv)) {
        timeRmv <-
          base::as.POSIXct(timeRmv, format = FmtTime, tz = 'GMT')
      }
      
      if ((timeInst < TimeEnd) &&
          (base::is.null(timeRmv) || (timeRmv > TimeBgn))) {
        # Mark this location to keep (it applies to this day)
        setKeepLoc <- c(setKeepLoc, idxLoc)
        
        # Truncate the install and remove dates to the start & end times, so that any 
        # upstream changes outside TimeBgn and TimeEnd are indistinguishable in 
        # downstream processing
        if(timeInst < TimeBgn){
          loc$features[[idxLoc]]$properties$install_date <- TimeBgnFmt
        }
        if(base::is.null(timeRmv) || (timeRmv > TimeEnd)){
          loc$features[[idxLoc]]$properties$remove_date <- TimeEndFmt
        }
        
        # Sort the top level and properties list
        loc$features[[idxLoc]] <- loc$features[[idxLoc]][sort(names(loc$features[[idxLoc]]))] # Top level
        loc$features[[idxLoc]]$properties <- loc$features[[idxLoc]]$properties[sort(names(loc$features[[idxLoc]]$properties))] # Properties list
      }
    }
    loc$features <- loc$features[setKeepLoc]
    
    # If setKeepLoc is empty, we're done. No applicable location information
    if (base::length(setKeepLoc) == 0) {
      base::write(rjson::toJSON(loc, indent = 3), file = NameFileOut)
      return(loc)
    }
    
    
    # Filter for particular properties
    if(!('all' %in% Prop)){
      for (idxLoc in base::seq_len(base::length(loc$features))) {
        # Traverse top level
        locIdx <- loc$features[[idxLoc]]
        nameProp <- base::names(locIdx)
        setKeep <- nameProp %in% c('properties','geometry',Prop)
        locIdx <- locIdx[sort(nameProp[setKeep])] # Also sort to ensure a change in ordering of properties in file results in the same output
        
        # Descend into "properties"
        locIdxProp <- locIdx$properties
        nameProp <- base::names(locIdxProp)
        setKeep <- nameProp %in% Prop
        locIdxProp <- locIdxProp[sort(nameProp[setKeep])] # Also sort to ensure a change in ordering of properties in file results in the same output
        
        # Put filtered set back into master list
        locIdx$properties <- locIdxProp
        loc$features[[idxLoc]] <- locIdx
      }
    }
    
    
    # For each applicable named location, filter its geolocation date ranges
    if (any(c('all','locations') %in% Prop)) {
      for (idxLoc in base::seq_len(base::length(loc$features))) {
        locGeo <- loc$features[[idxLoc]]$properties$locations$features
        
        # Filter the geolocations
        setKeepGeo <- base::numeric(0)
        for (idxGeo in base::seq_len(base::length(locGeo))) {
          timeBgnGeo <- locGeo[[idxGeo]]$properties$start_date
          if (!base::is.null(timeBgnGeo)) {
            timeBgnGeo <-
              base::as.POSIXct(timeBgnGeo, format = FmtTime, tz = 'GMT')
          }
          timeEndGeo <- locGeo[[idxGeo]]$properties$end_date
          if (!base::is.null(timeEndGeo)) {
            timeEndGeo <-
              base::as.POSIXct(timeEndGeo, format = FmtTime, tz = 'GMT')
          }
          
          if ((timeBgnGeo < TimeEnd) &&
              (base::is.null(timeEndGeo) ||
               (timeEndGeo > TimeBgn))) {
            setKeepGeo <- c(setKeepGeo, idxGeo)
            
            # Truncate the start and end dates to the time range we are interested in, so that any 
            # upstream changes outside TimeBgn and TimeEnd are indistinguishable in 
            # downstream processing
            if(timeBgnGeo < TimeBgn){
              locGeo[[idxGeo]]$properties$start_date <- TimeBgnFmt
            }
            if(base::is.null(timeEndGeo) || (timeEndGeo > TimeEnd)){
              locGeo[[idxGeo]]$properties$end_date <- TimeEndFmt
            }
            
            # Sort the top level feature list, properties and nested location_properties. For some reason, these reorder in the database call sometimes
            locGeo[[idxGeo]] <- locGeo[[idxGeo]][sort(names(locGeo[[idxGeo]]))]
            locGeo[[idxGeo]]$properties <- locGeo[[idxGeo]]$properties[sort(base::names(locGeo[[idxGeo]]$properties))]
            if('location_properties' %in% base::names(locGeo[[idxGeo]]$properties)){
              itemGeoProp <- locGeo[[idxGeo]]$properties$location_properties
              nameItem <- base::unlist(base::lapply(itemGeoProp,FUN=function(idxList){idxList[1]}))
              idxSortItem <- sort(nameItem,index.return=TRUE)$ix
              locGeo[[idxGeo]]$properties$location_properties <- locGeo[[idxGeo]]$properties$location_properties[idxSortItem]
            }
            
          
          } else {
            # We're deleting this geolocation, let's move on
            next
          }
          
          # Now filter the reference location geolocations (if there are any). Note that the reference locations
          # can have a chain of their own reference locations. We're going to navigate through them, marking those for
          # deletion at the end of the while loop
          locGeoRef00 <-
            locGeo[[idxGeo]]$properties$reference_location # Initialize
          idxGeoRef <- 1
          setRmvGeoRef <- base::list()
          cont <- TRUE
          
          # Sort the properties
          locGeoRef00$properties <- locGeoRef00$properties[sort(names(locGeoRef00$properties))]
          
          while (cont) {
            # Get the parent of our current level
            numLvl <- base::length(idxGeoRef)
            if (numLvl == 1) {
              txtEval <- 'locGeoRef00$properties$locations$features'
            } else if (numLvl == 2) {
              txtEval <-
                base::paste0(
                  'locGeoRef00$properties$locations$features[[',
                  idxGeoRef[1],
                  ']]',
                  base::paste0(
                    '$properties$reference_location$properties$locations$features',
                    collapse = ''
                  )
                )
            } else {
              txtEval <-
                base::paste0(
                  'locGeoRef00$properties$locations$features[[',
                  idxGeoRef[1],
                  ']]',
                  base::paste0(
                    '$properties$reference_location$properties$locations$features[[',
                    idxGeoRef[2:(length(idxGeoRef) - 1)],
                    ']]',
                    collapse = ''
                  ),
                  '$properties$reference_location$properties$locations$features'
                )
            }
            locRefPrnt <- base::eval(parse(text = txtEval))
            
            # How many geolocs do we have ?
            numGeoRef <- base::length(locRefPrnt)
            
            # Check whether we are beyond the end of reference geolocations at this level
            if (utils::tail(idxGeoRef, 1) > numGeoRef) {
              # Back out a level, increment the index
              idxGeoRef <- idxGeoRef[-length(idxGeoRef)]
              if (base::length(idxGeoRef) == 0) {
                # We've gone through the entire hierarchy. We're done.
                cont <- FALSE
                next
              } else {
                idxGeoRef[length(idxGeoRef)] <-
                  utils::tail(idxGeoRef, 1) + 1 # increment index
                next
              }
            }
            
            # Set locGeoRef to the data at our current level
            if (base::length(idxGeoRef) > 1) {
              txtEval <-
                base::paste0(
                  'locGeoRef00$properties$locations$features[[',
                  idxGeoRef[1],
                  ']]',
                  base::paste0(
                    '$properties$reference_location$properties$locations$features[[',
                    idxGeoRef[-1],
                    ']]',
                    collapse = ''
                  )
                )
            } else {
              txtEval <-
                base::paste0('locGeoRef00$properties$locations$features[[',
                             idxGeoRef[1],
                             ']]')
            }
            locGeoRef <- base::eval(parse(text = txtEval))
            
            # Check date range of geolocation
            timeBgnGeoRef <- locGeoRef$properties$start_date
            if (!base::is.null(timeBgnGeoRef)) {
              timeBgnGeoRef <-
                base::as.POSIXct(timeBgnGeoRef, format = FmtTime, tz = 'GMT')
            }
            timeEndGeoRef <- locGeoRef$properties$end_date
            if (!base::is.null(timeEndGeoRef)) {
              timeEndGeoRef <-
                base::as.POSIXct(timeEndGeoRef, format = FmtTime, tz = 'GMT')
            }
            
            # If this geoloc date range is outside our day, mark for deletion and move on
            if ((timeBgnGeoRef > TimeEnd) ||
                (!base::is.null(timeEndGeoRef) &&
                 timeEndGeoRef < TimeBgn)) {
              setRmvGeoRef[[base::length(setRmvGeoRef) + 1]] <- idxGeoRef
              
              # Move on. We don't need to dive further into reference locations for this geoloc
              idxGeoRef[base::length(idxGeoRef)] <-
                utils::tail(idxGeoRef, 1) + 1
              next
              
            } else {
              # Truncate the start and end dates to the time range we are interested in, so that any 
              # upstream changes outside TimeBgn and TimeEnd are indistinguishable in 
              # downstream processing
              if(timeBgnGeoRef < TimeBgn){
                txtTimeEval <- base::paste0(txtEval,'$properties$start_date <- TimeBgnFmt')
                base::eval(parse(text = txtTimeEval))
              }
              if(base::is.null(timeEndGeoRef) || (timeEndGeoRef > TimeEnd)){
                txtTimeEval <- base::paste0(txtEval,'$properties$end_date <- TimeEndFmt')
                base::eval(parse(text = txtTimeEval))
              }
              
              # Sort the properties and nested location_properties to ensure consistent output
              txtPropSortEval <- base::paste0(txtEval,'$properties <- ',txtEval,'$properties[sort(base::names(',txtEval,'$properties))]')
              base::eval(parse(text = txtPropSortEval))
              txtItemGeoProp <- base::paste0('itemGeoProp <- ',txtEval,'$properties$location_properties')
              base::eval(parse(text = txtItemGeoProp))
              nameItem <- base::unlist(base::lapply(itemGeoProp,FUN=function(idxList){idxList[1]}))
              idxSortItem <- sort(nameItem,index.return=TRUE)$ix
              txtItemGeoSort <- base::paste0(txtEval,'$properties$location_properties <- ',txtEval,'$properties$location_properties[idxSortItem]')
              base::eval(parse(text = txtItemGeoSort))

            }
            
            # Are there more reference locations?
            if (base::length(
              locGeoRef$properties$reference_location$properties$locations$features
            ) != 0) {
              # Go a level deeper
              idxGeoRef <- c(idxGeoRef, 1)
              next
              
            } else {
              # We found the end of the reference chain! Let's move on to the next geolocation at this same level.
              idxGeoRef[base::length(idxGeoRef)] <-
                utils::tail(idxGeoRef, 1) + 1
              
            }
          } # End loop around reference locations
          
          # Remove reference geolocations marked for deletion
          setRmvGeoRef <- base::rev(setRmvGeoRef) # Reverse order so we go from the bottom
          for (idxRmv in base::seq_len(base::length(setRmvGeoRef))) {
            idxGeoRef <- setRmvGeoRef[[idxRmv]]
            
            # Formulate the list path
            if (base::length(idxGeoRef) > 1) {
              txtEval <-
                base::paste0(
                  'locGeoRef00$properties$locations$features[[',
                  idxGeoRef[1],
                  ']]',
                  base::paste0(
                    '$properties$reference_location$properties$locations$features[[',
                    idxGeoRef[-1],
                    ']]',
                    collapse = ''
                  ),
                  ' <- NULL'
                )
            } else {
              txtEval <-
                base::paste0(
                  'locGeoRef00$properties$locations$features[[',
                  idxGeoRef[1],
                  ']] <- NULL'
                )
            }
            base::eval(parse(text = txtEval))
            
          }
          
          # Insert filtered reference locations back into the geolocation information
          if(base::is.null(locGeoRef00) && 'reference_location' %in% base::names(locGeo[[idxGeo]]$properties)){
            # Cover the scenario where the first reference location is listed as null in the location file
            locGeo[[idxGeo]]$properties['reference_location'] <- base::list(NULL)
          } else {
            locGeo[[idxGeo]]$properties$reference_location <- locGeoRef00
          }
          
          
        } # end loop around geolocations
        
        # Keep only applicable geolocations
        locGeo <- locGeo[setKeepGeo]
        loc$features[[idxLoc]]$properties$locations$features <- locGeo
        
      } # End loop around named locations
    }

    
    log$debug('Sensor location file filtered successfully.')
    
    # Write to file
    if (!base::is.null(NameFileOut)) {
      base::write(rjson::toJSON(loc, indent = 4), file = NameFileOut)
      log$debug(base::paste0('Filtered sensor location file written successfully to ',NameFileOut))
    }
    
    return(loc)
  }
