##############################################################################################
#' @title Get metadata/properties from sensor locations json file 

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Read sensor locations json file and return a data frame of metadata/properties 
#' filtered for a selected named location and/or install time range of interest. This function also
#' reads location-based location files (no sensor install information)

#' @param NameFile Filename (including relative or absolute path). Must be json format.
#' @param NameLoc Character value of the named location to restrict output to. Defaults to NULL, 
#' in which case no filtering is done for named location
#' @param TimeBgn POSIXct timestamp of the start time of interest (inclusive). Defaults to NULL, 
#' in which case no filtering is done for installed time range. Note that 
#' no time filtering is performed for location-based location files, since there is no sensor install 
#' information included, and only one location is included in the location-based location files.
#' @param TimeEnd POSIXct timestamp of the end time of interest (non-inclusive). Defaults to NULL, in 
#' which case the location information will be filtered for the exact time of TimeBgn. Note that 
#' no time filtering is performed for location-based location files, since there is no sensor install 
#' information included, and only one location is included in the location-based location files.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return 
#' A data frame with location metadata.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords currently none

#' @examples 
#' # Not run
#' # NameFile <- '/scratch/pfs/prt_calibrated_location_group/prt/2019/01/01/767/location/prt_767_locations.json'
#' # NameLoc <- 'CFGLOC100140'
#' # TimeBgn <- base::as.POSIXct('2019-01-01',tz='GMT)
#' # TimeEnd <- base::as.POSIXct('2019-01-02',tz='GMT)
#' # locMeta <- NEONprocIS.base::def.loc.meta(NameFile=NameFile,NameLoc=NameLoc,TimeBgn=TimeBgn,TimeEnd=TimeEnd)

#' @seealso \link[NEONprocIS.base]{def.read.avro.deve}
#'
#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-19)
#     original creation
#   Cove Sturtevant (2020-07-16)
#     output source_type and source_id from location file
#   Cove Sturtevant (2021-03-16)
#     Add parsing of active periods
#   Cove Sturtevant (2021-08-31)
#     Add error checking of location file schema
#   Cove Sturtevant (2021-11-01)
#     Remove transaction date from output
#     Add named location group to output
#   Cove Sturtevant (2022-09-22)
#     Incorporate IS default processing start date 
#     Accommodate updates to location files that ensure all possible scenarios for active periods 
#        are represented.
##############################################################################################
def.loc.meta <- function(NameFile,
                         NameLoc=NULL,
                         TimeBgn=NULL,
                         TimeEnd=NULL,
                         log=NULL
                         ){

  # Initialize log if not input
  if (is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Initialize output
  dmmyChar <- base::character(0)
  dmmyPosx <- base::as.POSIXct(dmmyChar)
  dmmyNumc <- base::numeric(0)
  rpt <- base::data.frame(name=dmmyChar,
                          site=dmmyChar,
                          install_date=dmmyPosx,
                          remove_date=dmmyPosx,
                          active_periods=dmmyChar,
                          context=dmmyChar,
                          group=dmmyChar,
                          location_id=dmmyChar,
                          location_code=dmmyChar,
                          HOR=dmmyChar,
                          VER=dmmyChar,
                          dataRate=dmmyNumc,
                          stringsAsFactors = FALSE)
  
 # There are two types of location files, asset-based and location-based. The schema must match one of these to be valid.
  # First, validate the syntax of input json to see if it is valid. This checks either type.
  #
  validateJson <-
    NEONprocIS.base::def.validate.json (NameFile,log)
  #
  # Second, validate the json against the schema only if the syntax is valid.
  # Otherwise, validateJsonSchema errors out due to the syntax error
  #
  
  validateJsonSchema <- FALSE
  
  # First try to validate the asset-based file schema
  if (validateJson == TRUE)  {
    locJsonSchema <- system.file("extdata", "locations-sensor-schema.json", package="NEONprocIS.base")
    log$debug('Checking location file against asset-based file schema.')    
    validateJsonSchema <-
      NEONprocIS.base::def.validate.json.schema (NameFile, locJsonSchema,log)
  }
  
  # Next try to validate the location-based file schema
  if (validateJson == TRUE && validateJsonSchema == FALSE)  {
    locJsonSchema <- system.file("extdata", "locations-namedLocation-schema.json", package="NEONprocIS.base")
    log$debug('Checking location file against location-based file schema.')    
    validateJsonSchema <-
      NEONprocIS.base::def.validate.json.schema (NameFile, locJsonSchema,log)
  }
  
  #if the validation fails, stop
  if ((validateJson == FALSE) || (validateJsonSchema == FALSE))
  {
    log$error(
      base::paste0(
        'In def.loc.meta::: Erred out due to the json validation failure of this file, ',
        NameFile
      )
    )
    stop("In def.loc.meta::::: Erred out due to the validation failure of the input JSON")
  }
  
  
  # Load the full json into list
  locFull <- rjson::fromJSON(file=NameFile,simplify=TRUE)
  
  # Top level properties of sensor-based location file
  if(!base::is.null(locFull$source_type)){
    srcType <- locFull$source_type
  } else {
    srcType <- NA
  }  
  if(!base::is.null(locFull$source_id)){
    srcId <- locFull$source_id
  } else {
    srcId <- NA
  }  
  
  # Properties of each named location listed in the locations file
  # Lists the named location, site, install_date, remove_date, active periods, etc
  locProp <- geojsonsf::geojson_sf(NameFile) # data frame
  if(!base::is.null(locProp$install_date)){
    locProp$install_date <- base::as.POSIXct(locProp$install_date,format='%Y-%m-%dT%H:%M:%SZ',tz='GMT')
  } else {
    locProp$install_date <- NA
  }
  if(!base::is.null(locProp$remove_date)){
    locProp$remove_date <- base::as.POSIXct(locProp$remove_date,format='%Y-%m-%dT%H:%M:%SZ',tz='GMT')
  } else {
    locProp$remove_date <- NA
  }
  if(!base::is.null(locProp$active_periods)){
    locProp$active_periods <- locProp$active_periods
  } else {
    locProp$active_periods <- NA
  }
  
  # Is there a named location and/or date range we want to restrict location info to?
  setLocProp <- base::seq_len(base::nrow(locProp))
  testTime <- base::any(!base::is.na(locProp$install_date))
  if(!base::is.null(NameLoc)){
    setLocProp <- base::intersect(setLocProp,base::which(locProp$name == NameLoc))
  }
  if(testTime && !base::is.null(TimeBgn) && (base::is.null(TimeEnd) || TimeBgn == TimeEnd)){
    setLocProp <- base::intersect(setLocProp,base::which(locProp$install_date <= TimeBgn & (base::is.na(locProp$remove_date) | locProp$remove_date > TimeBgn)))
  } else if (testTime && !base::is.null(TimeBgn) && !base::is.null(TimeEnd)){
    setLocProp <- base::intersect(setLocProp,base::which(locProp$install_date <= TimeEnd & (base::is.na(locProp$remove_date) | locProp$remove_date > TimeBgn)))
  }
  locProp <- locProp[setLocProp,]
  
  # Pull out additional properties not in the properties list but one level higher
  locPropMore <- locFull$features[setLocProp]
  
  # Expected property names that might not be there
  nameProp <- c('Required Asset Management Location ID',
                'Required Asset Management Location Code',
                'HOR',
                'VER',
                'Data Rate',
                'IS Processing Default Start Date') 
  
  # Populate the output data frame
  for(idxLoc in base::seq_len(base::nrow(locProp))){
    
    # Ensure property names available. Fill with NA otherwise.
    propFill <- base::lapply(nameProp,FUN=function(idxNameProp){
      if(base::is.null(locPropMore[[idxLoc]][[idxNameProp]])){
        return(NA)
      } else {
        return(locPropMore[[idxLoc]][[idxNameProp]])
      }
    })
    base::names(propFill) <- nameProp
    
    # format multiple values for context
    ctxt <- locProp$context[idxLoc]
    ctxt <- base::gsub(pattern='[\\[\\"]',replacement="",x=ctxt)
    ctxt <- base::gsub(pattern='\\]',replacement="",x=ctxt)
    ctxt <- base::strsplit(ctxt,',')[[1]]
    ctxt <- base::paste0(base::unique(ctxt),collapse='|')
    
    if(base::length(ctxt) == 0 || base::nchar(ctxt) == 0) {
      ctxt <- NA
    }
    
    # format multiple values for named location group
    grp <- locProp$group[idxLoc]
    grp <- base::gsub(pattern='[\\[\\"]',replacement="",x=grp)
    grp <- base::gsub(pattern='\\]',replacement="",x=grp)
    grp <- base::strsplit(grp,',')[[1]]
    grp <- base::paste0(base::unique(grp),collapse='|')
    
    if(base::length(grp) == 0 || base::nchar(grp) == 0) {
      grp <- NA
    }
    
    # Parse any active dates and place in a data frame
    if(!base::is.na(locProp$active_periods[idxLoc])){

      timeActvList <- rjson::fromJSON(json_str=locProp$active_periods[idxLoc])
      
      # Turn NULL to NA
      timeActvList <- base::lapply(timeActvList,FUN=function(timeActvIdx){
        if(base::is.null(timeActvIdx$start_date)){
          timeActvIdx$start_date <- NA
        }
        if(base::is.null(timeActvIdx$end_date)){
          timeActvIdx$end_date <- NA
        }
        return(timeActvIdx)
      }
      )
      
      numActv <- base::length(timeActvList)
      dmmyChar <- base::rep(NA,times=numActv)
      timeActv <- base::data.frame(start_date=dmmyChar,end_date=dmmyChar,stringsAsFactors = FALSE)
      for(idxActv in base::seq_len(base::length(timeActvList))){
        if(!base::is.null(timeActvList[[idxActv]]$start_date)){
          timeActv$start_date[idxActv] <- timeActvList[[idxActv]]$start_date
        }
        if(!base::is.null(timeActvList[[idxActv]]$end_date)){
          timeActv$end_date[idxActv] <- timeActvList[[idxActv]]$end_date
        }
      }

      # Convert to POSIX
      timeActv$start_date <- base::as.POSIXct(timeActv$start_date,format='%Y-%m-%dT%H:%M:%SZ',tz='GMT')
      timeActv$end_date <- base::as.POSIXct(timeActv$end_date,format='%Y-%m-%dT%H:%M:%SZ',tz='GMT')
      
      # put in a list so we can embed it in the data frame
      timeActv <- base::list(timeActv)
      
    } else {
      timeActv <- NA
    }
    
    # Parse IS processing default start date
    if(!base::is.na(propFill[['IS Processing Default Start Date']])){
      
      # Convert to POSIX
      timeProcBgnDflt <- base::as.POSIXct(propFill[['IS Processing Default Start Date']],format='%Y-%m-%dT%H:%M:%SZ',tz='GMT')

    } else {
      timeProcBgnDflt <- NA
    }
    
    rptIdx <- base::data.frame(name=locProp$name[idxLoc],
                                    site=locProp$site[idxLoc],
                                    source_type=srcType,
                                    source_id=srcId,
                                    install_date=locProp$install_date[idxLoc],
                                    remove_date=locProp$remove_date[idxLoc],
                                    context=ctxt,
                                    group=grp,
                                    active_periods=NA,
                                    IS_Processing_Default_Start_Date=timeProcBgnDflt,
                                    location_id=propFill[['Required Asset Management Location ID']],
                                    location_code=propFill[['Required Asset Management Location Code']],
                                    HOR=propFill$HOR,
                                    VER=propFill$VER,
                                    dataRate=propFill[['Data Rate']],
                                    stringsAsFactors = FALSE)
    rptIdx$active_periods <- timeActv # Add in the (potential) data frame of active periods.
    
    rpt <- base::rbind(rpt,rptIdx)
  }
  
  return(rpt)
  
}
