##############################################################################################
#' @title Get metadata/properties from groups json file 

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Read group json file and return a data frame of metadata/properties 
#' for the group.

#' @param NameFile Filename (including relative or absolute path). Must be json format.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return 
#' #' A data frame with group metadata.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords currently none

#' @examples 
#' # Not run
#' # NameFile <- "/scratch/pfs/group_loader/pressure-air/rel-humidity_HARV000060.json"
#' # grpMeta <- NEONprocIS.base::def.grp.meta(NameFile=NameFile)

#' @seealso None
#'
#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2022-11-20)
#     original creation

##############################################################################################
def.grp.meta <- function(NameFile,log=NULL){

  # Initialize log if not input
  if (is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Initialize output
  dmmyChar <- base::character(0)
  dmmyPosx <- base::as.POSIXct(dmmyChar)
  dmmyNumc <- base::numeric(0)
  rpt <- base::data.frame(name=dmmyChar,
                          active_periods=dmmyChar,
                          group=dmmyChar,
                          HOR=dmmyChar,
                          VER=dmmyChar,
                          product=dmmyChar,
                          stringsAsFactors = FALSE)
  
  # First, validate the syntax of input json to see if it is valid. 
  validateJson <-
    NEONprocIS.base::def.validate.json (NameFile)
  
  # Second, validate the json against the schema only if the syntax is valid.
  # Otherwise, validateJsonSchema errors out due to the syntax error
  #

  validateJsonSchema <- TRUE
  if (validateJson == TRUE)  {
    grpJsonSchema <- system.file("extdata", "group-member-schema.json", package="NEONprocIS.base")
    log$debug('Checking groups file against expected schema.')
    validateJsonSchema <-
      NEONprocIS.base::def.validate.json.schema (NameFile, grpJsonSchema)
  }
  
  
  #if the validation fails, stop
  if ((validateJson == FALSE) || (validateJsonSchema == FALSE))
  {
    log$error(
      base::paste0(
        'In def.grp.meta::: Erred out due to the json validation failure of this file, ',
        NameFile
      )
    )
    stop("In def.grp.meta::::: Erred out due to the validation failure of the input JSON")
  }
  
  
  # Load the full json into list
  grpFull <- rjson::fromJSON(file=NameFile,simplify=TRUE)

  # Pull out additional properties not in the properties list but one level higher
  grpProp <- geojsonsf::geojson_sf(NameFile)
  
  # Expected property names that might not be there
  nameProp <- c('name',
                'group',
                'active_periods',
                'HOR',
                'VER',
                'products') 
  
  # Populate the output data frame
  for(idxGrp in base::seq_len(base::nrow(grpProp))){
    
    # Ensure property names available. Fill with NA otherwise.
    prop <- base::lapply(nameProp,FUN=function(idxNameProp){
      if(base::is.null(grpProp[idxGrp,idxNameProp])){
        return(NA)
      } else {
        return(grpProp[idxGrp,idxNameProp])
      }
    })
    base::names(prop) <- nameProp
    
    # Parse any active dates
    if(!base::is.na(prop$active_periods)){

      timeActvList <- rjson::fromJSON(json_str=prop$active_periods)
      
      # Turn NULL to NA
      timeActvList <- lapply(timeActvList,FUN=function(timeActvIdx){
        if(base::is.null(timeActvIdx$start_date)){
          timeActvIdx$start_date <- NA
          }
        if(base::is.null(timeActvIdx$end_date)){
          timeActvIdx$end_date <- NA
        }
        return(timeActvIdx)
      }
      )
      
      timeActvChar <- base::unlist(timeActvList)
      typeTime <- base::names(timeActvChar)
      numBgn <- base::sum(typeTime=='start_date')
      numEnd <- base::sum(typeTime=='end_date')
      dmmyChar <- base::rep(NA,times=base::max(numBgn,numEnd))
      
      # Make a data frame
      timeActv <- base::data.frame(start_date=dmmyChar,end_date=dmmyChar,stringsAsFactors = FALSE)
      timeActv$start_date[base::seq_len(numBgn)] <- timeActvChar[typeTime=='start_date']
      timeActv$end_date[base::seq_len(numEnd)] <- timeActvChar[typeTime=='end_date']
      
      # Convert to POSIX
      timeActv$start_date <- base::as.POSIXct(timeActv$start_date,format='%Y-%m-%dT%H:%M:%SZ',tz='GMT')
      timeActv$end_date <- base::as.POSIXct(timeActv$end_date,format='%Y-%m-%dT%H:%M:%SZ',tz='GMT')
      
      # put in a list so we can embed it in the data frame
      timeActv <- base::list(timeActv)
      
    } else {
      timeActv <- NA
    }
    
    # HOR and VER are higher-level properties. Grab them.
    propMore <- grpFull$features[[idxGrp]]
    if(!base::is.null(propMore$HOR)){
      prop$HOR <- propMore$HOR
    }
    if(!base::is.null(propMore$VER)){
      prop$VER <- propMore$VER
    }
    
    # Consolidate the output
    rptIdx <- base::data.frame(member=prop$name,
                               group=prop$group,
                               active_periods=NA,
                               HOR=prop$HOR,
                               VER=prop$VER,
                               products=prop$products,
                               stringsAsFactors = FALSE)
    rptIdx$active_periods <- timeActv # Add in the (potential) data frame of active periods.
    
    rpt <- base::rbind(rpt,rptIdx)
  }
  
  return(rpt)
  
}
