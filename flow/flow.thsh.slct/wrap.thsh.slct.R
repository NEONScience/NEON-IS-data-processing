##############################################################################################
#' @title Threshold selection module for NEON IS data processing.

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description Wrapper function. Select thresholds for each processing day and location for sets of term and context(s)
#' among all available thresholds. 
#' The choice of constraint/threshold to use is determined by moving up the following hierarchy 
#' from finer to coarser constraints until one applies. Thus, the finest applicable level of constraint 
#' is chosen. Threshold selection order is as follows (1 being the finest possible constraint): 
#' 6. Realm, annual
#' 5. Realm, seasonal
#' 4. Site-specific, annual
#' 3. Site-specific, seasonal
#' 2. Sensor-spefific, annual
#' 1. Sensor-specific, seasonal
#'
#'
#' @param DirIn Character value. The input path to the data from a single sensor or location, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/id, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The id is the unique identifier of the sensor or location. \cr
#'
#' Nested within this path are the folders:
#'         /threshold
#'         /location 
#' The location folder holds at least 1 json file with location data/properties specific to the location 
#' identifier in the directory path. If there is more than one file in this directory, only the first will 
#' be used, since the properties of the named location (i.e. site) should be the same across files. 
#' The threshold folder contains a json file with available thresholds from which the threshold selection 
#' order will be used to choose from.
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#'
#' @param thshRaw List of thresholds to select from, as produced by reading a json formatted threshold file with 
#' rjson::fromJSON (see example)
#' 
#' @param thshPosx The list of thresholds thshRaw after passing it through NEONprocIS.qaqc::def.read.thsh.qaqc.list 
#' in order to turn the timestamps to POSIXct (see example)
#' 
#' @param ParaThsh A list of lists, each specifying a combination of term and context(s) to select thresholds for 
#' the day and location. Applicable thresholds will be selected separately for each sub-list and combined when written to file. 
#' Each sub-list contains two named elements:\cr
#' \code{Term} Character value. The term (i.e. variable) to select thresholds for. For example: 'temp'. \cr
#' \code{Ctxt} Character vector. Any number of contexts that apply to the term in order to define its application. 
#' For example, c('soil','deep') would indicate to select thresholds for term 'temp' matching both contexts 'soil', and 
#' 'deep'. Enter NULL if there are no applicable contexts, meaning that the term stands alone. 
#' Note that AND logic applies within each sub-list (matching term and all contexts).
#' 
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. not combined but carried through as-is).

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.

#' @return A directory structure created in DirOutBase, where the terminal directory of DirOutBase
#' replaces BASE_REPO of DirIn but otherwise retains the child directory structure of the input path. The filtered 
#' thresholds containing the thresholds applicable to each location, day, and sets of term and context(s) 
#' will be written to thresholds.json file in the same format as input thshRaw and placed in an additional subdirectory 
#' called 'thresholds' at the same level as the 
#' location directory. Be sure to specify "DirSubCopy=location" to pass the location directory through
#' to the output (along with any other desired subdirectories to copy over. Otherwise, they will be dropped. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Not run
#' # Open and parse the thresholds file (with all available thresholds)
#' FileThsh <- "~/pfs/tempSoil_threshold_filter/thresholds.json"
#' thshRaw <- rjson::fromJSON(file=FileThsh,simplify=TRUE),silent=FALSE)
#' thsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(listThsh=thshRaw) # This turns dates to POSIXct, which is required
#' ParaThsh <- list(list(Term='temp',Ctxt='soil')) 
#' wrap.thsh.slct(DirIn="~/pfs/tempSoil_locations/prt/2020/01/01/CFGLOC12345",
#'                DirOutBase="~/pfs/out",
#'                thsh=thsh,
#'                ParaThsh=ParaThsh,
#'                DirSubCopy='location'
#' )

#' @seealso None currently

# changelog and author contributions / copyrights
#   Cove Sturtevant (2021-09-02)
#     Convert flow script to wrapper function
##############################################################################################
wrap.thsh.slct <- function(DirIn,
                           DirOutBase,
                           thshRaw,
                           thshPosx,
                           ParaThsh,
                           DirSubCopy=NULL,
                           log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Check that thshRaw and thshPosx are the same length
  if(base::length(thshRaw) != base::length(thshPosx)){
    log$fatal('Inputs thshRaw and thshPosx to module wrapper function wrap.thsh.slct must be the same length. See function documentation.')
    stop()
  }
  
  # Get directory listing of input directory. 
  DirLoc <- base::paste0(DirIn,'/location')
  fileLoc <- base::dir(DirLoc)
  numFileLoc <- base::length(fileLoc)
  
  # If there is not at least one file for locations, quit
  if(numFileLoc == 0){
    log$error(base::paste0('No location data found in ',DirLoc))
    stop()
  }
  
  if(numFileLoc > 1){
    log$debug(base::paste0('There is more than location file in ',DirLoc,'. Using ',fileLoc[1]))
    fileLoc <- fileLoc[1]
  }
  
  # Create the base output directories 
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  dirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
  dirOutThsh <- base::paste0(dirOut,'/threshold')
  base::dir.create(dirOutThsh,recursive=TRUE)
  
  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirSubCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(base::paste0(DirIn,'/',DirSubCopy),dirOut,log=log)
  }  
  
  # The time frame of the data is one day, and this day is indicated in the directory structure. 
  timeBgn <-  InfoDirIn$time
  
  # Error check
  if(base::is.na(timeBgn)){
    # Generate error and stop execution
    log$error(base::paste0('Cannot interpret date from directory structure of datum path: ',DirIn)) 
    stop()
  }    
  
  # Grab the site from the location file
  nameLoc <- utils::tail(InfoDirIn$dirSplt,1) # Location identifier from directory path
  # Read the location data
  loc <- NEONprocIS.base::def.loc.meta(NameFile=base::paste0(DirLoc,'/',fileLoc),NameLoc=nameLoc)
  numLoc <- base::nrow(loc)
  if(numLoc == 0){
    log$error(base::paste0('No locations match ',nameLoc,' in location file ', fileLoc, 
                           ' as part of processing datum path: ',DirIn,
                           '. Cannot determine site for this named location.')) 
    stop()
  }
  
  # Grab the site
  site <- loc$site[1]
  
  # For each term|context grouping, select the appropriate thresholds
  setThshSlct <- base::lapply(ParaThsh,function(idxParaThsh){
    NEONprocIS.qaqc::def.thsh.slct(thsh=thshPosx,
                                   Time=timeBgn,
                                   Term=idxParaThsh$Term,
                                   Ctxt=idxParaThsh$Ctxt,
                                   Site=site,
                                   NameLoc=nameLoc,
                                   RptThsh=FALSE,
                                   log=log
                                   )
  })
  
  # Combine selected thresholds from all term|context groupings
  setThshSlct <- base::unique(base::unlist(setThshSlct))
  
  # Write the filtered threshold file
  fileOutThsh <- base::paste0(dirOutThsh,'/thresholds.json')
  thshSlct <- base::list(thresholds=thshRaw[setThshSlct])
  base::write(rjson::toJSON(thshSlct,indent=3),file=fileOutThsh)
  log$info(base::paste0('Selected thresholds written to ',fileOutThsh))
  
  return()
}
