##############################################################################################
#' @title Calling L1L4 workflow in eddy4r. 

#' @param DirIn Character value. The input path to the data and location info from a site
#' 
#' The input path path contains a json file holding the location data and some gzipped h5 files holding l0p data.
#' 
#' For example, for site CPER:
#' Input path = /scratch/pfs/turbulent_l0p/20209/01/05/CPER/ with nested files:\cr
#'    data/NEON.D10.CPER.IP0.00200.001.ecte.2020-01-01.l0p.h5\cr
#'    ... ... ...
#'    data/NEON.D10.CPER.IP0.00200.001.ecte.2020-01-05.l0p.h5\cr
#'    ... ... ...
#'    data/NEON.D10.CPER.IP0.00200.001.ecte.2020-01-09.l0p.h5\cr
#'    location/CPER.json\cr
#'    
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#' 
#' @return L1-L4 basic and expanded h5 files
#'  

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Not run
#' wrap.sae.ecte.l1l4(DirIn="/pfs/turbulent_ts_pad/2020/01/05/ABBY",DirOutBase="/pfs/out/turbulent_l1l4")

# changelog and author contributions / copyrights
#     original creation
##############################################################################################

wrap.sae.ecte.l1l4 <- function(DirIn,
                               DirOutBase,
                               log=NULL
                      ){

  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init(Lvl="debug")
  }

  # Gather info about the input directory and formulate the parent output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn,log=log) 
  idSrc <- utils::tail(InfoDirIn$dirSplt,1)
  dirOutPrnt <- base::paste0(c(DirOutBase,InfoDirIn$dirSplt[(InfoDirIn$idxRepo+1):(base::length(InfoDirIn$dirSplt))]),collapse='/')
  dataDirIn <- file.path(DirIn, "data")
  currDate <- as.character(InfoDirIn$time)

  log$debug(currDate)  # current date: '2020-01-05'
  log$debug(idSrc)  # 'ABBY'
  log$debug(dirOutPrnt) # '/pfs/out/turbulent_l1l4/2020/01/05/ABBY'

  # Get site location file
  fileLoc <- list.files(path=file.path(DirIn, "location"), pattern="\\.json", full.names=TRUE)
  log$debug(paste("location file: ",fileLoc))

  if(length(fileLoc) == 0) {
    log$warn(paste0("No location data under ", DirIn, "/location. Skipping..."))
    stop()
  }

  # define requested properties
  nameProp <- c('DistZaxsCnpy', 'DistZaxsDisp', 'PrdIncrCalc', 'PrdIncrPf', 'PrdWndwCalc', 'PrdWndwPf',
                'PresAtmSite', 'TypeEco', 'UTM Zone')
  # Load in the location json 
  # TODO: current location file doesn't include site level (geolocation) properties, need improvement
  # TODO: modify def.loc.meta.R function in NEONprocIS.base, or create a new function to retrieve site level properties
  source("./def.loc.meta.R")
  meta <- def.loc.meta(NameFile=fileLoc[[1]], nameProp=nameProp, log=log)
  planarWindow <- as.numeric(meta$PrdWndwPf)
  offDays <- planarWindow %/% 2
  
  # check if number of files equals to planarWindow
  ### TODO do not zip l0p file in pachyderm??
  file_in <- list.files(path=dataDirIn, pattern=".\\.h5.gz", full.names=TRUE)
  log$debug(paste0("number of zipped l0p files: ",length(file_in)))
  if (length(file_in) < planarWindow) {
    log$error("one or more l0p files are missing")
    stop()
  }
  
  # retrieve substr before "ecte" from input file name 
  fileBase <- gsub("^(.*?\\.ecte).*", "\\1", basename(file_in[[1]]))
  fileoutBase <- gsub("IP0", "IP4", fileBase)
  log$debug(paste("output file base: ", fileoutBase)) # 'NEON.D16.ABBY.IP4.00200.001.ecte'
  
  # decompress and rename input data files
  for (tmpfile in file_in) {
    log$debug(tmpfile)
    tmpdate <- extract_date_from_file(tmpfile)
    target_name <- file.path(dataDirIn, paste0("ECTE_dp0p_", idSrc, "_", tmpdate, ".h5"))
    log$debug(target_name)
    R.utils::gunzip(tmpfile, destname=target_name)
  }
  log$debug(list.files(path=dataDirIn, pattern=".\\.h5", full.names=TRUE))
  
  # call eddy4r workflow and calculate L1L4 for current date 
  log$info(paste0("Process L1L4 against date ", currDate))
  log$info(paste0("data files input from: ", dataDirIn))
  log_level = names(lgr::get_log_levels()[lgr::get_log_levels() == log$threshold])

  Sys.setenv(
    "PRDWNDWPF" = planarWindow,
    "PRDWNDWCALC" = as.numeric(meta$PrdWndwCalc),
    "PRDINCRPF" = as.numeric(meta$PrdIncrPf),
    "PRDINCRCALC" = as.numeric(meta$PrdIncrCalc),
    "DIRINP" = dataDirIn,
    "DIROUT" = dirOutPrnt,
    "DIRMNT" = dataDirIn,
    "DIRTMP" = "NA",
    "DIRWRK" = "NA",
    "VERSDP" = "001",
    "READ" = "hdf5",
    "FILEOUTBASE" = fileoutBase,
    "DATEOUT" = currDate,
    "METH" = "host",
    "NAMEDATAEXT" = "NA",
    "OUTMETH" = "NA",
    "OUTSUB" = "NA",
    "FILEINP" = "NA",
    "VERSEDDY" = "dp04",
    "LOG_LEVEL" = log_level
  )
  
  # call ECTE workflow
  log$info("Calling eddy4R ECTE L1-L4 Planar fit window workflow...")
  source("/home/eddy/NEON-FIU-algorithm/flow/flow.turb/flow.turb.tow.neon.dp04.r")
  log$info("ECTE workflow finished.")
}


extract_date_from_file <- function(tmp_file) {
  # Define a regular expression pattern to match the date
  date_pattern <- "\\d{4}-\\d{2}-\\d{2}"
  
  # Use the regexpr function to find the date in the string
  date_match <- regexpr(date_pattern, tmp_file)
  
  if (date_match == -1) {
    # Return NULL if no date is found
    return(NULL)
  } else {
    # Extract the matched date from the string
    matched_date <- substring(tmp_file, date_match, date_match + attr(date_match, "match.length") - 1)
    return(matched_date)
  }
}
