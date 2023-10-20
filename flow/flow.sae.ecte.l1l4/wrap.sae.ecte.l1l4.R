##############################################################################################
#' @title Calling L1L4 workflow in eddy4r. 

#' @param DirIn Character value. The input path to the data from a single sensor ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/site-code, 
#' where # indicates any number of parent and child directories of any name, so long as they are not 
#' 'pfs', 'location',or recognizable as the 'yyyy/mm/dd' structure which indicates the 4-digit year, 
#' 2-digit month, and 2-digit day of the data contained in the folder. The data day is identified from 
#' the input path. The site-code is the unique identifier of the site. \cr
#' 
#' The input path path holds a json file holding the location data and a gzipped h5 file holding l0p data.
#' 
#' For example, for site CPER:
#' Input path = /scratch/pfs/turbulent_l0p/2019/01/01/CPER/ with files:\cr
#'    NEON.D10.CPER.IP0.00200.001.ecte.2010-01-01.l0p.h5\cr
#'    CPER.json\cr
#'    
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#' 
#' @return A h5 file in DirOutBase
#'  

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Not run
#' wrap.loc.strc.repo(DirIn="/pfs/tempSoil_context_group/prt/2018/01/01",DirOutBase="/pfs/out",Comb=TRUE)

#' @seealso None
#' 
# changelog and author contributions / copyrights
#     original creation
##############################################################################################
wrap.sae.ecte.l1l4 <- function(DirIn,
                               DirOutBase,
                               DirTmp,
                               log=NULL
                      ){

  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init(Lvl="debug")
  }

  # Gather info about the input directory and formulate the parent output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn,log=log)
  idSrc <- utils::tail(InfoDirIn$dirSplt,1)
  dirOutPrnt <- base::paste0(c(DirOutBase,InfoDirIn$dirSplt[(InfoDirIn$idxRepo+1):(base::length(InfoDirIn$dirSplt)-1)]),collapse='/')
  
  log$debug(InfoDirIn$time)  # '2020/01/04 GMT'
  log$debug(idSrc)  # 'ABBY'
  log$debug(dirOutPrnt)

  # Get site location file
  fsite <- list.files(path=DirIn, pattern="\\.json")

  # define requested properties
  nameProp <- c('DistZaxsCnpy', 'DistZaxsDisp', 'PrdIncrCalc', 'PrdIncrPf', 'PrdWndwCalc', 'PrdWndwPf',
                'PresAtmSite', 'TypeEco', 'UTM Zone')
  # Load in the location json
  source("./def.loc.meta.R")
  meta <- def.loc.meta(NameFile=base::paste0(DirIn,'/',fsite), nameProp=nameProp, log=log)
  planarWindow <- as.numeric(meta$PrdWndwPf)
  offDays <- planarWindow %/% 2
  
  # retrieve all data path for planar fit window files
  source("./util.sae.ecte.l1l4.R")
  allPaths <- get.all.days.path(DirIn, InfoDirIn$time, offDays)

  # get current date
  currDate <- format(as.POSIXct(InfoDirIn$time, format="%Y-%m-%d %Z"), format="%Y-%m-%d")
  
  # get input files and copy to DirTmp (/tmp/pfs/)
  dir.create(DirTmp, showWarnings = FALSE, recursive=TRUE)
  for (i in seq_along(allPaths)) {
    date <- names(allPaths)[i]
    path <- allPaths[[i]]
    fdata <- list.files(path=path, pattern="\\.h5.gz", full.names=TRUE)
    if (length(fdata) == 0) {
      log.warn(paste0("ECTE L0p file not available for date ", date))
      return()
    }
    
    file.copy(fdata, DirTmp)
    R.utils::gunzip(file.path(DirTmp, basename(fdata)))
    
    # Rename the files
    target_name <- file.path(DirTmp, paste0("ECTE_dp0p_", idSrc, "_", date, ".h5"))
    file.rename(file.path(DirTmp, tools::file_path_sans_ext(basename(fdata))), target_name)
  }
  log$debug(list.files(path=DirTmp))
  fileBase <- gsub("^(.*?\\.ecte).*", "\\1", basename(fdata))
  fileoutBase <- gsub("IP0", "IP4", fileBase)

  library(lgr)
  Sys.setenv(
    "PRDWNDWPF" = planarWindow,
    "PRDWNDWCALC" = as.numeric(meta$PrdWndwCalc),
    "PRDINCRPF" = as.numeric(meta$PrdIncrPf),
    "PRDINCRCALC" = as.numeric(meta$PrdIncrCalc),
    "DIRINP" = DirTmp,
    "DIROUT" = dirOutPrnt,
    "DIRMNT" = DirTmp,
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
    "LOG_LEVEL" = names(lgr::get_log_levels()[lgr::get_log_levels() == log$threshold])
  )
  
  # call ECTE workflow
  log$info("Calling eddy4R ECTE L1-L4 Planar fit window workflow...")
  source("/home/eddy/NEON-FIU-algorithm/flow/flow.turb/flow.turb.tow.neon.dp04.r")
  log$info("ECTE workflow finished.")
}
