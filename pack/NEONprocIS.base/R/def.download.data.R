##############################################################################################
#' @title Download the files from Pachyderm and ECS

#' @author
#' Vislaakshi Chundru \email{vchundru@battelleecology.org}

#' @description
#' Download all the files needed for comparision both from Pachyderm and ECS

#' @param pachydermrepo String value of the pachyderm repository.
#' @param subDir String value of the directory inside the pachderm repo from which to donwload the data
#' @param site String value of the site
#' @param dpid String value of the data product id
#' @param temporalindex String value of the temporal index
#' @param startdate date value ,
#' @param enddate is optional. If there is not end assume that we need data only for the start date,
#' @param namedLocationName String value,
#' @param outputfilepath the String value of the file path where to put the final comparision output,
#' @param log = NULL
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' 
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return No output from this function other than performing the intended action.

#' @references Currently none

#' @keywords Currently none

#' @examples
#' def.download.data(pachydermrepo = "tempSurfacewater_egress", subDir = "/**", site="ARIK",dpid="DP1.20053.001", temporalindex = "005", startdate="2019-01-02", namedLocationName = "CFGLOC101670")


#' @seealso Currently none

#' @export


##############################################################################################
library(compareDF)
library(magrittr)
library(tidyr)
library(tools)
def.download.data <-
  function(pachydermrepo,
           subDir,
           site,
           dpid,
           temporalindex, 
           startdate,
           enddate = NULL,
           namedLocationName,
           outputfilepath,
           log = NULL) {
    browser()
    outputdir <- paste0(outputfilepath, "/", pachydermrepo)
    if (!dir.exists(outputdir)) {
      dir.create(outputdir)
    }
    
    
    pachydermFiles = system(
      command = paste0('pachctl list file ', pachydermrepo, '@master:', subDir),
      intern = T) %>%
      .[!grepl(pattern = "NAME", x = .)] %>%
      gsub(pattern = " file [0-9.]*[aA-zZ]{1,} ", replacement = "") %>%
      trimws()
    
    pachFilesOnly <-
      pachydermFiles[intersect(grep(temporalindex, pachydermFiles),
                               grep(namedLocationName, pachydermFiles))]
    
    transitionexeclist <- list()
    transitionexeclist[["DP1.20053.001"]] <-
      "L0_to_L1_Surface_Water_Temperature"
    if (length(pachFilesOnly) > 0) {
      for (i in 1:length(pachFilesOnly)) {
        url <-
          paste0(
            'http://pachd1.k8s.ci.neoninternal.org:600/',
            pachydermrepo,
            pachFilesOnly[i]
          )
        #https://s3.data.neonscience.org/prod-is-transition-output/provisional/dpid=DP1.20053.001/ms=2019-01/site=ARIK/ARIK_L0_to_L1_Surface_Water_Temperature_DP1.20053.001__2019-01-02.avro")
        splitpath  <-
          strsplit(pachFilesOnly[i], split = "/", fixed = TRUE)
        parquetfileName <- sapply(splitpath, tail, 1)
        avrofilenamedate <- splitpath[[1]][3]
        month_yr <- format(as.Date(avrofilenamedate), "%Y-%m")
        avrofilename <-
          paste0(site,
                 "_",
                 transitionexeclist[dpid],
                 "_",
                 dpid,
                 "__",
                 avrofilenamedate)
        avroextension <- ".avro"
        avrourl <- paste0(
          'https://s3.data.neonscience.org/prod-is-transition-output/provisional/dpid=',
          dpid,
          '/ms=',
          month_yr,
          "/site=",
          site,
          "/",
          avrofilename,
          avroextension
        )
        download.file(url, file.path(outputdir, parquetfileName), mode = "wb", method = "libcurl")
        avrofilename <- paste0(avrofilename, ".avro")
        download.file(avrourl, file.path(outputdir, avrofilename), mode = "wb", method = "libcurl")
        filelist <- list.files(path = outputdir, full.names = FALSE)
        avrofilepath <- file.path(outputdir, avrofilename)
        parquetfilepath <- file.path(outputdir, parquetfileName)
         if(file.exists(avrofilepath) && file.exists(parquetfilepath)) {
          outputfilepath <- paste0(outputdir, "/", parquetfileName , "_Output.txt")
          def.data.comp(avroFile = avrofilepath, parquetFile = parquetfilepath, 
                          temporalindex = temporalindex, namedlocname = namedLocationName, 
                          outputfilepath = outputfilepath)
          file.remove(avrofilepath)
          file.remove(parquetfilepath)
          
        }
      }
      
    } else{
      message(paste0(
        "No files found in repo: '",
        pachydermrepo,
        "', sub directory: '",
        subDir,
        "'"
      ))
    }
  }
