##############################################################################################
#' @title Download the files from Pachyderm and ECS

#' @author
#' Vislaakshi Chundru \email{vchundru@battelleecology.org}

#' @description
#' Download all the files needed for comparision both from Pachyderm and ECS

#' @param DirSrc String vector. Source directories. All files in these source directories will be copied to the
#' destination directories.
#' @param DirDest String value or vector. Destination director(ies). If not of length 1, must be same length
#' as DirDest, each index corresponding to the same index of DirDest. NOTE: DirDest should be the parent of the
#' distination directories. For example, to create a link from source /parent/child/ to /newparent/child,
#' DirSrc='/parent/child/' and DirDest='/newparent/'
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return No output from this function other than performing the intended action.

#' @references Currently none

#' @keywords Currently none

#' @examples
#' def.download.data(pachydermrepo = "tempSurfacewater_egress", subDir = "/**", site="ARIK",dpid="DP1.20053.001", temporalindex = "005", startdate="2019-01-02", namedLocationName = "CFGLOC101670")


#' @seealso Currently none

#' @exportdata:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACgAAAAkCAYAAAD7PHgWAAAA00lEQVR42mNgGAWjYBQMUxAauorZLL452TyhZQUtMMhs47QGLrIdaJ7QmtSyeP+5fTc//N98+e1/agGQWSvOvPqfNGHnRbO4lnjyHRjfvHzvzff/Zx5+/r9x60OqORBkFgg3bHnw1yy+ZQkFIdiyAuRbmIHUdiAIg+wYdeCoA0cdOOrAUQdSyYG0wKMOHHUgOQ6kNGOMOhCXpaMOHHXgiHTgSmDva9A6ENRvTejfcYFWDkzs33kBZAfZDvTMncQO6huDup+06rhbhvZxjg6RjILBDgAZYqbbTdtPRgAAAABJRU5ErkJggg==


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
           log = NULL) {
    browser()
    outputdir <- paste0(getwd(), "/", pachydermrepo)
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
