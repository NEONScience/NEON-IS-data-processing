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
#' def.dir.copy.symb(DirSrc='/scratch/pfs/proc_group/prt/27134/2019/01/01',DirDest='pfs/out/prt/27134/2019/01/01')


#' @seealso Currently none

#' @exportdata:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACgAAAAkCAYAAAD7PHgWAAAA00lEQVR42mNgGAWjYBQMUxAauorZLL452TyhZQUtMMhs47QGLrIdaJ7QmtSyeP+5fTc//N98+e1/agGQWSvOvPqfNGHnRbO4lnjyHRjfvHzvzff/Zx5+/r9x60OqORBkFgg3bHnw1yy+ZQkFIdiyAuRbmIHUdiAIg+wYdeCoA0cdOOrAUQdSyYG0wKMOHHUgOQ6kNGOMOhCXpaMOHHXgiHTgSmDva9A6ENRvTejfcYFWDkzs33kBZAfZDvTMncQO6huDup+06rhbhvZxjg6RjILBDgAZYqbbTdtPRgAAAABJRU5ErkJggg==


##############################################################################################
library(compareDF)
library(magrittr)
library(tidyr)
library(tools)
def.download.data <- function(pachydermrepo, subDir, namedLocationName, log = NULL) {
  browser()
  outputdir <- paste0(getwd(), "/", pachydermrepo)
  dir.create(outputdir)
  
  pachydermFiles = system(
    command = paste0('pachctl list file ', pachydermrepo, '@master:', subDir),
    intern = T
  ) %>%
    .[!grepl(pattern = "NAME", x = .)] %>%
    gsub(pattern = " file [0-9.]*[aA-zZ]{1,} ", replacement = "") %>%
    trimws()
  pachFilesOnly <- pachydermFiles[intersect(grep("005",pachydermFiles), grep("CFGLOC101670",pachydermFiles))]

  if (length(pachFilesOnly) > 0) {
    for (i in 1:length(pachFilesOnly)) {
        url <-
          paste0(
            'http://pachd1.k8s.ci.neoninternal.org:600/',
            pachydermrepo,
            pachFilesOnly[i]
          )
        splitpath  <- strsplit(pachFilesOnly[i], split ="/", fixed = TRUE)
        filename <- sapply(splitpath, tail, 1)
        parquetfileName  <- tools::file_path_sans_ext(filename)
        temp <-
          tempfile(pattern = parquetfileName,
                   tmpdir = outputdir,
                   fileext = ".parquet")
        download.file(url, temp)
        
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
