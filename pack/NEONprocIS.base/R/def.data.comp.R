##############################################################################################
#' @title Copy all files in a directory with a symbolic link

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Copy all files in a source directory to a destination directory with a symbolic link 

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
#library(arsenal)
library(tidyr)
def.data.comp <- function(avroFile, parquetFile,temporalindex, namedlocname, outputfilepath,log=NULL){
  
  browser()
  
  orignialAvroData <- NEONprocIS.base::def.read.avro.deve(NameFile = avroFile, NameLib ="/home/NEON/vchundru/git/NEON-IS-data-processing/pack/NEONprocIS.base/ravro.so")
  
  parquetData <- NEONprocIS.base::def.read.parq(NameFile = parquetFile)
  
  avrodata <- subset(orignialAvroData, (temporalIndex == temporalindex & namedLocationName == namedlocname ))
  
  avrodata$numValue = rowSums(cbind(avrodata$doubleValue, avrodata$intValue), na.rm = TRUE)
  
  avrodata <- pivot_wider(data = avrodata, id_cols = c("startDate", "endDate"), names_from = "termName", values_from ="numValue")
  
  names(avrodata)[names(avrodata) == "startDate"] <- "startDateTime"
  
  names(avrodata)[names(avrodata) == "endDate"] <- "endDateTime"
  
 #neededAvroData <- subset(avrodata, select = (names(parquetData)))
  
  neededAvroData <- subset(avrodata, select = (intersect(names(avrodata), names(parquetData))))
 
 # out <- capture.output(summary(comparedf(neededAvroData, parquetData, int.as.num = TRUE, tolerance = 1E-5, tolerance_type = difference)))
  out  <- capture.output(summary(compareDF::compare_df(df_new = neededAvroData, df_old = parquetData, group_col = "endDateTime", tolerance = 0.001)))
  
  #cat(out,file="/home/NEON/vchundru/statsOutput.txt",sep="\n",append=TRUE)
  cat(out,file = outputfilepath,sep="\n",append=TRUE)
  
  #NEONprocIS.base::def.data.comp(avroFile="/home/NEON/vchundru/pfs/ARIK_L0_to_L1_Surface_Water_Temperature_DP1.20053.001__2019-01-02.avro",  parquetFile = "/home/NEON/vchundru/pfs/tempSurfacewater_2019-01-02_CFGLOC101670_basicStats_005.parquet", temporalindex = "005", namedlocname = "CFGLOC101670")
  
}
