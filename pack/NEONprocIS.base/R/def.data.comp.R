##############################################################################################
#' @title Copy all files in a directory with a symbolic link

#' @author 
#' Visalakshi Chundru \email{vchundru@battelleecology.org}

#' @description 
#' Definition function. Copy all files in a source directory to a destination directory with a symbolic link 

#' @param avroFile String value. Source path of the avro file.
#' @param parquetFile String value. Source path of the parquet file.
#' @param temporalindex String value of the temporal Index (TMI) timing.
#' @param namedlocname String value of the particular namedlocname
#' @param outputfilepath String value of the path where to put the output of the comparision.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log

#' @return No output from this function other than performing the intended action.  

#' @references Currently none

#' @keywords Currently none

#' @examples 
#' def.data.comp <- function(avroFile = "~/git/NEON-IS-data-processing/, parquetFile,temporalindex = "005", namedlocname="", outputfilepath="",log=NULL)


#' @seealso Currently none

#' @export


##############################################################################################
#library(compareDF)
library(arsenal)
library(tidyr)
library(dplyr)
def.data.comp <- function(avroFile, parquetFile,temporalindex, namedlocname, outputfilepath,log=NULL){
  
  browser()
  
  orignialAvroData <- NEONprocIS.base::def.read.avro.deve(NameFile = avroFile, NameLib ="/home/NEON/vchundru/git/NEON-IS-data-processing/pack/NEONprocIS.base/ravro.so")
  
  parquetData <- NEONprocIS.base::def.read.parq(NameFile = parquetFile)
  
  numericCols <- unlist(lapply(parquetData, is.numeric)) 
  
  avrodata <- subset(orignialAvroData, (temporalIndex == temporalindex & namedLocationName == namedlocname ))
  
  avrodata$numValue = rowSums(cbind(avrodata$doubleValue, avrodata$intValue), na.rm = TRUE)
  
  avrodata <- pivot_wider(data = avrodata, id_cols = c("startDate", "endDate"), names_from = "termName", values_from ="numValue")
  
  names(avrodata)[names(avrodata) == "startDate"] <- "startDateTime"
  
  names(avrodata)[names(avrodata) == "endDate"] <- "endDateTime"
  
  neededAvroData <- subset(avrodata, select = (intersect(names(avrodata), names(parquetData))))
 
  out <- capture.output(summary(comparedf(neededAvroData, parquetData, int.as.num = TRUE, tol.num.val = 1E-5)))
 
  cat(out,file = outputfilepath,sep="\n",append=TRUE)
  
  
}
