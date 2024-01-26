
library(rlang)
library(stringr)

library(devtools)
library(testthat)
library(usethis)
library(covr)

library(dplyr)
library(arrow)

library(readxl)
tt_csv <- read_excel("pfs/pubWb/tt.csv.xlsx")
write_csv(tt_csv, "pubWbDf.csv")

# base test
setwd("/home/NEON/choim/R/NEON-IS-data-processing/pack/NEONprocIS.base/tests/testthat")
devtools::test(pkg="/home/NEON/choim/R/NEON-IS-data-processing/pack/NEONprocIS.base")
cov <- covr::package_coverage()
report(cov)
#

# pub test
setwd("/home/NEON/choim/R/NEON-IS-data-processing/pack/NEONprocIS.pub/tests/testthat")
devtools::test(pkg="/home/NEON/choim/R/NEON-IS-data-processing/pack/NEONprocIS.pub")
cov <- covr::package_coverage()
report(cov)


# flow.pub.tabl.srf
setwd("/home/NEON/choim/R/NEON-IS-data-processing/flow/tests/testthat")

cov <-file_coverage(c(("/home/NEON/choim/R/NEON-IS-data-processing/flow/flow.pub.tabl.srf/wrap.pub.tabl.srf.R")),
                    c(("/home/NEON/choim/R/NEON-IS-data-processing/flow/tests/testthat/test-wrap-pub-tabl-srf.R"))
)
report(cov)

#
# cal test
setwd("/home/NEON/choim/R/NEON-IS-data-processing/pack/NEONprocIS.cal/tests/testthat")
devtools::test(pkg="/home/NEON/choim/R/NEON-IS-data-processing/pack/NEONprocIS.cal")
cov <- covr::package_coverage()
report(cov)

# wq test
setwd("/home/NEON/choim/R/NEON-IS-data-processing/pack/NEONprocIS.wq/tests/testthat")
devtools::test(pkg="/home/NEON/choim/R/NEON-IS-data-processing/pack/NEONprocIS.wq")
cov <- covr::package_coverage()
report(cov)
#

# qaqc test
setwd("/home/NEON/choim/R/NEON-IS-data-processing/pack/NEONprocIS.qaqc/tests/testthat")
devtools::test(pkg="/home/NEON/choim/R/NEON-IS-data-processing/pack/NEONprocIS.qaqc")
cov <- covr::package_coverage()
report(cov)
#
# stat test
setwd("/home/NEON/choim/R/NEON-IS-data-processing/pack/NEONprocIS.stat/tests/testthat")
devtools::test(pkg="/home/NEON/choim/R/NEON-IS-data-processing/pack/NEONprocIS.stat")
cov <- covr::package_coverage()
report(cov)

#
testDir = "testdata/"
testFile = "sunav2_File1.parquet"
testFilesPath <- paste0(testDir, testFile)

sunavData_254 <-NEONprocIS.base::def.read.parq(NameFile=testFilesPath, log = NULL)
sunavData_254$header_light_frame <- FALSE

sunavData_FALSE <- NEONprocIS.base::def.wrte.parq(data=sunavData_254,NameFile='sunavData_FALSE.parquet')

#

Sys.Date()

# unit test for wrap.stat.basc.R
setwd("/home/NEON/choim/R/NEON-IS-data-processing/flow/tests/testthat")
cov <-file_coverage(c(("/home/NEON/choim/R/NEON-IS-data-processing/flow/flow.stat.basc/wrap.stat.basc.R")),
                    c(("/home/NEON/choim/R/NEON-IS-data-processing/flow/tests/testthat/test-wrap-stat-basc.R")))
report(cov)
#

inputfilepaths <-  'def.file.comb.ts/validFiles/testdata.parquet'
inputfilepaths <-  'def.file.comb.ts/validFiles/tchain_CFGLOC110702_2020-01-02_flagsCal.parquet'
returnData <- NEONprocIS.base::def.read.parq(NameFile = inputfilepaths)

#
data <- data.frame(
  "source_id" =(c("19963", "19962", "19961", "19960", "19959")),
  "site_id" = c('HARV', 'CIPR', 'DDDD', 'AAAA', 'BBBB'),
  "error_state" = c(TRUE, FALSE, TRUE, FALSE, TRUE),
  "temperature" = c(99.0767, 90.0769, 97.0771, 96.0771, 98.0763),
  "resistance" = c(100.0767, 100.0769, 100.0771, 100.0771, 100.0763)
)
#class(data$source_id) ="NULL"
outFile <- file.path(workingDirPath, "testdata/more_dataType.avro")

#  myData is a data frame
#' myData <- NEONprocIS.base::def.read.avro.deve(NameFile='/scratch/test/myFile.avro',NameLib='/ravro.so')
#' attr(myData,'schema') # Returns the schema

NEONprocIS.base::def.wrte.avro.deve (
  data = data, 
  NameFile = outFile,
  NameSchm = NULL, 
  NameSpceSchm =  NULL, 
  Schm = NULL, 
  NameFileSchm = NULL, 
  NameLib = 'ravro.so')
#
foo <- data.frame(c("a", "b"), c(1, 2))
names(foo) <- c("SomeFactor", "SomeNumeric")
lapply(foo, class)
rpt <- base::.Call('readavro',NameFile,PACKAGE='ravro')
lapply(rpt, class)

# read/write  .csv
write.csv(sunavData_254, "sunavData_254.csv")
HARV_data_df <- read.csv("testdata/sunaBurst_254.csv")
#

workingDirPath <- getwd()
schmFile <- file.path(workingDirPath, FileSchm="testdata/more_dataType.avsc")

avroFile <- file.path(workingDirPath, "testdata/more_dataType.avro")

nameLib <- file.path(workingDirPath, "ravro.so")

myData <- NEONprocIS.base::def.read.avro.deve(NameFile=outFile,NameLib=nameLib)

writeSuccess = -1
writeSuccess <- NEONprocIS.base::def.wrte.avro.deve (
  data = myData, 
  NameFile = avroFile,
  NameSchm = NULL, 
  NameSpceSchm =  NULL, 
  Schm = NULL, 
  NameFileSchm = NULL, 
  NameLib = nameLib)
#
setwd("/home/NEON/choim/R/NEON-IS-data-processing/flow/tests/testthat")
cov <-file_coverage(c(("/home/NEON/choim/R/NEON-IS-data-processing/flow/flow.loc.asgn/wrap.loc.asgn.R")),
                    c(("/home/NEON/choim/R/NEON-IS-data-processing/flow/tests/testthat/test-wrap-loc-asgn.R")))
report(cov)
#
setwd("/home/NEON/choim/R/NEON-IS-data-processing/flow/tests/testthat")
cov <-file_coverage(c(("/home/NEON/choim/R/NEON-IS-data-processing/flow/flow.cal.asgn/wrap.cal.asgn.R"),
                      ("/home/NEON/choim/R/NEON-IS-data-processing/flow/flow.cal.conv/wrap.cal.conv.dp0p.R"),
                      ("/home/NEON/choim/R/NEON-IS-data-processing/flow/flow.data.comb.ts/wrap.data.comb.ts.R"),
                      ("/home/NEON/choim/R/NEON-IS-data-processing/flow/flow.loc.grp.asgn/wrap.loc.grp.asgn.R"),
                      ("/home/NEON/choim/R/NEON-IS-data-processing/flow/flow.loc.data.trnc.comb/wrap.loc.data.trnc.comb.R"),
                      ("/home/NEON/choim/R/NEON-IS-data-processing/flow/flow.loc.repo.strc/wrap.loc.repo.strc.R"),
                      ("/home/NEON/choim/R/NEON-IS-data-processing/flow/flow.qaqc.plau/wrap.qaqc.plau.R"),
                      ("/home/NEON/choim/R/NEON-IS-data-processing/flow/flow.qaqc.qm.dp0p/wrap.qaqc.qm.dp0p.R"),
                      ("/home/NEON/choim/R/NEON-IS-data-processing/flow/flow.qaqc.qm/wrap.qaqc.qm.R"),
                      ("/home/NEON/choim/R/NEON-IS-data-processing/flow/flow.rglr/wrap.rglr.R"),
                      ("/home/NEON/choim/R/NEON-IS-data-processing/flow/flow.thsh.slct/wrap.thsh.slct.R")
),
c(("/home/NEON/choim/R/NEON-IS-data-processing/flow/tests/testthat/test-wrap-cal-asgn.R"),
  ("/home/NEON/choim/R/NEON-IS-data-processing/flow/tests/testthat/test-wrap-cal-conv-dp0p.R"),
  ("/home/NEON/choim/R/NEON-IS-data-processing/flow/tests/testthat/test-wrap-data-comb-ts.R"),
  ("/home/NEON/choim/R/NEON-IS-data-processing/flow/tests/testthat/test-wrap-loc-grp-asgn.R"),
  ("/home/NEON/choim/R/NEON-IS-data-processing/flow/tests/testthat/test-wrap-loc-data-trnc-comb.R"),
  ("/home/NEON/choim/R/NEON-IS-data-processing/flow/tests/testthat/test-wrap-loc-repo-strc.R"),
  ("/home/NEON/choim/R/NEON-IS-data-processing/flow/tests/testthat/test-wrap-qaqc-plau.R"),
  ("/home/NEON/choim/R/NEON-IS-data-processing/flow/tests/testthat/test-wrap-qaqc-qm-dp0p.R"),
  ("/home/NEON/choim/R/NEON-IS-data-processing/flow/tests/testthat/test-wrap-qaqc-qm.R"),
  ("/home/NEON/choim/R/NEON-IS-data-processing/flow/tests/testthat/test-wrap-rglr.R"),
  ("/home/NEON/choim/R/NEON-IS-data-processing/flow/tests/testthat/test-wrap-thsh-slct.R")
)
)
report(cov)
#

#
NameFile = "pfs/relHumidity_qaqc_flags_group/hmp155/2020/01/02/CFGLOC101252/flags/hmp155_CFGLOC101252_2020-01-02_flagsCal.parquet"
NameFile = "pfs/relHumidity_qaqc_flags_group/hmp155/2020/01/02/CFGLOC101252/quality_metrics/hmp155_flagsPlausibility.parquet"
outFile = "pfs/out/hmp155/2020/01/02/CFGLOC101252/quality_metrics/hmp155_qualityMetrics_.parquet"

data  <- NEONprocIS.base::def.read.parq(NameFile = outFile,log = log)

data$relativeHumiditySuspectCalQF <- NA
data$dewPointSuspectCalQF <- NA

data_noReadout_time <- subset(data, select=-c(readout_time))

NEONprocIS.base::def.wrte.parq(data,NameFile='NA_flags.parquet')

# in travis.yml file for example
- Rscript -e covr::package_coverage

# after_success 
- Rscript -e covr::codecov 
# or 
- Rscript -e covr::codecov (type = "all") 

######## misc. 

as.data.frame(cov)
print(cov, group="function")

covr:::codecov()
covr:::coveralls()

covr:::trace_calls(def.read.cal.xml)
coveralls()

cov_file <- file_coverage("R/def.read.cal.xml.R", "tests/testthat/test-read-cal-xml.R")

metaCal <- read.csv("metaCal.csv")

metaCal$timeValiBgn <- as.POSIXct(metaCal$timeValiBgn,tz='GMT')
metaCal$timeValiEnd <- as.POSIXct(metaCal$timeValiEnd,tz='GMT')

TimeBgn <- base::as.POSIXct('2019-05-01 00:10:20', tz='GMT')
TimeEnd <- base::as.POSIXct('2020-03-09 00:18:28', tz='GMT')

dfReturned <-
  NEONprocIS.cal::def.cal.slct(metaCal = metaCal,
                               TimeBgn = TimeBgn,
                               TimeEnd = TimeEnd)

write.csv(calSlct, "calSlct.csv")

########### ===== to read metaCal as data frame =============================

fileCal <- c('calibration.xml', 'calibration2.xml', 'calibration3.xml', 'calibration4.xml')
fileCal_noCertNum <- c('calibration5_NoCertNum.xml')

#========= make a data frame empty =============
data_empty <-data[-c(1, 2, 3), ]

#remove a column from a data frame =============

data_lessCol <- subset(data, select = -file )

########## === to generate metaCal ====================
fileCal <- c('calibration.xml', 'calibration2.xml', 'calibration3.xml', 'calibration4.xml')
fileCal_22 <- c('calibration.xml', 'calibration2.xml', 'calibration22.xml', 'calibration3.xml', 'calibration4.xml')
fileCal = fileCal_22
#

if (base::is.null(log)) {
  log <- NEONprocIS.base::def.log.init()
}

numCal <- base::length(fileCal)

# Get the filenames of the calibration files without path information
nameFileCal <- base::unlist(base::lapply(strsplit(fileCal,'/'),utils::tail,n=1))

# intialize output
metaCal <- base::vector(mode = "list", length = numCal)

for (idxFileCal in base::seq_len(numCal)){
  
  # Read in the cal
  infoCal <- NEONprocIS.cal::def.read.cal.xml(NameFile=fileCal[idxFileCal],Vrbs=TRUE)
  
  # Error check
  if(!NEONprocIS.cal::def.validate.info.cal(infoCal,NameList=c('cal','ucrt','file','timeVali'),log=log)){
    stop()
  } else if (base::is.null(infoCal$file$StreamCalVal$CertificateNumber)) {
    log$error(base::paste0('Cannot find calibration certificate number in ',fileCal[idxFileCal],
                           '. It is not returned in infoCal$file$StreamCalVal$CertificateNumber output from NEONprocIS.cal::def.read.cal.xml'))
    stop()
  }
  
  # Look for whether this calibration is marked as suspect
  
  # output metadata
  metaCal[[idxFileCal]] <- base::data.frame(file=nameFileCal[idxFileCal],timeValiBgn=infoCal$timeVali$StartTime,timeValiEnd=infoCal$timeVali$EndTime,
                                            id=base::as.numeric(infoCal$file$StreamCalVal$CertificateNumber),stringsAsFactors=FALSE)
  
}
metaCal <- base::Reduce(f=base::rbind,x=metaCal)

########## ========== to write data frame as csv ============================ 

write.csv(metaCal, "metaCal.csv")

########## ===== Set the log level ============= 

Sys.setenv(LOG_LEVEL='debug')

warnings() 