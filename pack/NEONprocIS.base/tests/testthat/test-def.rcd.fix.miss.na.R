# # library(testthat)
# # source("R/def.rcd.fix.miss.na.R")
# 
# test_that("Output path with prefix filename suffix and extension",
#           {
#              
#                                    
#             readout_time = as.Date(c(base::as.POSIXct('2019-06-12 00:10:20', tz = 'GMT'),
#                                      base::as.POSIXct("2012-01-01", tz = 'GMT'),
#                                      base::as.POSIXct("2013-09-23", tz = 'GMT'),
#                                      base::as.POSIXct("2014-11-15", tz = 'GMT'), 
#                                      base::as.POSIXct("2015-03-27", tz = 'GMT')))
#             output_filename <- NEONprocIS.base::def.rcd.fix.miss.na(nameFileIn='myFileName.json',prfx='Prefix_',sufx='_Suffix',ext='txt')
#             testthat::equals(output_filename, 'Prefix_myFileName_Suffix.txt')
#           }
# 
# )
# 
# test_that("Output when there is no extension in the nameFileIn",
#           {
# 
#             output_filename <- NEONprocIS.base::def.rcd.fix.miss.na(nameFileIn='myFileName',prfx='Prefix_',sufx='_Suffix',ext='txt')
#             testthat::equals(output_filename, 'Prefix_myFileName_Suffix.txt')
#           }
# 
# )
# 
# test_that("Output when there is no suffix",
#           {
# 
#             output_filename <- NEONprocIS.base::def.rcd.fix.miss.na(nameFileIn='myFileName',prfx='Prefix_',ext='txt')
#             testthat::equals(output_filename, 'Prefix_myFileName.txt')
#           }
# 
# )
# 
# test_that("Output when there is no prefix and suffix",
#           {
# 
#             output_filename <- NEONprocIS.base::def.rcd.fix.miss.na(nameFileIn='myFileName',ext='txt')
#             testthat::equals(output_filename, 'Prefix_myFileName.txt')
#           }
# 
# )
# 
# test_that("Output when there are multiple periods in the filenameIn",
#           {
#             
#             output_filename <- NEONprocIS.base::def.rcd.fix.miss.na(nameFileIn='myFileName.ext1.ext2.ext3', prfx='Prefix_',sufx='_Suffix', ext='txt')
#             testthat::equals(output_filename, 'Prefix_myFileName.ext1.ext2_Suffix.txt')
#           }
#           
# )
# 
