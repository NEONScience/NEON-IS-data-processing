##############################################################################################
#' @title Unit test of def.rcd.miss.na.R, which identifies any inconsistent or corrupt timeseries records across a set of files
#' 
#' @description
#' Given a set of file paths containing timeseries data, identify any timestamps
#' that are not consistently present across all files. Also identify any records that contain NA 
#' values.

#' @param fileData Character vector of any length, consisting of the full or relative paths to all
#' the data files to evaluate together for consistent timestamps and any NA-containing records. The
#' files must be avro or parquet files containing tabular data. These files will be read by 
#' NEONprocIS.base::def.read.avro.deve or NEONprocIS.base::def.read.parq, which will return a data frame. 
#' One column of the data frame must be readout_time.

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A list of \code{timeAll} and \code{timeBad}, each data frames with one column:
#' \code{readout_time} POSIXct timestamps signifying... for timeAll) the union of all encountered 
#' timestamps across the set of input files, and for timeBad) inconsistent or corrupt (NA-containing) 
#' records across the set of input files

#' @references Currently none

#' @keywords Currently none

#' @examples
#' NEONprocIS.base::def.rcd.miss.na(nameFile=c('/path/to/file1.avro','/another/path/to/file2.avro'))

#' @seealso \link[NEONprocIS.base]{def.read.avro.deve}

#' @export

# changelog and author contributions / copyrights
#   Mija Choi (2022-06-20)
#     revised to add a negative test
##############################################################################################
library(rapportools)

test_that("retrun extra timestamp in the second file",
          {

            inputfilepaths <- c('def.rcd.miss.na/valid_files/testdata.parquet', 'def.rcd.miss.na/valid_files/testflagsdata.parquet')

            returnList <-NEONprocIS.base::def.rcd.miss.na(fileData=inputfilepaths)
            testthat::expect_true(is.list(returnList))
            expect_true (length(returnList) == 2)
            testthat::equals(length(returnList$timeAll$readout_time), 6)
            testthat::equals(returnList$timeBad$readout_time[1], "2019-01-01 07:00:05 GMT")

          }

)

test_that("one txt, the other parquet file format",
          {

            inputfilepaths <- c('def.rcd.miss.na/second_test/textformat.txt', 'def.rcd.miss.na/valid_files/testflagsdata.parquet')
            returnList <-try(NEONprocIS.base::def.rcd.miss.na(fileData=inputfilepaths),
                             silent = TRUE)
            testthat::expect_true((class(returnList)[1] == "try-error"))
          }

)

test_that("avro file and the other parquet format",
          {
            
            inputfilepaths <- c('def.rcd.miss.na/prt_test.avro', 'def.rcd.miss.na/valid_files/testflagsdata.parquet')
            returnList <-try(NEONprocIS.base::def.rcd.miss.na(fileData=inputfilepaths),
                             silent = TRUE)
            testthat::expect_true((class(returnList)[1] == "try-error"))
          }
          
)

test_that("missing readout_time in one of the files",
          {

            inputfilepaths <- c('def.rcd.miss.na/third_test/incorrecttestdata.parquet', 'def.rcd.miss.na/valid_files/testflagsdata.parquet')
            returnList <-try(NEONprocIS.base::def.rcd.miss.na(fileData=inputfilepaths),
                                    silent = TRUE)
            testthat::expect_true((class(returnList)[1] == "try-error"))
          }
)

test_that("extra timestampe in the second file",
          {

            inputfilepaths <- c( 'def.rcd.miss.na/valid_files/testflagsdata.parquet', 'def.rcd.miss.na/valid_files/testdata.parquet')

            returnList <-NEONprocIS.base::def.rcd.miss.na(fileData=inputfilepaths)
            testthat::expect_true(is.list(returnList))
            expect_true (length(returnList) == 2)
            testthat::equals(length(returnList$timeAll$readout_time), 6)
            testthat::equals(returnList$timeBad$readout_time[1], "2019-01-01 07:00:05 GMT")

          }
)

test_that("NA in one of the columns",
           {

             inputfilepaths <- c( 'def.rcd.miss.na/NA_test/testdataWithNA.parquet', 'def.rcd.miss.na/valid_files/testdata.parquet')
 
             returnList <-NEONprocIS.base::def.rcd.miss.na(fileData=inputfilepaths)
             testthat::expect_true(is.list(returnList))
             expect_true (length(returnList) == 2)
             testthat::equals(length(returnList$timeAll$readout_time), 6)
             testthat::equals(returnList$timeBad$readout_time[1], "2019-01-01 00:00:02 GMT")
 
           }

)
