library(testthat)
source("data_comb.R")
test_that(    

   "Data Comb ts tests",
   {
   
     DirIn <- "tests/test_input/waterQuality_exoconductivity_data_uncertainty_group"
     DirOut <- "tests/test_output/pfs/out"
     FileSchmComb <- "tests/test_input/avro_schemas/dp01/waterQuality_exoconductivity_dp01_stats.avsc" 
     DirComb <- c("data", "uncertainty_data")
     NameDirCombOut <- "stats"
     NameVarTime <- "readout_time"
     ColKeep <- c("readout_time", "readout_time", "specificConductance", "specificConductance_ucrtExpn")
     NameFileSufx <- "_basicStats_100"
     outputFile <-  file.path(DirOut,"01/02/water-quality-283/exoconductivity/CFGLOC110700/stats/SENSOR000316_2019-01-02_basicStats_100.avro" )
     data_combine(DirIn = DirIn, DirOut = DirOut, FileSchmComb = FileSchmComb, DirComb = DirComb, NameDirCombOut = NameDirCombOut, NameVarTime = NameVarTime,
       ColKeep = ColKeep, NameFileSufx = NameFileSufx)
     expect_that(file.exists(outputFile),is_true())
    
   }
)


