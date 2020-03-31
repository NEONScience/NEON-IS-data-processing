library(testthat)
source("data_comb.R")
test_that(
   "Data Comb ts tests",
   {
     DirIn <- "tests/test_input"
     DirOut <- "tests/test_output/pfs/out"
     FileSchmComb <- file.path(DirIn,"avro_schemas/dp01/waterQuality_exoconductivity_dp01.avsc" )
     DirComb <- "data|uncertainty_data"
     NameDirCombOut <- "stats"
     NameVarTime <- "readout_time"
     ColKeep <- "readout_time|readout_time|specificConductance|specificConductance_ucrtExpn"
     NameFileSufx <- "_basicStats_100"
     
     data_combine(DirIn = DirIn, DirOut = DirOut, FileSchmComb = FileSchmComb, DirComb = DirComb, NameDirCombOut = NameDirCombOut, NameVarTime = NameVarTime,
       ColKeep = ColKeep, NameFileSufx = NNameFileSufxULL)
    
   }
)


