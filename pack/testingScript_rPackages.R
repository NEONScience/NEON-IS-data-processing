
setwd("~/GitHub/NEON-IS-data-processing/pack/neonprocIS.cal")
devtools::document()
devtools::check()
# setwd("~/GitHub/NEON-IS-data-processing/pack")
# devtools::install("neonprocIS.cal")

setwd("~/GitHub/NEON-IS-data-processing/pack/NEONprocIS.base")
devtools::document()
devtools::check()
# setwd("~/GitHub/NEON-IS-data-processing/pack")
# devtools::install("NEONprocIS.base")

setwd("~/GitHub/NEON-IS-data-processing/pack/NEONprocIS.qaqc")
devtools::document()
devtools::check()
# setwd("~/GitHub/NEON-IS-data-processing/pack")
# devtools::install("NEONprocIS.qaqc")