
library(dplyr)
library(lubridate)

site <- 'osbs'
prism <- read.csv(paste0('data/', site, '_prism.csv'))
sec <-  read.csv(paste0('data/', site, '_sec.csv'))

sum(sec$value)
sum(prism$value)
