### simple script to test original function
library(ggplot2)
library(plotly)
library(lubridate)

dat_pat <- '/scratch/PrimaryPrecipAlgo/'
func_pat <- '/home/NEON/tburlingame/GitHub/NEON-IS-data-processing/flow/flow.pri.precip.aepg/'
site <- 'konz'
raw_df <- read.csv(paste0(dat_pat, site, '_dfir.csv'))

source(paste0(func_pat, 'evap_primary_noise_func.R'))

avg_adj <- '1 hour'
df_hr <- noise_removal(raw_df = raw_df, 
                       changeFactor = 0.9, # factor by which nth-range is multiplied to determine if change in val = precip
                       nthVal = 1, #how wide of a range do we want to compare for noise (1 is essentially min and max/full range)
                       countThresh = 6, #how many hours does precip need to be increasing to be considered valid
                       rawThresh = 0.2, #expected sensitivity of instrument between individual points
                       avg_adj = avg_adj, #unit to average raw data
                       recharge = 250 #if raw data was this much less than bench mark likely a bucket empty/recalibration (original was 25)
)

#total pcp
df_hr$adj_bench[nrow(df_hr)]-df_hr$adj_bench[1]

p <- ggplot(df_hr, aes(x=summ)) +
  geom_line(aes( y = raw, color = 'Raw'))+
  geom_line(aes( y = bench, color = 'Bench'))+
  geom_line(aes( y = adj_bench, color = 'Adjusted Bench'))+
  ggtitle(paste0('Precip Cleaning Tests at ', toupper(site)))+
  labs(x = "Date",
       y = "Cumulative Precip mm",
       color = "Legend") 
p
ggplotly(p)
