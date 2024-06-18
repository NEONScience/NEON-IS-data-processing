library(dplyr)
library(ggplot2)
library(plotly)
library(lubridate)

dat_pat <- '/scratch/PrimaryPrecipAlgo/'
site <- 'konz'
prism <- read.csv(paste0(dat_pat, site, '_prism.csv'))
sec <-  read.csv(paste0(dat_pat, site, '_sec.csv'))
raw_df <- read.csv(paste0(dat_pat, site, '_dfir.csv'))

func_pat <- '/home/NEON/tburlingame/GitHub/NEON-IS-data-processing/flow/flow.pri.precip.aepg/'

source(paste0(func_pat, 'cumm_to_real_tim_func.R'))
source(paste0(func_pat, 'combo_evap_time_primary_noise_func.R'))


sum(sec$value)
sum(prism$value)


colors <- c("Adjusted Bench" = "blue",
            "Benchmark" = "black",
            "Secondary Precip" = "orange",
            "PRISM" = "purple", 
            "Raw" = 'maroon',
            "Envelope Median" = 'red', 
            "Envelope Mean" = 'green')

avg_adj <- '1 hour'
df_hr <- noise_removal(raw_df = raw_df, 
                       changeFactor = 0.9, # factor by which nth-range is multiplied to determine if change in val = precip
                       nthVal = 1, #how wide of a range do we want to compare for noise (1 is essentially min and max/full range)
                       countThresh = 6, #how many hours does precip need to be increasing to be considered valid
                       rawThresh = 0.2, #expected sensitivity of instrument between individual points
                       avg_adj = avg_adj, #unit to average raw data
                       recharge = 250 #if raw data was this much less than bench mark likely a bucket empty/recalibration (original was 25)
)


p <- ggplot(df_hr, aes(x=summ)) +
  geom_line(aes( y = adj_bench, color = 'Adjusted Bench'))+
  geom_line(aes( y = raw, color = 'Raw'))+
  geom_line(aes( y = bench, color = 'Bench'))+
  geom_point(aes(y = nMin), size = 0.7, color = 'orange')+
  geom_point(aes( y = nMean, color = 'Envelope Mean'), size = 0.7)+
  ggtitle(paste0('Precip Cleaning Tests at ', toupper(site)))+
  labs(x = "Date",
       y = "Cumulative Precip mm",
       color = "Legend") 

ggplotly(p)

#adjust cumulative data back to real time
rt_pcp <- cumm_to_real_time(df_hr)
rt_pcp$summ <- as.POSIXct(rt_pcp$summ)

p <- ggplot(rt_pcp, aes(x=summ)) +
  geom_line(aes( y = adj_bench_reset, color = 'Adjusted Bench'))+
  geom_line(aes( y = raw_reset, color = 'Raw'))+
  geom_line(aes( y = bench_reset, color = 'Benchmark'))+
  ggtitle(paste0('Precip Adjusted to Zero at ',  toupper(site)))+
  labs(x = "Date",
       y = "Cumulative Precip mm",
       color = "Legend") 

ggplotly(p)


sec_summ <- sec %>% select(startDate, value) %>%
  mutate(summ = lubridate::floor_date(as.POSIXct(startDate), unit = avg_adj)) %>%
  group_by(summ) %>% summarise(hourly = sum(value)) %>% 
  mutate(sec_cumm = cumsum(hourly))

all_pcp <- left_join(rt_pcp, sec_summ, by = 'summ')

p <- ggplot(all_pcp, aes(x=summ)) +
  geom_line(aes( y = adj_bench_rt, color = "Adjusted Bench"))+
  geom_line(aes( y = bench_rt, color = 'Benchmark'))+
  geom_line(aes( y = hourly, color = 'Secondary Precip'))+
  labs(x = "Date",
       y = "Precip mm",
       color = "Legend") +
  scale_color_manual(values = colors)+
  ggtitle(paste0('Correct Precip Real Time at ', site))

ggplotly(p)

#add prism data

pcp_daily <- all_pcp %>% 
  mutate(summ = summ + (12*60*60)) %>% 
  mutate(day = lubridate::floor_date(summ, unit = 'day')) %>% 
  group_by(day) %>% 
  summarise(bench = sum(bench_rt, na.rm = T),
            adj_bench = sum(adj_bench_rt, na.rm = T),
            sec_daily = sum(hourly, na.rm = T),
            raw_daily = sum(raw_rt, na.rm = T)) %>%
  mutate(tot_bench = cumsum(bench),
         tot_adj_bench = cumsum(adj_bench),
         tot_sec  = cumsum(sec_daily),
         tot_raw = cumsum(raw_daily)) 

prism$day <- as.POSIXct(as.character(prism$date),format = '%Y%m%d', tz = 'UTC')

pcp_daily <- left_join(pcp_daily, prism, by = 'day') %>% 
  dplyr::filter(!is.na(value)) %>%
  dplyr::filter(!is.na(tot_bench))%>%
  dplyr::mutate(prism = cumsum(value))

p <- ggplot(pcp_daily, aes(x=day)) +
  geom_line(aes( y = tot_adj_bench, color = "Adjusted Bench"))+
  geom_line(aes( y = tot_bench, color = 'Benchmark'))+
  geom_line(aes( y = tot_sec, color = 'Secondary Precip'))+
  geom_line(aes( y = tot_raw, color = 'Raw'))+
  geom_line(aes(y = prism, color = 'PRISM'))+
  labs(x = "Date",
       y = "Cumulative Precip mm",
       color = "Legend") +
  scale_color_manual(values = colors)+
  ggtitle(paste0('Cummulative Daily Precip at ', site))
ggplotly(p)
