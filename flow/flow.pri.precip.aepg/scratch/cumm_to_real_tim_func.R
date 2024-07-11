##############################################################################################
#' @title cumm_to_real_time

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr

#' @description convert cumulative benchmark precipitation data to realtime
#' @param cumm_pcp a data frame with variables:
#' raw = original raw precipitation data
#' bench = benchmark data not adjustested for evaporation
#' adj_bench = benchmark precipitation data adjusted for evaporation

#' @return cummulative data frame with additional variables for real-time precipitation amounts

# changelog and author contributions / copyrights
#   Teresa Burlingame(2023-12-11)
#     original creation
##############################################################################################
cumm_to_real_time=function(cumm_pcp = cumm_pcp){

  #adjust input data to zero. if already at zero should remain the same. 
  
  cumm_pcp$bench_reset <- cumm_pcp$bench - cumm_pcp$raw[1]
  cumm_pcp$adj_bench_reset <- cumm_pcp$adj_bench- cumm_pcp$raw[1]
  cumm_pcp$raw_reset <- cumm_pcp$raw - cumm_pcp$raw[1]
  
  
  rt_pcp <- cumm_pcp %>% mutate(bench_rt = c(bench_reset[1],diff(bench_reset)),
                                adj_bench_rt =  c(adj_bench_reset[1],diff(adj_bench_reset)),
                                raw_rt =  c(raw_reset[1],diff(raw_reset)))

  return(rt_pcp)
}
