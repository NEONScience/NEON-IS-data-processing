library(dplyr)
# y <- modwt(x=strainGaugeDepthAgr$strainGaugeDepth, wf = "la8", n.levels = 10, boundary = "reflection")
# y1 <- y; y1[2:11] <- NULL; d1 <- imodwt(y1)
MRA <- waveslim::mra(x=strainGaugeDepthAgr$strainGaugeDepth, wf = "la8", J = 10, method = "modwt", boundary = "reflection")
base::plot(MRA$D1+MRA$D2+MRA$D3+MRA$D4+MRA$D5+MRA$D6+MRA$D7+MRA$D8+MRA$D9+MRA$D10+MRA$S10)

# What if we just add variability at the small scale for uncertainty calc? actual precip results in longer scale variability (increase in bucket depth)
# Maybe a bit beyond diel scale
# 5-min data (288 values per day)
# detail scale 1 (2^1 values) = 10 min
# detail scale 2 (2^2 values) = 20 min
# detail scale 3 (2^3 values) = 40 min
# detail scale 4 (2^4 values) = 1 hr 20 min
# detail scale 5 (2^5 values) = 2 hr 40 min
# detail scale 6 (2^6 values) = 5 hr 20 min
# detail scale 7 (2^7 values) = 10 hr 40 min
# detail scale 8 (2^8 values) = 21 hr 20 min
# detail scale 9 (2^9 values) = 42 hr 40 min
# detail scale 10 (2^10 values) = 3 days 13 hr 20 min
# detail scale 11 (2^11 values) = 7 days 2 hr 40 min
base::plot(MRA$D1+MRA$D2+MRA$D3+MRA$D4+MRA$D5+MRA$D6+MRA$D7+MRA$D8+MRA$D9)
base::plot(MRA$D10 + MRA$S10)

# Subtract the benchmark first? Then do wavelets? This would remove evaporation, which we do want to test for
depthMinusBench <- strainGaugeDepthAgr$strainGaugeDepth - strainGaugeDepthAgr$bench
MRA2 <- waveslim::mra(x=depthMinusBench, wf = "la8", J = 10, method = "modwt", boundary = "reflection")
base::plot(MRA2$D1+MRA2$D2+MRA2$D3+MRA2$D4+MRA2$D5+MRA2$D6+MRA2$D7+MRA2$D8+MRA2$D9+MRA2$D10+MRA2$S10)
base::plot(MRA2$D10 + MRA2$S10)

