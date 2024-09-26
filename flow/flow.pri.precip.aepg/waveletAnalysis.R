# y <- modwt(x=strainGaugeDepthAgr$strainGaugeDepth, wf = "la8", n.levels = 10, boundary = "reflection")
# y1 <- y; y1[2:11] <- NULL; d1 <- imodwt(y1)
MRA <- waveslim::mra(x=strainGaugeDepthAgr$strainGaugeDepth, wf = "la8", J = 10, method = "modwt", boundary = "reflection")
base::plot(MRA$D1+MRA$D2+MRA$D3+MRA$D4+MRA$D5+MRA$D6+MRA$D7+MRA$D8+MRA$D9+MRA$D10+MRA$S10)