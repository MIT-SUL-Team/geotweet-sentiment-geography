library(tidyverse)
library(sp)
library(sf)

# ----- load adm1 ---------------------------------------------
library(rmapshaper)
adm <- st_read("/home/yongxuan/GlobalSentiment/data/00_spatial/bound/adm1.shp")

countries <- unique(adm$ISO)

listofsf <- NULL
for(i in 1:length(countries)){
  cat(as.character(Sys.time()), i, countries[i], "\n")
  adm_sub <- adm[adm1$ISO == countries[i], ]
  adm_sim <- rmapshaper::ms_simplify(adm_sub) 
  listofsf[[i]] <- adm_sim
}

adm_simplify <- st_as_sf(dplyr::bind_rows(listofsf))

saveRDS(adm_simplify, file = "/home/jianghao/adm1_simplify.Rds")
sf::st_write(adm_simplify, "/home/jianghao/adm1_simplify.shp")