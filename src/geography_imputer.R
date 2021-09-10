# Initial -----------------------------------------------------------------
​
library(dplyr)
library(ggplot2)
# library(lubridate)
library(sf)
​
twitter.dir <- "/n/holyscratch01/cga/nicogj/geo_input/"
save.dir <- "/n/holyscratch01/cga/nicogj/geo_output/"
​
# load geographical boundary ----------------------------------------------
​
admin1 <- "data/spatial/adm1.shp" %>%
  st_read() %>%
  select(ISO, HASC_1, NAME_1) %>%
  rename(country = ISO) %>%
  rename(admin1_hasc = HASC_1) %>%
  rename(admin1 = NAME_1) %>%
  as_Spatial()
​
admin2 <- "data/spatial/adm2.shp" %>%
  st_read() %>%
  dplyr::select(NAME_2, HASC_2) %>%
  rename(admin2_hasc = HASC_2) %>%
  rename(admin2 = NAME_2) %>%
  as_Spatial()
​
crs <- st_crs(admin1)
​
# Sentiment summary -------------------------------------------------------
​
files <- list.files(twitter.dir)
​
for (i in 1:length(files)) {

  file <- files[i]

  cat(as.character(Sys.time()), i, file, "\n")

  # load twitter data
  tweets <- data.table::fread(paste0(twitter.dir, file))

  tweets <- tweets %>%
    select(message_id, longitude, latitude) %>%
    mutate(longitude = as.double(longitude)) %>%
    mutate(latitude = as.double(latitude)) %>%
    distinct() %>%
    filter(!is.na(longitude)) %>%
    filter(!is.na(latitude))

  # st_intersection is slow
  ov <- cbind(
    data.frame("message_id" = tweets$message_id),
    sp::over(as_Spatial(st_as_sf(tweets, coords = c('longitude', 'latitude'), crs = crs)), admin1, returnList = F) %>%
      as.data.frame(),
    sp::over(as_Spatial(st_as_sf(tweets, coords = c('longitude', 'latitude'), crs = crs)), admin2, returnList = F) %>%
      as.data.frame()
  ) %>%
    mutate_if(is.factor, as.character)

  write.csv(ov, paste0())

  saveRDS(ov, file = paste0(save.dir, "geography_", file), compress = T)
  cat("   ->", as.character(Sys.time()), "Admin imputed\n")

  rm(ov, tweets)
  gc()
}
