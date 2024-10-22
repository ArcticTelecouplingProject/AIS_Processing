################################################################################
# TITLE: Hex Grid Creation Script
# PURPOSE:  Script that creates a hex grid for the Alaska Conservation Fund 
  # AIS study area 
# AUTHOR: Ben Sullender
# CREATED: 2021-08-09
# LAST UPDATED ON: 2021-0809
# 
# NOTE: This script does not contain relative file paths. Running it will require
  # adjusting file paths to point to appropriate inputs. 
################################################################################

# Import libaries
library(sf)
library(dplyr)
library(tidyr)

# create a bounding box
# 7/24: Updated lower bounding extent to 200000.
AOIbbox <- st_polygon(list(matrix(c(-2550000,200000,550000,200000,550000,2720000,-2550000,2720000,-2550000,200000),ncol=2, byrow=TRUE)))

# here, sf's default cellsize = "diameter" of hexagon = 2x apothem (distance from center to midpoint of a side)
# so a square of diameter 10,000m = 10km has area 10^2 = 100km^2, but a hex of "diameter" 10km has area 10^2 * 2*sqrt(3) = 346.4 km^2
# for a hex of area ~100 km^2, we want "diameter" / 2x apothem to be ~5.37285km (results in 243,000 hexes)
# for a hex of area ~625 km^2, "diameter" / 2x apothem = 26.86424km (results in 12,730 hexes)

system.time(hexes <- st_make_grid(AOIbbox,cellsize=26864.24,square=FALSE,flat_topped=TRUE))

length(hexes)

hexesSF <- st_as_sf(hexes) %>%
  st_set_crs(3338) %>%
  mutate(hexID=1:length(hexes))

# Read in the boundaries of the study area used to collect AIS data 
hexbounds <- st_read("./Data_Raw/ais_reshape/ais_reshape.shp")
# Spatial intersection of AIS data collection boundaries and hex grid
hexgrid <- st_intersection(hexesSF, hexbounds$geometry)

# Save output
st_write(hexesSF, "./hex_test/Hexes_625km2.shp")

hexbounds <- st_read("./Data_Raw/ais_reshape/ais_reshape.shp")

# this is based on the inverse of AK_RUS_CAN, then simplified with 50m snapping tolerance to try to minimize geometry
AK_sea <- st_read("./Data_RAW/AK_CAN_RUS/AK_CAN_RUS.shp") %>% st_crop(hexbounds) %>% st_simplify(dTolerance=50)
t <- st_intersection(AK_sea, hexbounds)
AK_sea_new <- st_difference(hexbounds, st_union(t))
# AK_sea <- st_read("../../GIS/Basedata/AK_CAN_RUS_Ocean_50m_simplify.shp")

# warning: this takes ~2 hours since AK_CAN_RUS  has such complex geometry
# hexv2 <- st_join(hexesSF,AK_sea_new)
# 
# plot(hexesSF$x)
# plot(AK_sea_new$geometry, col="red")
test <- st_intersects(hexesSF, AK_sea_new, sparse=F)

saveRDS(test, "tempHex_intersects.rds")

hexv2 <- hexesSF %>% rename(geometry=x)

hexv2$inocean <- test

# hexv2 <- readRDS("tempHex.rds")
# now drop all the hexes that don't touch the ocean baselayer
hexv3 <- hexv2 %>%
  filter(inocean == TRUE)

# now merge the results with the original AOI and drop hexes that don't touch the AOI
hexv4 <- st_join(hexv3,hexbounds) %>%
  filter(!is.na(VISIBLE)) 

# clean out all the other attributes and re-populate hex ID so it's sequential again
hexRevised <- hexv4 %>%
  select(hexID) %>%
  mutate(hexID=1:nrow(hexv4))

write_sf(hexRevised,paste0(getwd(),"/RevisedHexes.shp"))




# Save output
st_write(hexRevised, "./Data_Processed/HexGrid_625km2_KEK.shp")
