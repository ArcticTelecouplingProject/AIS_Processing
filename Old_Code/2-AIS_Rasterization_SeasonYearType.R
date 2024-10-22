################################################################################
# TITLE: AIS Rasterization Script
# PURPOSE: This script takes AIS vector daily transit segments and transforms
  # them into raster pixels of a specified resolution containing total length of 
  # vessel traffic.
# AUTHOR: Kelly Kapsar & Ben Sullender
# CREATED: 2021 
# LAST UPDATED ON: 20240517
# 
# NOTE: 
################################################################################

# Load Libraries
library(spatstat)
library(raster)
library(maptools)
library(sf)
library(sp)
library(dplyr)


starttot <- proc.time()

# Projection (Alaska Albers)
AA <- "+proj=aea +lat_1=55 +lat_2=65 +lat_0=50 +lon_0=-154 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs"

# Source of vector files 
dsn <-"../Data_Processed/Vector/"

# Destination to save files 
savedsn <-"../Data_Processed/Raster/"

# Mask outside of AIS bounds 
ais_mask <- st_read( "../Data_Raw/ais_reshape/ais_reshape.shp")

# Specify cell size 
cellsize <- 4000

# Derive all combinations of output rasters 
vessel_type <-  c("Tanker", 
                  "Pleasure", 
                  "Passenger", 
                  "Sailing", 
                  "TugTow", 
                  "Cargo", 
                  "Fishing", 
                  "Other")
season <- c("winter", "spring", "summer", "fall")
year <- 2015:2022

df <- expand.grid(vessel_type, season, year)
colnames(df) <- c("vessel_type", "season", "year")

# Function to create yearly rasters for each vessel type by season 
make_ais_raster <- function(df, dsn, savedsn, cellsize, ais_mask){
  # Load in all shp files 
  filelist <- intersect(list.files(dsn, 
                                   pattern= as.character(df$year), 
                                   full.names = T),
                        list.files(dsn, 
                                   pattern = as.character(df$vessel_type), 
                                   full.names=T)) 
  files <- filelist[grepl(".shp", filelist)]
  
  
  if(df$season == "spring"){
    toload <- grep("03-|04-|05-", files, value=T)
  }
  if(df$season == "summer"){
    toload <- grep("06-|07-|08-", files, value=T)
  }
  if(df$season == "fall"){
    toload <- grep("09-|10-|11-", files, value=T)
  }
  if(df$season == "winter"){
    toload <- grep("12-|01-|02-", files, value=T)
  }
  
  ships <- lapply(toload, st_read, quiet=T) %>% do.call(rbind, .)
  
  moSHP <- as(ships, "Spatial")
  
  
  #convert to spatial lines format - this step takes the longest
  moPSP <- as.psp(moSHP)
  
  #create bounding extent for all area
  extentAOI <- as.owin(list(xrange=c(-2550000,550000),yrange=c(235000,2720000)))
  
  #create mask with pixel size
  allMask <- as.mask(extentAOI,eps=cellsize)
  
  #run pixellate with mask
  moPXL <- pixellate.psp(moPSP,W=allMask)
  
  #render as raster and convert units to km
  moRAST <- raster(moPXL)/1000
  
  cellsize_km <- cellsize/1000
  
  
  rast <- terra::mask(x = moRAST, mask = ais_mask)
  
  raster::crs(rast) <- AA # NOT WORKING AND IDK WHY... 
  
  writeRaster(rast, filename = paste0(savedsn, "Raster_", df$vessel_type, "_", df$year, "_", df$season, ".tif"))
}

# Call function for each unique combination of ship type, season, and year 
for(i in 1:length(df$vessel_type)){
  print(df[i,])
  make_ais_raster(df = df[i,], dsn, savedsn, cellsize, ais_mask)
}

proc.time() - start
