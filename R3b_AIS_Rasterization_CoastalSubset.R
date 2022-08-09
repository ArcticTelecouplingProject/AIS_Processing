################################################################################
# TITLE: Coastal Subset Script
# PURPOSE: Using 10km coastal buffer, 1km raster size
  # Takes vector (lines) as input, groups by month, then writes a raster for each month
  # Uses a polygon window object (AOIowin) to mask out all cells >10km from coast
  # Takes about 4.5 hours to run when grouped by year
# AUTHOR: Ben Sullender
# CREATED: 2021-08
# LAST UPDATED ON: --
# 
# NOTE: This script does not contain relative file paths. Running it will require
  # adjusting file paths to point to appropriate inputs. 
################################################################################


# Coastal Subset Script
# Using 10km coastal buffer, 1km raster size
# Takes vector (lines) as input, groups by month, then writes a raster for each month
# Uses a polygon window object (AOIowin) to mask out all cells >10km from coast
# Takes about 4.5 hours to run when grouped by year
# Script by Ben Sullender, Aug 2021

library(spatstat)
library(rgdal)
library(raster)
library(maptools)
library(tidyverse)
library(rgeos)
library(sf)

AOI10k <- readOGR("/Users/bensullender/Documents/KickstepApproaches_Projects/FWS_AIS/ais_reshape/AIS_10km_buffer_final.shp")
AOIowin <- as.owin(AOI10k)


AIS.Rasta.sf.agg <- function(vectorList, cellsize=1000){
  firstSF <- st_read(vectorList[[1]])
  outGeom <- firstSF$geometry
  
  for (i in 2:length(vectorList)){
    nextSF <- st_read(vectorList[[i]])
    nextGeom <- nextSF$geometry
    outGeom <- append(outGeom,nextGeom)
  }
  
  #read line shapefile
  moSHP <- as_Spatial(outGeom)
  
  #convert to spatial lines format - this step takes the longest
  moPSP <- as.psp(moSHP)
  
  #create mask with pixel size
  allMask <- as.mask(AOIowin,eps=cellsize)
  
  #run pixellate with mask
  moPXL <- pixellate.psp(moPSP,W=allMask)
  
  #render as raster
  moRAST <- raster(moPXL)
  return(moRAST)
  
}







setwd("/Users/bensullender/Documents/KickstepApproaches_Projects/FWS_AIS/Data/Vector/2020_unzipped")
all2020 <- list.files(pattern=".shp")

Jan2020 <- as.list(all2020[1:4])
Feb2020 <- as.list(all2020[5:8])
Mar2020 <- as.list(all2020[9:12])
Apr2020 <- as.list(all2020[13:16])
May2020 <- as.list(all2020[17:20])
Jun2020 <- as.list(all2020[21:24])

Jul2020 <- as.list(all2020[25:28])
Aug2020 <- as.list(all2020[29:32])
Sep2020 <- as.list(all2020[33:36])
Oct2020 <- as.list(all2020[37:40])
Nov2020 <- as.list(all2020[41:44])
Dec2020 <- as.list(all2020[45:48])




system.time(Jan2020R <- AIS.Rasta.sf.agg(Jan2020))
print("Above time for Jan")
writeRaster(Jan2020R,"/Users/bensullender/Documents/KickstepApproaches_Projects/FWS_AIS/Data/CoastalSubset/Jan2020R.tif")


system.time(Feb2020R <- AIS.Rasta.sf.agg(Feb2020))
print("Above time for Feb")
writeRaster(Feb2020R,"/Users/bensullender/Documents/KickstepApproaches_Projects/FWS_AIS/Data/CoastalSubset/Feb2020R.tif")


system.time(Mar2020R <- AIS.Rasta.sf.agg(Mar2020))
print("Above time for Mar")
writeRaster(Mar2020R,"/Users/bensullender/Documents/KickstepApproaches_Projects/FWS_AIS/Data/CoastalSubset/Mar2020R.tif")


system.time(Apr2020R <- AIS.Rasta.sf.agg(Apr2020))
print("Above time for Apr")
writeRaster(Apr2020R,"/Users/bensullender/Documents/KickstepApproaches_Projects/FWS_AIS/Data/CoastalSubset/Apr2020R.tif")


system.time(May2020R <- AIS.Rasta.sf.agg(May2020))
print("Above time for May")
writeRaster(May2020R,"/Users/bensullender/Documents/KickstepApproaches_Projects/FWS_AIS/Data/CoastalSubset/May2020R.tif")


system.time(Jun2020R <- AIS.Rasta.sf.agg(Jun2020))
print("Above time for Jun")
writeRaster(Jun2020R,"/Users/bensullender/Documents/KickstepApproaches_Projects/FWS_AIS/Data/CoastalSubset/Jun2020R.tif")

system.time(Jul2020R <- AIS.Rasta.sf.agg(Jul2020))
print("Above time for July")
writeRaster(Jul2020R,"/Users/bensullender/Documents/KickstepApproaches_Projects/FWS_AIS/Data/CoastalSubset/Jul2020R.tif")


system.time(Aug2020R <- AIS.Rasta.sf.agg(Aug2020))
print("Above time for Aug")
writeRaster(Aug2020R,"/Users/bensullender/Documents/KickstepApproaches_Projects/FWS_AIS/Data/CoastalSubset/Aug2020R.tif")


system.time(Sep2020R <- AIS.Rasta.sf.agg(Sep2020))
print("Above time for Sep")
writeRaster(Sep2020R,"/Users/bensullender/Documents/KickstepApproaches_Projects/FWS_AIS/Data/CoastalSubset/Sep2020R.tif")


system.time(Oct2020R <- AIS.Rasta.sf.agg(Oct2020))
print("Above time for Oct")
writeRaster(Oct2020R,"/Users/bensullender/Documents/KickstepApproaches_Projects/FWS_AIS/Data/CoastalSubset/Oct2020R.tif")


system.time(Nov2020R <- AIS.Rasta.sf.agg(Nov2020))
print("Above time for Nov")
writeRaster(Nov2020R,"/Users/bensullender/Documents/KickstepApproaches_Projects/FWS_AIS/Data/CoastalSubset/Nov2020R.tif")


system.time(Dec2020R <- AIS.Rasta.sf.agg(Dec2020))
print("Above time for Dec")
writeRaster(Dec2020R,"/Users/bensullender/Documents/KickstepApproaches_Projects/FWS_AIS/Data/CoastalSubset/Dec2020R.tif")







