################################################################################
# TITLE: AIS Rasterization Script
# PURPOSE: This script takes AIS vector daily transit segments and transforms
  # them into raster pixels of a specified resolution containing total length of 
  # vessel traffic.
# AUTHOR: Ben Sullender (modified by Kelly Kapsar)
# CREATED: 2021 
# LAST UPDATED ON: ??
# 
# NOTE: 
################################################################################

# Load Libraries 
library(spatstat)
library(rgdal)
library(raster)
library(maptools)


AIS.Rasta <- function(vectorName, dsn, savedsn, cellsize=25000){
  # start timer 
  starttime <- proc.time()
  
  #read line shapefile
  moSHP <- readOGR(dsn, vectorName)
  
  #convert to spatial lines format - this step takes the longest
  moPSP <- as.psp(moSHP)
  
  #create bounding extent for all area
  extentAOI <- as.owin(list(xrange=c(-2550000,550000),yrange=c(235000,2720000)))
                  
  #create mask with pixel size
  allMask <- as.mask(extentAOI,eps=cellsize)
  
  #run pixellate with mask
  moPXL <- pixellate.psp(moPSP,W=allMask)
  
  #render as raster
  moRAST <- raster(moPXL)
  
  cellsize_km <- cellsize/1000
  
  #write output
  writeRaster(moRAST,paste0(savedsn,"/AISRaster",substr(vectorName,7,nchar(vectorName)),"_",cellsize_km,"km",".tif"))
  
  runtime <- proc.time() - starttime 
  print(runtime)
}

# Setup to see syntax - yours will vary based on input/output file structure
files <- list.files("../Data_Processed_HPCC_SPOOFIN/2020/Vector/", pattern='.shp')

# the "-4" part gets rid of ".shp"
inS <- substr(files,1,nchar(files)-4)
dsn <- "D:/AlaskaConservation_AIS_20210225/Data_Processed_HPCC_FINAL/2020/Vector"
savedsn <- "D:/AlaskaConservation_AIS_20210225/Data_Processed_HPCC_FINAL/2020/Raster_10km"

start <- proc.time()
lapply(inS, function(x){AIS.Rasta(vectorName=x, dsn=dsn, savedsn=savedsn, cellsize=10000)}) 
tottime <- proc.time()-start
browseURL("https://www.youtube.com/watch?v=AZQxH_8raCI&ab_channel=worldslover234")

# wd <- "D:/AlaskaConservation_AIS_20210225/Data_Processed_HPCC_FINAL/2020/Raster"
# files <- paste0(wd,"/",list.files(wd, pattern='.tif'))
# file.rename(files, gsub(".tif","_25km.tif",files))

