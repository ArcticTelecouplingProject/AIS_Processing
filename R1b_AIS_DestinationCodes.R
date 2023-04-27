################################################################################
# TITLE: Destination coding addition to vector data
# PURPOSE: This script takes monthly vector files by ship type and adds manually
# interpreted revisions of destination codes to individual track lines. 
# AUTHOR: Kelly Kapsar & Bella Block
# CREATED: 2023-04-27
# LAST UPDATED ON: 2023-04-27
# 
# NOTE: 
################################################################################

# Load libraries
library(sf)
library(dplyr)

# Create list of files 
vpath <- "D:/AIS_V2_DayNight_60km6hrgap/Vector/"

vecs <- list.files(vpath, pattern='.shp', full.names = F)
vecsfull <- list.files(vpath, pattern='.shp', full.names = T)

# Read in codes. 


