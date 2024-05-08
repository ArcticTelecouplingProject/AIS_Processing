
library(tidyverse)
library(sf)

# Set data and saved data locations
dsn <-"../../AISPRocessing_V2/Data_Processed/Vector"
outdsn <- "../../AISPRocessing_V2/Data_Processed/Vector_By_Type/"

singletypes <- c("Tanker", "Pleasure", "Passenger", "Sailing", "TugTow")
bigtypes <- c("Cargo", "Fishing", "Other")


savefile <- function(dsn, outdsn, vessel_type, chunks = FALSE){
  
  filelist <- intersect(list.files(dsn, pattern='.shp', full.names = T), 
                        list.files(dsn, pattern = vessel_type, full.names = T)) 
  if(chunks == FALSE){
    files <- lapply(filelist, st_read, quiet=T)
    ships <- do.call(rbind, files)
    st_write(ships, paste0(outdsn, "Tracks_DayNightFALSE_", vessel_type, ".shp"))
    print(paste0("Saved ", vessel_type, " vessel data."))
  }
  if(chunks == TRUE){
    metalist <- split(filelist,   ceiling(seq_along(filelist) / 33))
    for(i in 1:length(metalist)){
      files <- lapply(metalist[[i]], st_read, quiet=T)
      ships <- do.call(rbind, files)
      st_write(ships, 
               paste0(outdsn, "Tracks_DayNightFALSE_", 
                      vessel_type, "_Part_", i, ".shp"))
      print(paste0("Saved ", vessel_type, "_Part_", i, " vessel data."))
    }
  }
}

lapply(singletypes, function(x){savefile(dsn, outdsn, x)})
lapply(bigtypes, function(x){savefile(dsn, outdsn, x, chunks=T)})