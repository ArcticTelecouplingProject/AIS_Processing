
ls <- list.files("/mnt/home/kapsarke/Documents/AIS/Data_Raw/NSF_AIS_2023-2024", full.names = TRUE)

ais_dates <- list()  # Initialize as an empty vector instead of a list

for(i in 1:length(ls)) {
  
  # if(i %% 10 == 0) { print(i) }  # Print progress every 10 files
  print(i)
    
  df <- read.csv(ls[i])  # Read CSV file
  
  # Extract unique dates from 'position_timestamp' column
  temp <- unique(lubridate::date(df$position_timestamp))
  
  ais_dates <- append(ais_dates, list(temp))
}


ais_dates <- as.Date(unlist(ais_dates))

# Print final list of unique dates
print(ais_dates)

write.csv(ais_dates, "/mnt/home/kapsarke/Documents/AIS/AIS_2023-2024_dates.csv")

ais_dates <- read.csv("/mnt/home/kapsarke/Documents/AIS/AIS_2023-2024_dates.csv")


t <- as.Date(unique(ais_dates$x))
t[!(t %in% dates)]
