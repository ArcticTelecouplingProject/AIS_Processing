library(data.table)
library(dplyr)
library(scales)
library(ggplot2)
library(sf)
library(ggplot2)
library(scales)
library(lubridate)

################################################################################
# Check for number of signals per day 2023-2024

# Define the directory where the new CSV files are saved
dir <- "/mnt/home/kapsarke/Documents/AIS/Data_Raw/2023-2024_Raw_OriginalFileNames/"

# Get a list of all the processed CSV files
csv_files <- list.files(dir, pattern = "Historical_AOI", full.names = TRUE)


# Initialize an empty data frame
summary_df <- data.frame(Year = integer(), Month = integer(), Date = as.Date(character()), 
                         Total_Rows = integer(), Unique_Ships = integer(), stringsAsFactors = FALSE)

# Loop through each file and extract information using dplyr
for (file in csv_files) {
  # Read the CSV file
  df <- fread(file)
  print(file)
  
  # Ensure the necessary columns exist
  if (!all(c("position_timestamp", "mmsi") %in% colnames(df))) {
    message("Skipping file (missing columns): ", file)
    next
  }
  
  # Process the data using dplyr pipes
  date_summary <- df %>%
    mutate(Date = as.Date(position_timestamp, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")) %>%
    group_by(Date) %>%
    summarise(
      Total_Rows = n(),                          # Count total AIS signals
      Unique_Ships = n_distinct(mmsi),        # Count unique ships
      .groups = 'drop'
    ) %>%
    mutate(
      Year = lubridate::year(Date),
      Month = lubridate::month(Date)
    ) %>%
    select(Year, Month, Date, Total_Rows, Unique_Ships)  # Reorder columns
  
  # Append to summary dataframe
  summary_df <- bind_rows(summary_df, date_summary)
}

# Print or save the summary
print(summary_df)

summary_df <- summary_df %>% group_by(Date) %>% summarize(Total_Rows = sum(Total_Rows), Unique_Ships = sum(Unique_Ships))
# Optionally, save it as a CSV for future reference
write.csv(summary_df, "../Data_Raw/2023-2024_summary_by_date.csv", row.names = FALSE)

################################################################################
# Check for number of signals per day 2021-2022

# Define the directory where the new CSV files are saved
dir <- "/mnt/home/kapsarke/Documents/AIS/Data_Raw/2022/"

# Get a list of all the processed CSV files
csv_files <- list.files(dir, pattern = ".csv", full.names = TRUE)

# Initialize an empty data frame
summary_df <- data.frame(Year = integer(), Month = integer(), Date = as.Date(character()), 
                         Total_Rows = integer(), Unique_Ships = integer(), stringsAsFactors = FALSE)

# Loop through each file and extract information using dplyr
for (file in csv_files) {
  # Read the CSV file
  df <- fread(file)
  print(file)
  
  # Ensure the necessary columns exist
  if (!all(c("dt_pos_utc", "mmsi") %in% colnames(df))) {
    message("Skipping file (missing columns): ", file)
    next
  }
  
  # Process the data using dplyr pipes
  date_summary <- df %>%
    mutate(Date = as.Date(dt_pos_utc, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")) %>%
    group_by(Date) %>%
    summarise(
      Total_Rows = n(),                          # Count total AIS signals
      Unique_Ships = n_distinct(mmsi),        # Count unique ships
      .groups = 'drop'
    ) %>%
    mutate(
      Year = lubridate::year(Date),
      Month = lubridate::month(Date)
    ) %>%
    select(Year, Month, Date, Total_Rows, Unique_Ships)  # Reorder columns
  
  # Append to summary dataframe
  summary_df <- bind_rows(summary_df, date_summary)
}

summary_df <- summary_df %>% group_by(Date) %>% summarize(Total_Rows = sum(Total_Rows), Unique_Ships = sum(Unique_Ships))
# Optionally, save it as a CSV for future reference
write.csv(summary_df, "../Data_Raw/2022_summary_by_date.csv", row.names = FALSE)



################################################################################
# Check for number of signals per day 2015-2020

for(i in 2015:2020){
  # Define the directory where the new CSV files are saved
  dir <- paste0("/mnt/home/kapsarke/Documents/AIS/Data_Raw/", i, "/")
  
  # Get a list of all the processed CSV files
  csv_files <- list.files(dir, pattern = ".csv", full.names = TRUE)
  
  # Initialize an empty data frame
  summary_df <- data.frame(Year = integer(), Month = integer(), Date = as.Date(character()), 
                           Total_Rows = integer(), Unique_Ships = integer(), stringsAsFactors = FALSE)
  
  # Loop through each file and extract information using dplyr
  for (file in csv_files) {
    # Read the CSV file
    df <- fread(file) %>% subset(!(Message_ID %in% c(5, 24, 27)))
    print(file)
    
    # Ensure the necessary columns exist
    if (!all(c("Time", "MMSI") %in% colnames(df))) {
      message("Skipping file (missing columns): ", file)
      next
    }
    
    # Process the data using dplyr pipes
    date_summary <- df %>%
      mutate(Date = as.Date(Time, format = "%Y%m%d_%H%M%S", tz = "UTC")) %>%
      group_by(Date) %>%
      summarise(
        Total_Rows = n(),                          # Count total AIS signals
        Unique_Ships = n_distinct(MMSI),        # Count unique ships
        .groups = 'drop'
      ) %>%
      mutate(
        Year = lubridate::year(Date),
        Month = lubridate::month(Date)
      ) %>%
      select(Year, Month, Date, Total_Rows, Unique_Ships)  # Reorder columns
    
    # Append to summary dataframe
    summary_df <- bind_rows(summary_df, date_summary)
  }
  
  summary_df <- summary_df %>% group_by(Date) %>% summarize(Total_Rows = sum(Total_Rows), Unique_Ships = sum(Unique_Ships))
  # Optionally, save it as a CSV for future reference
  write.csv(summary_df, paste0("../Data_Raw/", i, "_summary_by_date.csv"), row.names = FALSE)
}


################################################################################
# Check for number of signals per day 2015-2024 CLEAN data 


# Define the directory where the new CSV files are saved
dir <- "/mnt/home/kapsarke/Documents/AIS/Data_Processed_V4/Points/"

# Get a list of all the processed CSV files
csv_files <- list.files(dir, pattern = "Clean_Points", full.names = TRUE)


# Initialize an empty data frame
summary_df <- data.frame(Year = integer(), Month = integer(), Date = as.Date(character()), 
                         Total_Rows = integer(), Unique_Ships = integer(), stringsAsFactors = FALSE)

# Loop through each file and extract information using dplyr
for (file in csv_files) {
  # Read the CSV file
  df <- fread(file)
  print(file)
  
  # Process the data using dplyr pipes
  date_summary <- df %>%
    mutate(Date = as.Date(Date, format = "%Y_%m_%d", tz = "UTC")) %>%
    group_by(Date) %>%
    summarise(
      Total_Rows = n(),                          # Count total AIS signals
      Unique_Ships = n_distinct(scramblemmsi),        # Count unique ships
      .groups = 'drop'
    ) %>%
    mutate(
      Year = lubridate::year(Date),
      Month = lubridate::month(Date)
    ) %>%
    select(Year, Month, Date, Total_Rows, Unique_Ships)  # Reorder columns
  
  # Append to summary dataframe
  summary_df <- bind_rows(summary_df, date_summary)
}

# Print or save the summary
print(summary_df)

summary_df <- summary_df %>% group_by(Date) %>% summarize(Total_Rows = sum(Total_Rows), Unique_Ships = sum(Unique_Ships))

# Optionally, save it as a CSV for future reference
write.csv(summary_df, "../Data_Processed_V4/2015-2024_CLEAN_summary_by_date.csv", row.names = FALSE)


################################################################################
# Check for number of signals per day 2015-2024 CLEAN VECTOR data 


# Define the directory where the new CSV files are saved
dir <- "/mnt/home/kapsarke/Documents/AIS/Data_Processed_V4/Vector/"

# Get a list of all the processed CSV files
shp_files <- list.files(dir, pattern = ".shp", full.names = TRUE)


# Initialize an empty data frame
summary_df <- data.frame(Year = c(), 
                         Month = c(), 
                         Total_Rows = integer(), 
                         Unique_Ships = integer(), 
                         Ship_Type=character(), 
                         Distance_Km = numeric(), 
                         stringsAsFactors = FALSE)


# Initialize an empty data frame
summary_df_type_codes <- data.frame(
                         Month = c(), 
                         Ship_Type = factor(), 
                         AIS_Type = factor(), 
                         n = integer(),
                         stringsAsFactors = FALSE)

# Loop through each file and extract information using dplyr
for (file in shp_files) {
  # Read the SHP file
  df <- st_read(file, quiet=T)
  print(file)
  
  type_summary <- df %>% 
    st_drop_geometry() %>% 
    mutate(Month = floor_date(Tm_Strt, "month")) %>%  # Convert Date to first day of the month
    group_by(Month, Shp_Typ, AIS_Typ) %>% 
    summarize(n=length(unique(scrmblm)), 
              .groups = 'drop')
    
    
  # Process the data using dplyr pipes
  date_summary <- df %>%
    st_drop_geometry() %>% 
    mutate(Month = floor_date(Tm_Strt, "month")) %>%  # Convert Date to first day of the month
    group_by(Month) %>%
    summarise(
      Total_Rows = n(),                          # Count total AIS signals
      Unique_Ships = n_distinct(scrmblm),        # Count unique ships
      Ship_Type = unique(AIS_Typ), 
      Distance_Km = sum(Lngth_K),
      .groups = 'drop'
    ) %>%
    select(Month, Total_Rows, Unique_Ships, Ship_Type, Distance_Km)  # Reorder columns

  # Append to summary dataframe
  summary_df <- bind_rows(summary_df, date_summary)
  summary_df_type_codes <- bind_rows(summary_df_type_codes, type_summary)
}

# Print or save the summary


# Optionally, save it as a CSV for future reference
write.csv(summary_df, "../Data_Processed_V4/2015-2024_CLEAN_distance_summary_by_month_type.csv", row.names = FALSE)
write.csv(summary_df_type_codes, "../Data_Processed_V4/2015-2024_CLEAN_summary_vessel_type_codes.csv", row.names = FALSE)

################################################################################
# Plot timeseries 

# ls <- list.files("../Data_Raw", pattern = "_summary_", full.names = T)
# 
# ls <- lapply(ls, read.csv)
# 
# summary_df <- do.call(rbind, ls) %>% mutate(Date = as.Date(Date))
# summary_df <- read.csv("../Data_Processed_V4/2015-2024_CLEAN_summary_by_date.csv") %>% mutate(Date = as.Date(Date))
summary_df <- read.csv("../Data_Processed_V4/2015-2024_CLEAN_distance_summary_by_month_type.csv") %>% mutate(Date = as.Date(Month))

# Define time periods for shading
shaded_areas <- data.frame(
  xmin = as.Date(c("2015-01-01", "2023-01-01")),  # Start dates of shaded areas
  xmax = as.Date(c("2020-12-31", "2024-12-31")),  # End dates of shaded areas
  ymin = -Inf,  # Cover entire y-axis range
  ymax = Inf
)

# Set x-axis limits
# min_date <- as.Date("2015-01-01")  # Start at the earliest date in the data
min_date <- as.Date("2015-01-01")  
max_date <- as.Date("2024-12-31")  # End at December 31, 2024


# Create the improved line plot
p1 <- ggplot(summary_df, aes(x = Date, y = Total_Rows)) +
  # Add shaded background regions
  geom_rect(data = shaded_areas, aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax),
            fill = "gray", alpha = 0.2, inherit.aes = FALSE) +  # Semi-transparent gray
  geom_point(color = "red", size = 1, alpha = 0.5) +  # Smaller, semi-transparent points
  geom_line(color = "blue", size = 0.5, alpha = 0.7) +  # Thinner, slightly transparent line
  geom_smooth(method = "loess", color = "black", size = 1, se = FALSE) +  # Trend line without confidence band
  scale_x_date(limits = c(min_date, max_date), date_breaks = "6 months", date_labels = "%b %Y", expand=c(0,0)) +  # Set x-axis range
  labs(
    title = "",
    x = "Date",
    y = "Number of AIS Signals"
  ) +
  theme_minimal() +  # Clean theme
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1)  # Rotate x-axis labels
  )
p1
ggsave("../Figures/CleanData_SignalsPerDay.png", p1, width = 10, units = "in")

# Create the improved line plot
p2 <- ggplot(summary_df, aes(x = Date, y = Unique_Ships)) +
  # Add shaded background regions
  geom_rect(data = shaded_areas, aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax),
            fill = "gray", alpha = 0.2, inherit.aes = FALSE) +  # Semi-transparent gray
  geom_point(color = "red", size = 1, alpha = 0.5) +  # Smaller, semi-transparent points
  geom_line(color = "blue", size = 0.5, alpha = 0.7) +  # Thinner, slightly transparent line
  geom_smooth(method = "loess", color = "black", size = 1, se = FALSE) +  # Trend line without confidence band
  scale_x_date(limits = c(min_date, max_date), date_breaks = "6 months", date_labels = "%b %Y", expand=c(0,0)) +  # Set x-axis range
  labs(
    title = "",
    x = "Date",
    y = "Number of Unique MMSIs",
    caption = ""
  ) +
  theme_minimal() +  # Clean theme
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1)  # Rotate x-axis labels
  )
p2
ggsave("../Figures/CleanData_ShipsPerDay.png", p2, width = 10, units = "in")


# Distance line plot 

p3 <- ggplot(summary_df, aes(x = Date, y = Distance_Km/1000)) +
  # Add shaded background regions
  geom_rect(data = shaded_areas, aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax),
            fill = "gray", alpha = 0.2, inherit.aes = FALSE) +  # Semi-transparent gray
  geom_point(color = "red", size = 1, alpha = 0.5) +  # Smaller, semi-transparent points
  geom_line(color = "blue", size = 0.5, alpha = 0.7) +  # Thinner, slightly transparent line
  geom_smooth(method = "loess", color = "black", size = 1, se = FALSE) +  # Trend line without confidence band
  scale_x_date(limits = c(min_date, max_date), date_breaks = "6 months", date_labels = "%b %Y", expand=c(0,0)) +  # Set x-axis range
  labs(
    title = "",
    x = "Date",
    y = "Total Distance Traveled (1000s km)"
  ) +
  theme_minimal() +  # Clean theme
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1)  # Rotate x-axis labels
  )
p3

ggsave("../Figures/CleanData_TotalDistanceTravelled.png", p3, width = 10, units = "in")


# Total number of ships by type 

p4 <- ggplot(summary_df, aes(x = Date, y = Distance_Km/1000, group = Ship_Type, color=Ship_Type)) +
  # Add shaded background regions
  geom_rect(data = shaded_areas, aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax),
            fill = "gray", alpha = 0.2, inherit.aes = FALSE) +  # Semi-transparent gray
  geom_point(size = 1, alpha = 0.5) +  # Smaller, semi-transparent points
  geom_line(size = 0.5, alpha = 0.7) +  # Thinner, slightly transparent line
  geom_smooth(method = "loess", color = "black", size = 1, se = FALSE) +  # Trend line without confidence band
  scale_x_date(limits = c(min_date, max_date), date_breaks = "6 months", date_labels = "%b %Y", expand=c(0,0)) +  # Set x-axis range
  labs(
    title = "",
    x = "Date",
    y = "Total Distance Traveled (1000s km)"
  ) +
  theme_minimal() +  # Clean theme
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),  # Rotate x-axis labels
    text = element_text(size=20) 
  ) + 
  facet_wrap(facets =vars(Ship_Type), scales = "free_y")
p4

ggsave("../Figures/CleanData_NumberShips_ByType.png", p4, width = 20, height=10, units = "in")
