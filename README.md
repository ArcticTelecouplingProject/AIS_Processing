# AIS Data Processing Pipeline

## Table of Contents

- [Introduction](#Introduction)
- [Data Sources and Preprocessing](#Data-Sources-and-Preprocessing)
- [Usage](#Usage)
- [Workflow](#Workflow)
- [Scripts](#Scripts)
- [Changeable Parameters](#Changeable-Parameters)
- [Location of Data](#Location-of-Data)
- [Areas in Progress](#Areas-in-Progress)
- [Contributors](#Contributors)
- [Contact Information](#Contact-information)
- [How to Contribute](#How-to-Contribute)

## Introduction (Updated from [Kapsar et al. 2022](https://www.sciencedirect.com/science/article/pii/S2352340922007387))

This repository contains code to generate a spatially explicit dataset of shipping intensity in the Pacific Arctic region from January 1, 2015 to December 31, 2024. We calculated shipping intensity based on Automatic Identification System (AIS) data, a type of GPS transmitter required by the International Maritime Organization on all ships over 300 gross tonnes on an international voyage, all cargo ships over 500 gross tonnes, and all passenger ships. We used AIS data received by the Spire (formerly exactEarth) satellite constellation, ensuring spatial coverage regardless of national jurisdiction or remoteness. Our analytical approach converted raw AIS input into monthly raster and vector datasets, separated by vessel type. We first filtered raw AIS messages to remove spurious records and GPS errors, then joined remaining vessel positional records with static messages including descriptive attributes. We further categorized these messages into one of eight general ship types (cargo, fishing, military, recreation, tanker, tug/tow, other, and unknown). These monthly datasets provide a critical snapshot of dynamic commercial and natural systems in the Pacific Arctic region. Recent declines in sea ice have lengthened the duration of the shipping season and have expanded the spatial coverage of large vessel routes, from the Aleutian Islands through the Bering Strait and into the southern Chukchi Sea. As vessel traffic has increased, the social and natural systems of these regions have been increasingly exposed to the risks posed by large ships, including oil spills, underwater noise pollution, large cetacean ship-strikes, and discharges of pollutants. This dataset provides scientific researchers, regulatory managers, local community members, maritime industry representatives, and other decision makers with a quantitative means to evaluate the distribution and intensity of shipping across space and through time.

---

### Data Sources and Preprocessing

The AIS data used in this workflow originates from **[Spire](https://spire.com/maritime/)** (formerly known as exactEarth). The data were acquired in three distinct batches:

- **2015-2020**
- **2021-2022**
- **2023-2024**

Each batch was pre-processed slightly differently by the provider (see below). To ensure compatibility across all three datasets, the preprocessing script (`1-Prep.R`) contains functions to standardize the formatting, structure, and variable names. This ensures that all datasets can be seamlessly integrated and processed in the same workflow.

---

## Usage

### Quickstart Guide

To execute the workflow, run the following command in the terminal:

`Rscript Master_Script.R 2023`

This will process AIS data for the year 2023. Ensure that the raw data is correctly placed in `../Data_Raw/` before running.

### Required Software and Dependencies

This workflow was developed in **R version X.X.X** and requires the following packages:

- `tidyverse`
- `sf`
- `data.table`
- `lubridate`
- `doParallel`
- `stringi`

To install all required packages, run:

### Hardware Requirements

For large AIS datasets, we recommend:

- **Memory**: At least 16GB RAM
- **Processor**: Multi-core CPU for parallel processing

---

## Workflow

### 1. Input Sources
- **2015–2020 AIS data**
- **2021–2022 AIS data**
- **2023–2024 AIS data**

### 2. Preparation
- **Rename files to include month of transmission** *(only for 2023–2024 data)*
- **Generate scrambled MMSIs** for anonymization

**Note:** The following steps are run **on each month of data individually**. The process varies slightly depending on the year of the data.

### 3. Pre-processing
- **Standardize column names** across input datasets
- **Ensure data types are correct** (e.g., speed is numeric)
- **Integrate static and position messages** *(only for 2015–2020 data)*

### 4. Cleaning
- Remove NA latitude/longitude values
- Remove MMSI entries with fewer than 9 digits
- Remove "Aids to Navigation"
- Anonymize ship IDs (e.g., using functions like `scramblemmsi`)
- Remove multiple repeat messages from the exact same location
- Identify stopped vessels based on parameters:
  - Speed < 2 km/hr for >1 hour
- Determine whether transmission occurred during **daytime or night**
  - Based on time, location, and solar position
- Identify ship type

### 5. Vectorization (Connecting the Dots)
- **Connect points into lines**
  - Do **not** connect if:
    - Time gap > 6 hours **or**
    - Distance gap > 60 km
  - If multiple non-null ship attributes (type, country, size, destination, etc.) appear within a line segment, return NA
- **Start new line segments** when:
  - A new month begins
  - Ship comes to a stop
  - (Optional) A **day/night transition** occurs *(not used this time)*
- **Connect segments** when a transition is detected (e.g., last point in one segment is connected to the first of the next)

---

## Scripts

### `1-Prep.R`
- **Purpose**: Contains functions for loading and preprocessing raw AIS data.

### `2-Cleaning.R`
- **Purpose**: Contains functions for removing invalid data points, filtering unnecessary records, and preparing for analysis.

### `3a-Vectorization.R` 
- **Purpose**: Contains functions for converting cleaned ship tracks into spatial LINESTRING geometries.

### `4-Metadata.R` 
- **Purpose**: Updates and stores metadata about the processed AIS data.
- **Key Functions**:
  - `update_metadata()`: Tracks unique vessel counts.
  - `shiptype_metadata()`: Updates metadata for each ship type.
  - `missing_data_metadata()`: Calculates the percentage of missing data in ship dimensions and speed.

### `5-MasterFunction.R` 
- **Purpose**: Consolidates all functions from `1-Prep.R`, `2-Cleaning.R`, `3a-Vectorization.R`, and `4-Metadata.R` into a single master function for data processing.
- **Functionality**:
  - Loads raw AIS data.
  - Cleans and processes data using functions from scripts 1, 2, 3, and 4.
  - Outputs processed data in specified formats (vector, hex).

### `Master_Script.R` 
- **Purpose**: Implements parallelized processing of AIS data.
- **Functionality**:
  - Loads raw AIS files for a given year.
  - Calls `process_ais_data()` in parallel using `foreach` and `doParallel`.
- **Outputs**: Parallelized execution of data processing, saving results efficiently.

---

## Changeable Parameters

The following parameters can be modified in `5-MasterFunction.R`:

- `output`: Specifies output format (`"vector"`, `"hex"`).
- `speed_threshold`: Speed below which a vessel is considered stopped (default: 2 knots).
- `time_threshold`: Minimum stop duration (default: 1 hour).
- `timediff_threshold`: Time gap defining new track segments (default: 6 hours).
- `distdiff_threshold`: Distance gap defining new track segments (default: 60 units).
- `daynight`: Specify whether output should be divided by whether it occurred during daytime and nighttime based on the latitude, longitude, timestamp, and solar position of the ship.

---

## Location of Data

Raw data is stored in `../Data_Raw/`, and processed outputs are saved in `../Data_Processed_V4/` under different formats (vector, hex, csv summary).

---

### Data Accessibility

- **Proprietary Data:** The raw AIS data, as well as the cleaned points and vector data, are proprietary and restricted for use within the team only. These datasets cannot be distributed beyond the authorized research group.
- **Publicly Available Data:** The hex-grid data and (once available) raster outputs will be published in the **NSF Arctic Data Center repository** ([arcticdata.io](https://arcticdata.io)) for public access and reuse.

---

## File Structure 
```
/AIS/                                      # Root project directory
│── /Data_Raw/                             # Contains raw AIS data (Proprietary, restricted access)
│   │── /YYYY/                             # Raw data (one folder per year) 
│   │── FlagCodes.csv                      # Country flag data
│   │── ScrambledMMSI_Keys_2015-2024.csv   # Scrambled MMSI mapping
│   │── Destination_Recoding.csv           # Destination codes
│   │── BlankHexes.shp                     # Hex grid for spatial analysis
│
│── /Data_Processed_V4/          # Contains processed outputs
│   │── /Points/                 # Cleaned AIS points (Proprietary)
│   │   │── Clean_Points_YYYY_MM_DD.csv  
│   │── /Vector/                 # Vectorized ship tracks (Proprietary)
│   │   │── Tracks_DayNightTrue_YYYYMM-ShipType.shp  
│   │   │── Tracks_DayNightFalse_YYYYMM-ShipType.shp  
│   │── /Hex/                    # Publicly available hex-grid data
│   │   │── Hex_YYYYMM_DayNightTrue.shp  
│   │── /Raster/                 # Placeholder for future raster outputs
│
│── /AISProcessing_V2/            # Scripts directory
│   │── /R/
│   │   │── 1-Prep.R                  # Preprocessing functions
│   │   │── 2-Cleaning.R              # Cleaning functions
│   │   │── 3a-Vectorization.R        # Vectorization functions
│   │   │── 4-Metadata.R              # Metadata tracking functions
│   │   │── 5-MasterFunction.R        # Master processing function
│   │── Master_Script.R           # Parallel processing script
│
│── /Metadata/                    # Stores metadata about processed datasets
│   │── Vector_Metadata_YYYYMM.csv
│   │── Hex_Metadata_YYYYMM.csv
│   │── Vector_Runtimes_YYYYMM.csv
│
│── README.md                     # Documentation and usage guidelines
│── install_packages.R             # Script to install required R dependencies
│── .gitignore                      # Specifies files to exclude from version control
```

---

### Key Notes on File Structure: 
- Raw data (`/Data_Raw/`): Stores input files obtained from Spire (exactEarth). These are not publicly shareable.
- Processed data (`/Data_Processed_V4/`): Stores cleaned and transformed outputs. The hex data and raster outputs (once available) will be publicly accessible, while cleaned points and vector data are proprietary.
- Scripts (`/R/`): Contains R scripts for preprocessing, cleaning, vectorizing, and managing metadata.
- Metadata (`/Metadata/`): Stores metadata files describing processing results and performance (useful for tracking issues and reproducibility).
- README.md: Provides documentation, workflow instructions, and guidelines.

---

## Areas in Progress

- **Raster Outputs**: The workflow currently supports vectorized and hex-grid outputs. Rasterization is not yet implemented.
- **Additional Data Cleaning Rules**: More criteria may be added to refine ship track segmentation.

---

## How to Contribute

We encourage contributions to improve the pipeline or adapt it to work with other data sources! Here’s how you can help:

1. **Report Issues**: If you encounter bugs, report them in the repository’s issue tracker.
2. **Suggest Enhancements**: Have ideas for improvement? Open a discussion!
3. **Follow Coding Conventions**:
   - Use consistent indentation and commenting.
   - Ensure functions are modular and well-documented.
4. **Submit a Pull Request (PR)**:
   - Fork the repository.
   - Make changes in a new branch.
   - Submit a PR describing your modifications.

---

## Contributors

Kelly Kapsar (2015-2020, 2021-2022, and 2023-2024)
Ben Sullender (2015-2020 data set)

---

## Funding 

These data were purchased with funding from the [Alaska Conservation Foundation](https://alaskaconservation.org/) (for the 2015-2020 data) and the National Science Foundation-funded ([Grant #2033507](https://www.nsf.gov/awardsearch/showAward?AWD_ID=2033507&HistoricalAwards=false)) [Arctic Telecoupling Project](https://arctictelecoupling.org/) (for the 2021-2022 and 2023-2024 data). 

## Contact Information

For inquiries, please contact Kelly Kapsar (kelly.kapsar@gmail.com).

