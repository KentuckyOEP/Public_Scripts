###### Gather United States Census Data ######

# This module will pull together data from different sources in the Q:/Data_Warehouse/ folder to form a comprehensive set 
# of data structured by state and year

##### Clear Environment #####

rm(list=ls())

##### Load Necessary Packages #####

library(tidyverse)
library(httr)
library(jsonlite) 
library(prophet)
library(lubridate)
library(readxl)
library(curl)

###### Establish API Key. Please Register for the Census API at https://www.census.gov/data/developers/guidance/api-user-guide.html ######

## Fill in your API Key Below ##

#key <- 

###### Load Necessary Indices ######

#stateindex <- read_csv("C:/Database_Administration/Data/Working_Data/National/US Data/Annual/State_CodeName_Index.csv")

###### Total Household Data By County ######

# Build URL # 

for(yr in seq(2010,2100,1)){
  
  censusurl = paste0("https://api.census.gov/data/",yr,"/acs/acs5/subject?get=NAME,group(S2504)&for=county:*&in=state:*&key=",key)
  
  chicken <- GET(url = censusurl)
  
  # if the request does not go through successfully, Break the loop
  
  if(chicken$status_code == 200){
    
    raw.content <- rawToChar(chicken$content)
    
    content <- fromJSON(raw.content) 
    
    assign(paste("HousingPhysChar", yr, sep = "_"), content)      
    
    next
    
  }
  
  else{
    
    break
    
  }
  
}

EnvData <- as.list(.GlobalEnv)

hhpc_vars <- grep("HousingPhysChar_*",names(as.list(.GlobalEnv)), value = TRUE)

hhpc_Data <- EnvData[hhpc_vars]

## Convert HH Data to Tibbles for Bind_Rows ##

hhpc_DataFrames <- lapply(hhpc_Data, function(x){as.data.frame(x, stringsAsFactors = FALSE)})

for(df in 1:length(hhpc_DataFrames)){
  
  names(hhpc_DataFrames[[df]]) <- hhpc_DataFrames[[df]][1,]
  
  # Drop First Row of each Data Frame
  
  hhpc_DataFrames[[df]] <- hhpc_DataFrames[[df]][2:nrow(hhpc_DataFrames[[df]]),]
  
  try(hhpc_DataFrames[[df]] <- hhpc_DataFrames[[df]][, !duplicated(colnames(hhpc_DataFrames[[df]]))])
  
}

HHPCData <- bind_rows(hhpc_DataFrames, .id = "year")

HouseholdPhysicalChar <- HHPCData %>%
  
  mutate(year = str_extract(year, "[0-9]{4}")) %>%
  
  select(-state, -county) %>%
  
  # Create County and State Vars #
  
  separate(NAME, into = c("County", "State"), sep = ",") %>%
  
  # Trim Variables #
  
  mutate(State = str_trim(State, side = "both")) %>%
  
  mutate(County = str_remove(County, pattern = "Parish|County|Census Area|Borough|Municipality|Municipio")) %>%
  
  mutate(County = tolower(str_trim(County, side = "both"))) %>% 
  
  rename(county = County) %>%
  
  left_join(stateindex, by = c("State" = "name")) 


## Select the Variables We Need For a Smaller Dataset. Shelve the Larger Dataset In Case We Need it Later. We can always bring in More Variables as Needed ##

HouseholdPhysicalCharacteristics <- HouseholdPhysicalChar %>%
  
  rename(OccHU = S2504_C01_001E, YrBuilt_2014plus = S2504_C01_009E, YrBuilt_2010_2013 = S2504_C01_010E, YrBuilt_2000_2009 = S2504_C01_011E,
         YrBuilt_1980_1999 = S2504_C01_012E, YrBuilt_1960_1979 = S2504_C01_013E, YrBuilt_1940_1959 = S2504_C01_014E, YrBuilt_1939OrLess = S2504_C01_015E, UtilityGas = S2504_C01_032E,
         Heating_LPGas = S2504_C01_033E, Heating_Elec = S2504_C01_034E, Heating_FuelOil = S2504_C01_035E, Heating_Coal= S2504_C01_036E, Heating_Other = S2504_C01_037E, 
         Heating_None = S2504_C01_038E) %>%
  
  # Drop Every Variable that has not been renamed #
  
  select(year, State, county, statecode, starts_with("YrBuilt"), starts_with("Heating"), starts_with("OccHU"))

#write.csv(HouseholdPhysicalCharacteristics, "C:/Database_Administration/Data_Warehouse/Housing/County/Annual/PhysicalCharacteristicsByCounty.csv")









