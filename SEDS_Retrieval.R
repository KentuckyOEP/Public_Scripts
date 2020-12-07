    ##### Clear Environment ##### 
  
  rm(list=ls())
  
  ###### Load Necessary Packages ##### 
  
  library(tidyverse)
  library(httr)
  library(jsonlite) 
  library(prophet)
  library(lubridate)
  library(readxl)
  
  ###### Load Indexes ######
  
  ## This is a simple index of State Names and State Abbreviations. These Indices are Available in the Repository ##
  
  stateindex <- read_csv("C:/Database_Administration/Data/Working_Data/National/US Data/Annual/State_CodeName_Index.csv")
  
  ###### Reading In EIA Bulk Data Files ######
  
  # Stream import large data from zip file
  
  tmp <- tempfile()
  download.file("https://api.eia.gov/bulk/SEDS.zip", tmp)
  seds.bulk <- stream_in(unz(tmp, "SEDS.txt"))
  
  ###### Alright, Let's Reshape the Data to a large list of lists to a Panel Data Frame That We Would Find More Useful  ####### 
  
  SEDS <- lapply(seds.bulk$data, function(x){as.data.frame(x, stringsAsFactors = F)})
  
  for(i in 1:length(SEDS)){
  
    SEDS[[i]][,"geography"] <- seds.bulk$geography[i]
    SEDS[[i]][,"group"] <- seds.bulk$geoset_id[i]
  
  }
  
  ## Bind Data Set Together
  
  SEDS <- bind_rows(SEDS[1:length(SEDS)])
  
  ## Spread the Data Set ###
  
  SEDS <- SEDS %>%
    
             mutate(geography = ifelse(str_length(geography) != 6, geography, str_sub(geography, 5, 6))) %>%
    
             filter(!is.na(geography)) %>%
    
             # Create codeyr variable 
    
             mutate(codeyr = paste(geography, V1, sep = "_"), group = str_sub(group, 6, 10)) %>%
             
             spread(group, V2) %>%
    
             rename(year = V1, code = geography) %>%
    
             left_join(stateindex, by = c("code" = "statecode")) %>% 
    
             arrange(code, year)
  
  #### Retrieve the Generation by Fuel Data. It will Be Integrated into Larger SEDS Data #### 
  
  ## URL for Monthly Generation by State ## 
  
  url1 <- "https://www.eia.gov/electricity/data/state/generation_monthly.xlsx"
  
  ## Apply get request from HTTR package ## 
  
  GET(url1, write_disk(tf <- tempfile(fileext = ".xlsx")))
  
  ## Get Sheets ##
  
  sheets <- excel_sheets(tf)
  
  ## Delete Unnecessary Sheets ##
  
  sheets <- sheets[which(sheets != "EnergySource_Notes")]
  
  ## Have to do two separate calls of read_excel. Older documents don't require a skip ##
  
  gen_monthly1 <- lapply(sheets[1:5], read_excel, path = tf, trim_ws = TRUE)
  
  gen_monthly2 <- lapply(sheets[6:length(sheets)], read_excel, path = tf, skip = 4, trim_ws = TRUE)
  
  # Assign Names to the List #
  
  names(gen_monthly1) <- sheets[1:5]
  
  names(gen_monthly2) <- sheets[6:length(sheets)]
  
  for(sheet in sheets[1:5]){
    
    names(gen_monthly1[[sheet]]) <- c("year", "month", "state", "typeofproducer", "energysource", "generation") 
    
  }
  
  for(sheet in sheets[6:length(sheets)]){
    
    names(gen_monthly2[[sheet]]) <- c("year", "month", "state", "typeofproducer", "energysource", "generation") 
    
  }
  
  gen_monthly_total1 <- bind_rows(gen_monthly1)
  gen_monthly_total2 <- bind_rows(gen_monthly2)
  
  # Rename all variables in both lists for a successful row bind
  
  gen_monthly_total <- bind_rows(gen_monthly_total1, gen_monthly_total2) %>% 
    
                       mutate(year_month = paste(year, month, "01", sep = "-")) %>%
    
                       filter(typeofproducer == "Total Electric Power Industry")
  
  # Spread gen_monthly_total for use in USDATA monthly Data # 
  
  US_Gen_Monthly <- gen_monthly_total %>%
    
                    spread(energysource, generation)
  
  # write.csv(US_Gen_Monthly,)
  
  ########## Onward to a Tidy Annual File ##########
  
  ## Replace Energy Source Variable with a Different Name. We Will Spread This Variable Over the Dataset
  
  gen_monthly_total$energysource[gen_monthly_total$energysource == "Total"] <- "EGTEP"
  gen_monthly_total$energysource[gen_monthly_total$energysource == "Coal"] <- "EGCLP"
  gen_monthly_total$energysource[gen_monthly_total$energysource == "Geothermal"] <- "EGGEP"
  gen_monthly_total$energysource[gen_monthly_total$energysource == "Hydroelectric Conventional"] <- "EGHYP"
  gen_monthly_total$energysource[gen_monthly_total$energysource == "Natural Gas"] <- "EGNGP"
  gen_monthly_total$energysource[gen_monthly_total$energysource == "Nuclear"] <- "EGNUP"
  gen_monthly_total$energysource[gen_monthly_total$energysource == "Petroleum"] <- "EGPAP"
  gen_monthly_total$energysource[gen_monthly_total$energysource == "Pumped Storage"] <- "EGPSP"
  gen_monthly_total$energysource[gen_monthly_total$energysource == "Solar Thermal and Photovoltaic"] <- "EGSOP"
  gen_monthly_total$energysource[gen_monthly_total$energysource == "Wind"] <- "EGWYP"
  gen_monthly_total$energysource[gen_monthly_total$energysource == "Wood and Wood Derived Fuels"] <- "EGWDP"
  gen_monthly_total$energysource[gen_monthly_total$energysource == "Other"] <- "EGOTP"
  gen_monthly_total$energysource[gen_monthly_total$energysource == "Other Gases"] <- "EGOGP"
  gen_monthly_total$energysource[gen_monthly_total$energysource == "Other Biomass"] <- "EGOBMP"
  
  
  # Save Monthly Totals as .csv and .rds #
  
  # write.csv(gen_monthly_total, )
  
  #saveRDS(gen_monthly_total, )
    
  # Create Annual File 
  
  gen_annual_tidy <-  gen_monthly_total %>%
                              
                      filter(typeofproducer == "Total Electric Power Industry") %>% 
                              
                      mutate(state = if_else(state == "US-TOTAL" | state == "US-Total", "US", state)) %>%
                              
                      group_by(year, state, energysource) %>% 
                              
                      summarise(annual_gen = sum(generation, na.rm = TRUE)) %>%
    
                      spread(energysource, annual_gen) %>%
                              
                      replace(is.na.data.frame(.), 0) %>%
    
                      mutate(codeyr = paste0(state, "_", year), EGREP = EGSOP + EGWDP + EGOBMP + EGPSP + EGGEP + EGWYP + EGHYP) %>% 
                              
                      ungroup() %>%
                              
                      rename(statecode = state)
  
  # write.csv(gen_annual_tidy, )
  
  ###### Merge Fuel Generation Data with SEDS Data ###### 
  
  SEDS <- SEDS %>% 
    
          left_join(gen_annual_tidy, by = "codeyr") %>%
    
          rename(year = year.x) %>%
    
          select(-year.y, -statecode)
          
  # Alright. Now we have a nice clean dataset that we can insheet and use for dashboarding. Let's make a codebook to go with that dataset. We will generate it from the data, so it will be automatically updated every time. 
  
  state.names <- c("Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|District of Columbia|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|United States|Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming")
  
  codebook.df <-  data.frame(Variable = seds.bulk$geoset_id, Description = seds.bulk$description, Units = seds.bulk$units, geography = seds.bulk$geography) %>% 
    
                  filter(!is.na(geography)) %>%
    
                  select(Variable, Description, Units) %>%
    
                  mutate(Description = str_replace_all(Description, state.names, ""), Description = str_replace(Description, "See.*$", ""), Description = str_replace(Description, ",", "")) %>%
    
                  mutate(Variable = str_sub(Variable, 6, 10))
  
  codebook <- unique.data.frame(codebook.df)
  
  codebook <- codebook %>%
  
              arrange(Variable)
  
  ##### Save the Data Set on the Q Drive ##### 
  
  # write.csv(SEDS, )
  # write.csv(codebook, )
  
  ##### Save a copy of the R Database as Well #####
  
  # saveRDS(SEDS,)
  

