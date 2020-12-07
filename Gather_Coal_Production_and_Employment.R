###### Gather the Coal Data ######

#options(warn = -1)

###### Clear Environment ###### 

rm(list=ls())

###### Load Necessary Packages ######

library(tidyverse)

###### Get Data #######

tmp <- tempfile()
download.file("https://arlweb.msha.gov/OpenGovernmentData/DataSets/MinesProdQuarterly.zip", tmp, method = "libcurl")
minesprodquarterly <- read.table(unz(tmp, "MinesProdQuarterly.txt"), sep = "|", header = T, stringsAsFactors = F, fill = TRUE)
unlink(tmp)

tmp <- tempfile()
download.file("https://arlweb.msha.gov/OpenGovernmentData/DataSets/Mines.zip", tmp, method = "libcurl")
mines <- read.table(unz(tmp, "Mines.txt"), sep = "|", header = T, stringsAsFactors = F, fill = TRUE)
unlink(tmp)

#write.csv(minesprodquarterly, )
#write.csv(mines, )

###### Merge Mines and Production Data ######

TQCPE <- merge(minesprodquarterly, mines, by = "MINE_ID")

# Keep all .x Variables # 

TQCPE <- TQCPE[, -grep("*.y", x=names(TQCPE))]
names(TQCPE) <- gsub("*.x", "", names(TQCPE))

###### Filter Data to only Include Kentucky Coal Mines ##### 

TQCPE <- TQCPE[which(TQCPE$COAL_METAL_IND == "C"),]

TQCPE$Date_qtr <- paste0(as.character(TQCPE$CAL_YR), "-", as.character(TQCPE$CAL_QTR))

###### Merge in Region and Mine Type Variables ###### 

# These indices are available for download within the repository #

mtypeindex <- read.csv(, stringsAsFactors = FALSE)
regionindex <- read.csv(, stringsAsFactors=FALSE)

# Normalize county name in both TQCPE and region index 

TQCPE$FIPS_CNTY_NM <- tolower(TQCPE$FIPS_CNTY_NM)

TQCPE <- merge(TQCPE, regionindex, by.x = "FIPS_CNTY_NM", by.y = "county")

TQCPE <- merge(TQCPE, mtypeindex, by.x = "SUBUNIT_CD", by.y = "SUBUNIT_CD")

TQCPE <- TQCPE %>%
  
         mutate(COAL_PRODUCTION = as.numeric(COAL_PRODUCTION), AVG_EMPLOYEE_CNT = as.numeric(AVG_EMPLOYEE_CNT),
                CAL_YR = as.numeric(CAL_YR), CAL_QTR = as.numeric(CAL_QTR))

# Write United States Database to the Data Warehouse Folder#

#write.csv(TQCPE,)

# Create Kentucky Database #

TQCPE_KY <- TQCPE[which(TQCPE$STATE == "KY"),]

# Write Databases to the Data Warehouse Folder#

#write.csv(TQCPE,)
#write.csv(TQCPE_KY,)

###### Generate growth rates that we can display in Tableau #######

          #### Production Calculations by Region for Page 1 ####
        
          #--- Year over Year Growth

          Growth_Rates_Total <-  TQCPE_KY %>%
  
                                  group_by(CAL_YR, CAL_QTR) %>%
                                  
                                  summarise(clpttp = sum(COAL_PRODUCTION, na.rm = TRUE)) %>%
                                  
                                  # Now do the growth rates # 
            
                                  ungroup() %>%
            
                                  mutate(clpttp_YOY_Total = (clpttp - lag(clpttp, n = 4L))/lag(clpttp, n = 4L)) %>%
                                  
                                  mutate(clpttp_Qtr_Total = (clpttp - lag(clpttp, n = 1L))/lag(clpttp, n = 1L)) %>%
            
                                  # Generate Region Variable for bind_rows
            
                                  mutate(Region = "Total", mtype = "Total")
  
          Growth_Rates_Region <-  TQCPE_KY %>%
                  
                                  group_by(Region,CAL_YR, CAL_QTR) %>%
            
                                  summarise(clpttp = sum(COAL_PRODUCTION, na.rm = TRUE)) %>%
            
                                  # Now do the growth rates #
            
                                  ungroup() %>%
            
                                  group_by(Region) %>%
            
                                  mutate(clpttp_YOY_Region = (clpttp - lag(clpttp, n = 4L))/lag(clpttp, n = 4L)) %>%
            
                                  mutate(clpttp_Qtr_Region = ((clpttp - lag(clpttp, n = 1L))/lag(clpttp, n = 1L)))
            
          GrowthRatesByRegionPage1 <- bind_rows(Growth_Rates_Region, Growth_Rates_Total) %>%
            
                                      # Generate Date Qtr Variable
            
                                      mutate(Date_Qtr = paste0(as.character(CAL_YR),"-", as.character(CAL_QTR*3), "-", "01")) %>%
            
                                      mutate(clpttp_YOY_Region = if_else(is.na(clpttp_YOY_Region), clpttp_YOY_Total, clpttp_YOY_Region)) %>%
                        
                                      mutate(clpttp_Qtr_Region = if_else(is.na(clpttp_Qtr_Region), clpttp_Qtr_Total, clpttp_Qtr_Region))
            
          #write.csv(GrowthRatesByRegionPage1,)
          
          # Production Calculations by Mine Type for Page 1 #
          
          Growth_Rates_Mtype <-  TQCPE_KY %>%
                                  
                                  group_by(mtype, CAL_YR, CAL_QTR) %>%
                                  
                                  summarise(clpttp = sum(COAL_PRODUCTION, na.rm = TRUE)) %>%
                                  
                                  # Now do the growth rates #
                                  
                                  ungroup() %>%
                                  
                                  group_by(mtype) %>%
                                  
                                  mutate(clpttp_YOY_Mtype = (clpttp - lag(clpttp, n = 4L))/lag(clpttp, n = 4L)) %>%
                                  
                                  mutate(clpttp_Qtr_Mtype = ((clpttp - lag(clpttp, n = 1L))/lag(clpttp, n = 1L))) 
          
          GrowthRatesByMtypePage1 <- bind_rows(Growth_Rates_Mtype, Growth_Rates_Total) %>%
            
                                    # Generate Date Qtr Variable
                                    
                                    mutate(Date_Qtr = paste0(as.character(CAL_YR),"-", as.character(CAL_QTR*3), "-", "01")) %>%
            
                                    mutate(clpttp_YOY_Mtype = if_else(is.na(clpttp_YOY_Mtype), clpttp_YOY_Total, clpttp_YOY_Mtype)) %>%
                                    
                                    mutate(clpttp_Qtr_Mtype = if_else(is.na(clpttp_Qtr_Mtype), clpttp_Qtr_Total, clpttp_Qtr_Mtype))
          
          #write.csv(GrowthRatesByMtypePage1, )          
                    
          #### Production Calculations for Eastern Kentucky Page 2 ####
          
          Growth_Rates_East <-  TQCPE_KY %>%
            
                                  filter(Region == "East") %>%
            
                                  group_by(CAL_YR, CAL_QTR) %>%
                                  
                                  summarise(clpttp = sum(COAL_PRODUCTION, na.rm = TRUE)) %>%
                                  
                                  # Now do the growth rates # 
                                  
                                  ungroup() %>%
                                  
                                  mutate(clpttp_YOY_East = (clpttp - lag(clpttp, n = 4L))/lag(clpttp, n = 4L)) %>%
                                  
                                  mutate(clpttp_Qtr_East = (clpttp - lag(clpttp, n = 1L))/lag(clpttp, n = 1L)) %>%
                                  
                                  # Generate Region Variable for bind_rows
                                  
                                  mutate(mtype = "Total")
          
          
          Growth_Rates_MtypePage2 <-  TQCPE_KY %>%
            
                                      filter(Region == "East") %>%
                  
                                      group_by(mtype, CAL_YR, CAL_QTR) %>%
                                      
                                      summarise(clpttp = sum(COAL_PRODUCTION, na.rm = TRUE)) %>%
                                      
                                      # Now do the growth rates #
                                      
                                      ungroup() %>%
                                      
                                      group_by(mtype) %>%
                                      
                                      mutate(clpttp_YOY_Mtype = (clpttp - lag(clpttp, n = 4L))/lag(clpttp, n = 4L)) %>%
                                      
                                      mutate(clpttp_Qtr_Mtype = ((clpttp - lag(clpttp, n = 1L))/lag(clpttp, n = 1L))) 
          
          GrowthRatesByMtypePage2 <- bind_rows(Growth_Rates_MtypePage2, Growth_Rates_East) %>%
            
                                    # Generate Date Qtr Variable
                                    
                                    mutate(Date_Qtr = paste0(as.character(CAL_YR),"-", as.character(CAL_QTR*3), "-", "01")) %>%
                                    
                                    mutate(clpttp_YOY_Mtype = if_else(is.na(clpttp_YOY_Mtype), clpttp_YOY_East, clpttp_YOY_Mtype)) %>%
                                    
                                    mutate(clpttp_Qtr_Mtype = if_else(is.na(clpttp_Qtr_Mtype), clpttp_Qtr_East, clpttp_Qtr_Mtype))
                                  
          #write.csv(GrowthRatesByMtypePage2, )
          
          #### Production Calculations for Western Kentucky Page 3 ####
          
          Growth_Rates_West <-  TQCPE_KY %>%
            
                                filter(Region == "West") %>%
                                
                                group_by(CAL_YR, CAL_QTR) %>%
                                
                                summarise(clpttp = sum(COAL_PRODUCTION, na.rm = TRUE)) %>%
                                
                                # Now do the growth rates # 
                                
                                ungroup() %>%
                                
                                mutate(clpttp_YOY_West = (clpttp - lag(clpttp, n = 4L))/lag(clpttp, n = 4L)) %>%
                                
                                mutate(clpttp_Qtr_West = (clpttp - lag(clpttp, n = 1L))/lag(clpttp, n = 1L)) %>%
                                
                                # Generate Region Variable for bind_rows
                                
                                mutate(mtype = "Total")
          
          
          Growth_Rates_MtypePage3 <-  TQCPE_KY %>%
            
                                      filter(Region == "West") %>%
                                      
                                      group_by(mtype, CAL_YR, CAL_QTR) %>%
                                      
                                      summarise(clpttp = sum(COAL_PRODUCTION, na.rm = TRUE)) %>%
                                      
                                      # Now do the growth rates #
                                      
                                      ungroup() %>%
                                      
                                      group_by(mtype) %>%
                                      
                                      mutate(clpttp_YOY_Mtype = (clpttp - lag(clpttp, n = 4L))/lag(clpttp, n = 4L)) %>%
                                      
                                      mutate(clpttp_Qtr_Mtype = ((clpttp - lag(clpttp, n = 1L))/lag(clpttp, n = 1L))) 
          
          
          GrowthRatesByMtypePage3 <- bind_rows(Growth_Rates_MtypePage3, Growth_Rates_West) %>%
            
                                    # Generate Date Qtr Variable
                                    
                                    mutate(Date_Qtr = paste0(as.character(CAL_YR),"-", as.character(CAL_QTR*3), "-", "01")) %>%
                                    
                                    mutate(clpttp_YOY_Mtype = if_else(is.na(clpttp_YOY_Mtype), clpttp_YOY_West, clpttp_YOY_Mtype)) %>%
                                    
                                    mutate(clpttp_Qtr_Mtype = if_else(is.na(clpttp_Qtr_Mtype), clpttp_Qtr_West, clpttp_Qtr_Mtype))
          
          #write.csv(GrowthRatesByMtypePage3, )
          
          
          #### Production by County #### 
          
          proper=function(x) paste0(toupper(substr(x, 1, 1)), tolower(substring(x, 2)))
          
          Growth_Rates_County <-  TQCPE_KY %>%
            
                                  group_by(FIPS_CNTY_NM, CAL_YR, CAL_QTR) %>%
                                  
                                  summarise(clpctp = sum(COAL_PRODUCTION, na.rm = TRUE)) %>%
                                  
                                  ## Now do the growth rates ## 
                                  
                                  ungroup() %>%
                                  
                                  mutate(clpctp_YOY_County = (clpctp - lag(clpctp, n = 4L))/lag(clpctp, n = 4L)) %>%
            
                                  mutate(clpctp_Qtr_County = (clpctp - lag(clpctp, n = 1L))/lag(clpctp, n = 1L)) %>%
            
                                  ## Get Region Variable ##
            
                                  left_join(regionindex, by = c("FIPS_CNTY_NM" = "county")) %>%
            
                                  # Change County Name #
            
                                  mutate(FIPS_CNTY_NM = proper(FIPS_CNTY_NM)) %>%
          
                                  mutate(FIPS_CNTY_NM = if_else(FIPS_CNTY_NM == "Mclean", "McLean", FIPS_CNTY_NM), FIPS_CNTY_NM == if_else(FIPS_CNTY_NM == "Mccreary", "McCreary", FIPS_CNTY_NM)) %>%
            
                                  mutate(Date_Qtr = paste0(as.character(CAL_YR),"-", as.character(CAL_QTR*3), "-", "01"))
          
          #write.csv(Growth_Rates_County, )
          

# Employment Data and Growth Rates ----------------------------------------

          Emp_by_Mtype <- TQCPE_KY %>%
            
                              group_by(mtype, CAL_YR, CAL_QTR) %>%
                              
                              summarise(clpetp = sum(AVG_EMPLOYEE_CNT, na.rm = TRUE)) %>% 
            
                              ungroup() %>%
            
                              mutate(clpetp_YOY_mtype = (clpetp - lag(clpetp, n = 4L))/lag(clpetp, n = 4L)) %>%
            
                              mutate(clpetp_YOY_AbsChange = (clpetp - lag(clpetp, n = 4L))) %>%
                              
                              mutate(clpetp_Qtr_mtype = (clpetp - lag(clpetp, n = 1L))/lag(clpetp, n = 1L)) %>%
            
                              mutate(clpetp_Qtr_AbsChange = (clpetp - lag(clpetp, n = 1L)))
  
          Emp_GrowthRates_Total <- TQCPE_KY %>%
            
                                    group_by(CAL_YR, CAL_QTR) %>%
                                    
                                    summarise(clpetp = sum(AVG_EMPLOYEE_CNT, na.rm = TRUE)) %>% 
                                    
                                    ungroup() %>%
                                    
                                    mutate(clpetp_YOY_Total = (clpetp - lag(clpetp, n = 4L))/lag(clpetp, n = 4L)) %>%
                                    
                                    mutate(clpetp_YOY_TotalChange = (clpetp - lag(clpetp, n = 4L))) %>%
            
                                    mutate(clpetp_Qtr_Total = (clpetp - lag(clpetp, n = 1L))/lag(clpetp, n = 1L)) %>%
            
                                    mutate(clpetp_Qtr_TotalChange = (clpetp - lag(clpetp, n = 1L))) %>%
            
                                    mutate(mtype = "Total", Region = "Total")
          
          
          Emp_by_MineType <- bind_rows(Emp_by_Mtype, Emp_GrowthRates_Total) %>%
          
                              mutate(Date_Qtr = paste0(as.character(CAL_YR),"-", as.character(CAL_QTR*3), "-", "01")) %>%
                                    
                              mutate(clpetp_YOY_mtype = if_else(is.na(clpetp_YOY_mtype), clpetp_YOY_Total, clpetp_YOY_mtype)) %>%
                                    
                              mutate(clpetp_Qtr_mtype = if_else(is.na(clpetp_Qtr_mtype), clpetp_Qtr_Total, clpetp_Qtr_mtype)) %>%
            
                              mutate(clpetp_YOY_AbsChange = if_else(is.na(clpetp_YOY_AbsChange), clpetp_YOY_TotalChange, clpetp_YOY_AbsChange)) %>%
                              
                              mutate(clpetp_Qtr_AbsChange = if_else(is.na(clpetp_Qtr_AbsChange), clpetp_Qtr_TotalChange, clpetp_Qtr_AbsChange))
          
                                   
          #write.csv(Emp_by_MineType, )
            
          # Employment by County
          
          Emp_Growth_Rates_ByRegion <- TQCPE_KY %>%
            
                                      group_by(Region, CAL_YR, CAL_QTR) %>%
            
                                      summarise(clprte = sum(AVG_EMPLOYEE_CNT, na.rm = TRUE)) %>%
            
                                      ungroup() %>%
            
                                      mutate(clprte_YOY_byRegion = (clprte - lag(clprte, n = 4L))/lag(clprte, n = 4L)) %>%
                                      
                                      mutate(clprte_YOY_AbsChange = (clprte - lag(clprte, n = 4L))) %>%
            
                                      mutate(clprte_Qtr_byRegion = (clprte - lag(clprte, n = 1L))/lag(clprte, n = 1L)) %>%
            
                                      mutate(clprte_Qtr_AbsChange = (clprte - lag(clprte, n = 1L)))
          
          Emp_GrowthRates_County <-  TQCPE_KY %>%
            
                                  group_by(FIPS_CNTY_NM, CAL_YR, CAL_QTR) %>%
                                  
                                  summarise(clpcte = sum(AVG_EMPLOYEE_CNT, na.rm = TRUE)) %>%
                                  
                                  ## Now do the growth rates ## 
                                  
                                  ungroup() %>%
                                  
                                  mutate(clpcte_YOY_County = (clpcte - lag(clpcte, n = 4L))/lag(clpcte, n = 4L)) %>%
                                  
                                  mutate(clpcte_YOY_AbsChange = (clpcte - lag(clpcte, n = 4L))) %>%
            
                                  mutate(clpcte_Qtr_County = (clpcte - lag(clpcte, n = 1L))/lag(clpcte, n = 1L)) %>%
                                  
                                  mutate(clpcte_Qtr_AbsChange = (clpcte - lag(clpcte, n = 1L))) %>%
            
                                  ## Get Region Variable ##
                                  
                                  left_join(regionindex, by = c("FIPS_CNTY_NM" = "county")) %>%
                                  
                                  # Change County Name #
            
                                  mutate(FIPS_CNTY_NM = proper(FIPS_CNTY_NM)) %>%
                                  
                                  mutate(FIPS_CNTY_NM = if_else(FIPS_CNTY_NM == "Mclean", "McLean", FIPS_CNTY_NM), FIPS_CNTY_NM == if_else(FIPS_CNTY_NM == "Mccreary", "McCreary", FIPS_CNTY_NM)) 
            
          Emp_Growth_Rates_Page4 <- bind_rows(Emp_GrowthRates_County, Emp_Growth_Rates_ByRegion) %>%    
            
                                    mutate(clpcte_YOY_County = if_else(is.na(clpcte_YOY_County), clprte_YOY_byRegion, clpcte_YOY_County)) %>%
                                    
                                    mutate(clpcte_Qtr_County = if_else(is.na(clpcte_Qtr_County), clprte_Qtr_byRegion, clpcte_Qtr_County)) %>%
                                    
                                    mutate(clpcte_YOY_AbsChange = if_else(is.na(clpcte_YOY_AbsChange), clprte_YOY_AbsChange, clpcte_YOY_AbsChange)) %>%
                                    
                                    mutate(clpcte_Qtr_AbsChange = if_else(is.na(clpcte_Qtr_AbsChange), clprte_Qtr_AbsChange, clpcte_Qtr_AbsChange)) %>%
            
                                    mutate(clpcte = if_else(is.na(clpcte), clprte, clpcte)) %>%
            
                                    mutate(FIPS_CNTY_NM = if_else(is.na(FIPS_CNTY_NM), Region, FIPS_CNTY_NM)) %>%
          
                                    mutate(Date_Qtr = paste0(as.character(CAL_YR),"-",as.character(CAL_QTR*3), "-", "01")) 
            
          #write.csv(Emp_Growth_Rates_Page4, )
          

# Page 5 : Coal Production Table ------------------------------------------

          Emp_GrowthRates_byRegion <- bind_rows(Emp_Growth_Rates_ByRegion, Emp_GrowthRates_Total) %>%
            
                                      mutate(Date_Qtr = paste0(as.character(CAL_YR),"-", as.character(CAL_QTR*3), "-", "01")) %>%
                                      
                                      mutate(clprte_YOY_byRegion = if_else(is.na(clprte_YOY_byRegion), clpetp_YOY_Total, clprte_YOY_byRegion)) %>%
                                      
                                      mutate(clprte_Qtr_byRegion = if_else(is.na(clprte_Qtr_byRegion), clpetp_Qtr_Total, clprte_Qtr_byRegion)) %>%
            
                                      mutate(clprte = if_else(is.na(clprte), clpetp, clprte))
          
          #write.csv(Emp_GrowthRates_byRegion, )
            
                            
          
          
          
