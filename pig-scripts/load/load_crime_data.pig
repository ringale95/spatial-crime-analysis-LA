

-- Create folders to load raw data 
--  ./hadoop fs -mkdir -p /crime_data_analysis/raw    
--  ./hadoop fs -mkdir -p /crime_data_analysis/output     

-- Load the crime data from HDFS: 
-- ./hadoop fs -copyFromLocal /Users/ingale.r/Downloads/Crime_Data_from_2020_to_Present.csv /crime_data_analysis/raw/crime_data.csv  

-- Load the raw data in Pig
raw_crimes = LOAD '/crime_data_analysis/raw/crime_data.csv'  
             USING PigStorage(',')               
             AS (DR_NO:long,                   
                 Date_Rptd:chararray,                   
                 DATE_OCC:chararray,                   
                 TIME_OCC:int,                   
                 AREA:int,                   
                 AREA_NAME:chararray,                   
                 Rpt_Dist_No:int,                   
                 Part_1_2:int,                  
                 Crm_Cd:int,                   
                 Crm_Cd_Desc:chararray,                   
                 Mocodes:chararray,                   
                 Vict_Age:int,                   
                 Vict_Sex:chararray,                   
                 Vict_Descent:chararray,                   
                 Premis_Cd:int,                   
                 Premis_Desc:chararray,                   
                 Weapon_Used_Cd:int,                   
                 Weapon_Desc:chararray,                  
                 Status:chararray,                   
                 Status_Desc:chararray,                   
                 Crm_Cd_1:int,                   
                 Crm_Cd_2:int,                  
                 Crm_Cd_3:int,                   
                 Crm_Cd_4:int,                   
                 LOCATION:chararray,                   
                 Cross_Street:chararray,                  
                 LAT:double,                   
                 LON:double);  

small_sample = LIMIT raw_crimes 3;  
DUMP small_sample;

-- Filter out header rows and invalid records
filtered_raw_crimes = FILTER raw_crimes BY 
                 DR_NO IS NOT NULL AND 
                 DR_NO > 0 AND
                 DATE_OCC IS NOT NULL;

-- Store the filtered raw data for further processing
STORE filtered_raw_crimes INTO '/crime_data_analysis/output/filtered_crimes' USING PigStorage(',');