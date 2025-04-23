-- Create folders to load raw data  
--  ./hadoop fs -mkdir -p /crime_data_analysis/raw      
--  ./hadoop fs -mkdir -p /crime_data_analysis/output     

-- Load the raw data in Pig
raw_crimes = LOAD '/crime_data_analysis/raw/crime_data.csv'
             USING PigStorage(',')
             AS (DR_NO:chararray,
                 Date_Rptd:chararray,
                 DATE_OCC:chararray,
                 TIME_OCC:chararray,
                 AREA:chararray,
                 AREA_NAME:chararray,
                 Rpt_Dist_No:chararray,
                 Part_1_2:chararray,
                 Crm_Cd:chararray,
                 Crm_Cd_Desc:chararray,
                 Mocodes:chararray,
                 Vict_Age:chararray,
                 Vict_Sex:chararray,
                 Vict_Descent:chararray,
                 Premis_Cd:chararray,
                 Premis_Desc:chararray,
                 Weapon_Used_Cd:chararray,
                 Weapon_Desc:chararray,
                 Status:chararray,
                 Status_Desc:chararray,
                 Crm_Cd_1:chararray,
                 Crm_Cd_2:chararray,
                 Crm_Cd_3:chararray,
                 Crm_Cd_4:chararray,
                 LOCATION:chararray,
                 Cross_Street:chararray,
                 LAT:chararray,
                 LON:chararray);

small_sample = LIMIT raw_crimes 3;
DUMP small_sample;

-- Filter out invalid records but keep header
filtered_raw_crimes = FILTER raw_crimes BY DATE_OCC IS NOT NULL;

-- Store the filtered raw data for further processing
STORE filtered_raw_crimes INTO '/crime_data_analysis/output/filtered_crimes' USING PigStorage(',');