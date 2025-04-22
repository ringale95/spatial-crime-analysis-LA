-- Cleanup the data
raw_crimes = LOAD '/crime_data_analysis/output/filtered_crimes' USING PigStorage(',') 
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

-- Filter out records with header values or empty critical fields
filtered_crimes = FILTER raw_crimes BY 
                DR_NO IS NOT NULL AND 
                DR_NO != 'DR_NO' AND
                DATE_OCC IS NOT NULL;

-- Clean and transform the data
cleaned_crimes = FOREACH filtered_crimes GENERATE
    -- Rename fields
    DR_NO AS division_record_number,
    
    -- Clean Date_Rptd (remove time component)
    (INDEXOF(Date_Rptd, ' ') > 0 ? 
        SUBSTRING(Date_Rptd, 0, INDEXOF(Date_Rptd, ' ')) : 
        Date_Rptd) AS date_reported,
    
    -- Clean DATE_OCC (remove time component)
    (INDEXOF(DATE_OCC, ' ') > 0 ? 
        SUBSTRING(DATE_OCC, 0, INDEXOF(DATE_OCC, ' ')) : 
        DATE_OCC) AS date_occurred,
    
    -- Keep TIME_OCC and rename
    TIME_OCC AS time_of_occurrence,
    
    -- Keep AREA_NAME only (drop AREA code)
    AREA_NAME AS area_name,
    
    -- Rename Rpt_Dist_No
    Rpt_Dist_No AS reporting_district_number,
    
    -- Keep Part_1_2
    Part_1_2 AS part_category,
    
    -- Keep Crm_Cd_Desc and rename
    Crm_Cd_Desc AS crime_description,
    
    -- Keep Crm_Cd and rename
    Crm_Cd AS crime_code,
    
    -- Keep other demographic fields
    Mocodes AS modus_operandi_codes,
    Vict_Age AS victim_age,
    Vict_Sex AS victim_sex,
    Vict_Descent AS victim_descent,
    
    -- Keep Premis_Desc only and rename
    Premis_Desc AS premises_description,
    
    -- Keep Weapon_Desc only and rename
    Weapon_Desc AS weapon_description,
    
    -- Keep Status_Desc only and rename
    Status_Desc AS status_description,
    
    -- Keep location information
    LOCATION AS location,
    Cross_Street AS cross_street,
    LAT AS latitude,
    LON AS longitude;

-- Create a sample of cleaned data for inspection
cleaned_sample = LIMIT cleaned_crimes 10;
DUMP cleaned_sample;

-- Store the cleaned data
STORE cleaned_crimes INTO '/crime_data_analysis/output/cleaned_crimes' USING PigStorage(',');