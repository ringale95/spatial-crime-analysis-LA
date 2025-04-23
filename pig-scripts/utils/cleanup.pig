-- Load raw data
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

-- Remove header and null DR_NO - only filter the data rows
filtered_crimes = FILTER raw_crimes BY DR_NO != 'DR_NO' AND DR_NO IS NOT NULL;

-- Clean date and time, drop unused columns
-- Avoid using aliases for complex expressions
cleaned_crimes = FOREACH filtered_crimes GENERATE
    DR_NO,
    SUBSTRING(Date_Rptd, 0, 10),
    SUBSTRING(DATE_OCC, 0, 10),
    -- Convert military time to regular time with AM/PM
    (SIZE(TIME_OCC) > 0 ? 
        (((int)TIME_OCC >= 1200) ? 
            -- PM times
            ((int)TIME_OCC == 1200 ? 
                '12:00 PM' : 
                CONCAT(CONCAT((chararray)((int)TIME_OCC/100 - 12), ':'), 
                    CONCAT(((int)TIME_OCC%100 < 10 ? 
                        CONCAT('0', (chararray)((int)TIME_OCC%100)) : 
                        (chararray)((int)TIME_OCC%100)), ' PM'))
            ) : 
            -- AM times
            ((int)TIME_OCC == 0 ? 
                '12:00 AM' : 
                CONCAT(CONCAT((chararray)((int)TIME_OCC/100), ':'), 
                    CONCAT(((int)TIME_OCC%100 < 10 ? 
                        CONCAT('0', (chararray)((int)TIME_OCC%100)) : 
                        (chararray)((int)TIME_OCC%100)), ' AM'))
            )
        ) : TIME_OCC
    ),
    AREA,
    Rpt_Dist_No,
    Part_1_2,
    Crm_Cd,
    Mocodes,
    Vict_Age,
    Vict_Sex,
    Vict_Descent,
    Premis_Cd,
    Weapon_Used_Cd,
    Status,
    LOCATION,
    Cross_Street,
    LAT,
    LON;

-- Filter rows missing important values
final_clean = FILTER cleaned_crimes BY Crm_Cd IS NOT NULL;

-- Store cleaned output without header
STORE final_clean INTO '/crime_data_analysis/output/cleaned_crimes_data' USING PigStorage(',');

-- ========== LOOKUP TABLES ==========
-- Crime Code Lookup
crime_code_pairs = FOREACH raw_crimes GENERATE Crm_Cd, Crm_Cd_Desc;
crime_code_lookup = DISTINCT crime_code_pairs;
STORE crime_code_lookup INTO '/crime_data_analysis/output/lookups/crime_code_lookup' USING PigStorage(',');

-- Area Code Lookup
area_pairs = FOREACH raw_crimes GENERATE AREA, AREA_NAME;
area_lookup = DISTINCT area_pairs;
STORE area_lookup INTO '/crime_data_analysis/output/lookups/area_code_lookup' USING PigStorage(',');

-- Premise Code Lookup
premise_pairs = FOREACH raw_crimes GENERATE Premis_Cd, Premis_Desc;
premise_lookup = DISTINCT premise_pairs;
STORE premise_lookup INTO '/crime_data_analysis/output/lookups/premise_code_lookup' USING PigStorage(',');

-- Weapon Code Lookup
weapon_pairs = FOREACH raw_crimes GENERATE Weapon_Used_Cd, Weapon_Desc;
weapon_lookup = DISTINCT weapon_pairs;
STORE weapon_lookup INTO '/crime_data_analysis/output/lookups/weapon_code_lookup' USING PigStorage(',');

-- Status Code Lookup
status_pairs = FOREACH raw_crimes GENERATE Status, Status_Desc;
status_lookup = DISTINCT status_pairs;
STORE status_lookup INTO '/crime_data_analysis/output/lookups/status_code_lookup' USING PigStorage(',');