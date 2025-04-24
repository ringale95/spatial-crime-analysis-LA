-- Clean Crime Code Lookup
raw_crime_lookup = LOAD '/crime_data_analysis/output/lookups/crime_code_lookup' 
                  USING PigStorage(',') 
                  AS (code:chararray, description:chararray);

clean_crime_lookup = FILTER raw_crime_lookup BY 
                     code != 'Crm Cd' AND 
                     description != 'Crm Cd Desc' AND
                     code IS NOT NULL;

final_crime_lookup = FOREACH clean_crime_lookup GENERATE 
                     code, 
                     REPLACE(description, '"', '');

STORE final_crime_lookup INTO '/crime_data_analysis/output/lookups/clean_crime_code_lookup' 
      USING PigStorage(',');

-- Clean Area Code Lookup
raw_area_lookup = LOAD '/crime_data_analysis/output/lookups/area_code_lookup' 
                 USING PigStorage(',') 
                 AS (code:chararray, name:chararray);

clean_area_lookup = FILTER raw_area_lookup BY 
                    code != 'AREA' AND 
                    name != 'AREA NAME' AND
                    code IS NOT NULL;

STORE clean_area_lookup INTO '/crime_data_analysis/output/lookups/clean_area_code_lookup' 
      USING PigStorage(',');

-- Clean Premise Code Lookup
raw_premise_lookup = LOAD '/crime_data_analysis/output/lookups/premise_code_lookup' 
                    USING PigStorage(',') 
                    AS (code:chararray, description:chararray);

clean_premise_lookup = FILTER raw_premise_lookup BY 
                       code != 'Premis Cd' AND 
                       description != 'Premis Desc' AND
                       code IS NOT NULL;

final_premise_lookup = FOREACH clean_premise_lookup GENERATE 
                       code, 
                       REPLACE(description, '"', '');

STORE final_premise_lookup INTO '/crime_data_analysis/output/lookups/clean_premise_code_lookup' 
      USING PigStorage(',');

-- Clean Weapon Code Lookup
raw_weapon_lookup = LOAD '/crime_data_analysis/output/lookups/weapon_code_lookup' 
                   USING PigStorage(',') 
                   AS (code:chararray, description:chararray);

clean_weapon_lookup = FILTER raw_weapon_lookup BY 
                      code != 'Weapon Used Cd' AND 
                      description != 'Weapon Desc' AND
                      code IS NOT NULL;

final_weapon_lookup = FOREACH clean_weapon_lookup GENERATE 
                      code, 
                      REPLACE(description, '"', '');

STORE final_weapon_lookup INTO '/crime_data_analysis/output/lookups/clean_weapon_code_lookup' 
      USING PigStorage(',');

-- Clean Status Code Lookup
raw_status_lookup = LOAD '/crime_data_analysis/output/lookups/status_code_lookup' 
                   USING PigStorage(',') 
                   AS (code:chararray, description:chararray);

clean_status_lookup = FILTER raw_status_lookup BY 
                      code != 'Status' AND 
                      description != 'Status Desc' AND
                      code IS NOT NULL;

final_status_lookup = FOREACH clean_status_lookup GENERATE 
                      code, 
                      REPLACE(description, '"', '');

STORE final_status_lookup INTO '/crime_data_analysis/output/lookups/clean_status_code_lookup' 
      USING PigStorage(',');