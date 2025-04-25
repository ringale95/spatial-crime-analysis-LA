USE crime_analysis;

DROP TABLE IF EXISTS premise_code_lookup;

CREATE EXTERNAL TABLE IF NOT EXISTS premise_code_lookup (
  code STRING,
  Premis_Desc STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/crime_data_analysis/output/lookups/backup/premise_code_lookup_orig/premise_code_lookup/';

-- Step 1: Drop if exists
DROP TABLE IF EXISTS crime_premise_correlation;

-- Step 2: Create analysis table
CREATE TABLE crime_premise_correlation AS
SELECT 
  TRIM(cr.Crm_Cd_Desc) AS crime_type,
  TRIM(p.Premis_Desc) AS premise_type,
  COUNT(*) AS incident_count
FROM crimes_cleaned_resolution_ready c
LEFT JOIN crime_code_lookup cr 
  ON TRIM(c.crm_cd) = TRIM(cr.Crm_Cd)
LEFT JOIN premise_code_lookup p 
  ON TRIM(c.premis_cd) = TRIM(p.code)
WHERE 
  c.crm_cd IS NOT NULL AND 
  c.premis_cd IS NOT NULL
GROUP BY 
  TRIM(cr.Crm_Cd_Desc),
  TRIM(p.Premis_Desc);

SELECT * 
FROM crime_premise_correlation
ORDER BY incident_count DESC
LIMIT 10;
